// loadgen simulates 20M-user-scale ingestion load against the Go gateway.
//
// Usage:
//
//	go run scripts/benchmarks/loadgen.go \
//	  -target http://localhost:8080 \
//	  -vus 2000 \
//	  -duration 30s \
//	  -users 20000000 \
//	  -collision-rate 0.7 \
//	  -hot-pool 1000
//
// The keyspace generator emits synthetic keys in the form "user_<1-N>" where N
// is set by -users. With -collision-rate 0.7, 70% of requests reuse a key from the
// hot-pool (exercising the Redis fast-path), and 30% generate novel keys (exercising
// the PostgreSQL transactional outbox).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	status   int
	latency  time.Duration
	cached   bool
	isNew    bool
	err      error
}

func main() {
	target := flag.String("target", "http://localhost:8080", "Gateway base URL")
	vus := flag.Int("vus", 2000, "Number of concurrent virtual users")
	duration := flag.Duration("duration", 30*time.Second, "Load test duration")
	users := flag.Int64("users", 20_000_000, "User address space size for key generation")
	collisionRate := flag.Float64("collision-rate", 0.70, "Fraction of requests that reuse a key (0.0–1.0)")
	hotPool := flag.Int64("hot-pool", 1000, "Size of the hot key pool to simulate realistic active-user collisions")
	seedTasks := flag.Int("seed-tasks", 0, "If >0, seed this many tasks directly via DB before load (set DATABASE_DSN env)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *seedTasks > 0 {
		dsn := os.Getenv("DATABASE_DSN")
		if dsn == "" {
			dsn = "postgres://postgres:postgres@localhost:5432/orchestrator"
		}
		logger.Info("seeding tasks directly into database", "count", *seedTasks)
		if err := seedDB(dsn, *seedTasks); err != nil {
			logger.Error("seed failed", "err", err)
			os.Exit(1)
		}
		logger.Info("seed complete")
	}

	hotKeyPoolSize := *hotPool
	if hotKeyPoolSize > *users {
		hotKeyPoolSize = *users
	}

	logger.Info("load test starting",
		"target", *target,
		"vus", *vus,
		"duration", *duration,
		"user_space", *users,
		"collision_rate", *collisionRate,
		"hot_key_pool", hotKeyPoolSize,
	)

	var (
		totalRequests  atomic.Int64
		totalErrors    atomic.Int64
		totalCacheHits atomic.Int64
		totalNewJobs   atomic.Int64
		latencySum     atomic.Int64 // nanoseconds
		p50            time.Duration
		p95            time.Duration
		p99            time.Duration
	)

	results := make(chan result, *vus*10)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Aggregator goroutine.
	var latencies []time.Duration
	var aggMu sync.Mutex
	aggDone := make(chan struct{})
	go func() {
		defer close(aggDone)
		for r := range results {
			totalRequests.Add(1)
			if r.err != nil || r.status >= 500 {
				totalErrors.Add(1)
			}
			if r.cached {
				totalCacheHits.Add(1)
			}
			if r.isNew {
				totalNewJobs.Add(1)
			}
			latencySum.Add(r.latency.Nanoseconds())
			aggMu.Lock()
			latencies = append(latencies, r.latency)
			aggMu.Unlock()
		}
	}()

	// Launch VU goroutines.
	var wg sync.WaitGroup
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *vus * 2,
			MaxIdleConnsPerHost: *vus * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	for i := 0; i < *vus; i++ {
		wg.Add(1)
		go func(vuID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(vuID) + time.Now().UnixNano()))

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Key selection: collision-rate fraction reuses hot pool; rest are unique.
				var key string
				if rng.Float64() < *collisionRate {
					key = fmt.Sprintf("user_%d", rng.Int63n(hotKeyPoolSize)+1)
				} else {
					key = fmt.Sprintf("user_%d", rng.Int63n(*users)+1+hotKeyPoolSize)
				}

				handlerName := "compute_handler"
				if rng.Intn(3) == 0 {
					handlerName = "payment_handler"
				}

				body := fmt.Sprintf(`{"job_type":"loadgen","handler_name":%q,"payload":{"vu":%d}}`,
					handlerName, vuID)

				start := time.Now()
				reqCtx, reqCancel := context.WithTimeout(context.Background(), 10*time.Second)
				req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
					*target+"/v1/jobs", strings.NewReader(body))
				if err != nil {
					reqCancel()
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", key)

				resp, err := client.Do(req)
				lat := time.Since(start)
				reqCancel()

				var r result
				r.latency = lat
				r.err = err

				if err == nil {
					r.status = resp.StatusCode
					var payload struct {
						Cached bool `json:"cached"`
					}
					json.NewDecoder(resp.Body).Decode(&payload) //nolint:errcheck
					resp.Body.Close()
					r.cached = payload.Cached
					r.isNew = !payload.Cached
				}

				results <- r
			}
		}(i)
	}

	wg.Wait()
	close(results)
	<-aggDone

	elapsed := *duration

	// Compute percentiles.
	aggMu.Lock()
	sortDurations(latencies)
	n := len(latencies)
	if n > 0 {
		p50 = latencies[n*50/100]
		p95 = latencies[n*95/100]
		p99 = latencies[n*99/100]
	}
	aggMu.Unlock()

	reqs := totalRequests.Load()
	errs := totalErrors.Load()
	hits := totalCacheHits.Load()
	news := totalNewJobs.Load()
	tps := float64(reqs) / elapsed.Seconds()
	var avgLatency time.Duration
	if reqs > 0 {
		avgLatency = time.Duration(latencySum.Load() / reqs)
	}
	successRate := 100.0
	if reqs > 0 {
		successRate = float64(reqs-errs) / float64(reqs) * 100
	}
	cacheHitRate := 0.0
	if reqs > 0 {
		cacheHitRate = float64(hits) / float64(reqs) * 100
	}

	fmt.Println("\n╔══════════════════════════════════════════════════════╗")
	fmt.Println("║          Go Gateway Load Test — Results               ║")
	fmt.Println("╠══════════════════════════════════════════════════════╣")
	fmt.Printf("║  Duration          : %-30s ║\n", elapsed)
	fmt.Printf("║  Virtual Users     : %-30d ║\n", *vus)
	fmt.Printf("║  User Address Space: %-30d ║\n", *users)
	fmt.Printf("║  Hot Key Pool Size : %-30d ║\n", hotKeyPoolSize)
	fmt.Printf("║  Total Requests    : %-30d ║\n", reqs)
	fmt.Printf("║  Throughput (RPS)  : %-30.2f ║\n", tps)
	fmt.Printf("║  Success Rate      : %-29.2f%% ║\n", successRate)
	fmt.Printf("║  Errors            : %-30d ║\n", errs)
	fmt.Printf("║  Cache Hits        : %-30d ║\n", hits)
	fmt.Printf("║  Cache Hit Rate    : %-29.2f%% ║\n", cacheHitRate)
	fmt.Printf("║  New Jobs (DB)     : %-30d ║\n", news)
	fmt.Printf("║  Avg Latency       : %-30s ║\n", avgLatency.Round(time.Microsecond))
	fmt.Printf("║  p50 Latency       : %-30s ║\n", p50.Round(time.Microsecond))
	fmt.Printf("║  p95 Latency       : %-30s ║\n", p95.Round(time.Microsecond))
	fmt.Printf("║  p99 Latency       : %-30s ║\n", p99.Round(time.Microsecond))
	fmt.Println("╚══════════════════════════════════════════════════════╝")

	if successRate < 99.0 {
		logger.Warn("success rate below 99% threshold", "rate", successRate)
		os.Exit(1)
	}
	logger.Info("load test passed", "tps", tps, "p95", p95, "success_rate", successRate)
}

// sortDurations sorts a []time.Duration in-place using insertion sort.
func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		key := d[i]
		j := i - 1
		for j >= 0 && d[j] > key {
			d[j+1] = d[j]
			j--
		}
		d[j+1] = key
	}
}

// seedDB seeds count tasks directly into PostgreSQL via generate_series batch.
func seedDB(dsn string, count int) error {
	pythonCode := fmt.Sprintf(`
import asyncio, asyncpg

async def main():
    conn = await asyncpg.connect(%q)
    print(f"Seeding %d jobs and tasks directly into PostgreSQL...")
    await conn.execute("""
        INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
        SELECT gen_random_uuid(), 'seed_' || g, 'loadgen', 'PENDING', now()
        FROM generate_series(1, %d) g
        ON CONFLICT (idempotency_key) DO NOTHING;
        
        INSERT INTO job_tasks (id, job_id, handler_name, status, payload, retry_count, max_retries, created_at, updated_at)
        SELECT gen_random_uuid(), id, 'compute_handler', 'PENDING', '{}'::jsonb, 0, 3, now(), now()
        FROM jobs WHERE job_type = 'loadgen' AND status = 'PENDING'
        ON CONFLICT DO NOTHING;
    """)
    await conn.close()
    print("Seed complete.")

asyncio.run(main())
`, dsn, count, count)

	cmd := exec.Command("python3", "-c", pythonCode)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
