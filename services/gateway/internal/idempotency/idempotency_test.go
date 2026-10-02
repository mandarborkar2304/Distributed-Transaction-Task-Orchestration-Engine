package idempotency_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/cache"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/db"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/idempotency"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/model"
)

func getTestDSN() string {
	if dsn := os.Getenv("DATABASE_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5432/orchestrator"
}

func getTestRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// TestRedisFastPathTTL verifies atomic Redis fast-path caching, retrieval,
// and key TTL expiration (24h).
func TestRedisFastPathTTL(t *testing.T) {
	ctx := context.Background()
	rc := cache.New(getTestRedisAddr())
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		t.Fatalf("Redis ping failed: %v", err)
	}

	key := fmt.Sprintf("test-ttl-%s", uuid.NewString())
	payload := `{"job_id":"` + uuid.NewString() + `","status":"PENDING"}`

	// Cache miss initially
	val, hit, err := rc.GetIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error on cache miss: %v", err)
	}
	if hit {
		t.Fatalf("expected cache miss for novel key %s, got hit with %s", key, val)
	}

	// Set key with 24h TTL
	if err := rc.SetIdempotencyKey(ctx, key, payload); err != nil {
		t.Fatalf("SetIdempotencyKey failed: %v", err)
	}

	// Cache hit immediately
	val, hit, err = rc.GetIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error on cache hit: %v", err)
	}
	if !hit {
		t.Fatalf("expected cache hit for key %s, got miss", key)
	}
	if val != payload {
		t.Fatalf("payload mismatch: expected %s, got %s", payload, val)
	}

	// Clean up test key
	_ = rc.DeleteLock(ctx, key)
}

// TestConcurrentIdempotency100Goroutines concurrently fires 100 goroutines submitting
// identical idempotency keys to POST /v1/jobs. It asserts:
// 1. All 100 requests succeed with HTTP 202.
// 2. Exactly 1 request inserts to PostgreSQL (cached: false).
// 3. Exactly 99 requests are served from the Redis fast-path or conflict path (cached: true).
// 4. Exactly 1 row exists in the jobs table for this idempotency key.
func TestConcurrentIdempotency100Goroutines(t *testing.T) {
	ctx := context.Background()

	pool, err := db.New(ctx, getTestDSN())
	if err != nil {
		t.Fatalf("PostgreSQL connect failed: %v", err)
	}
	defer pool.Close()

	rc := cache.New(getTestRedisAddr())
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		t.Fatalf("Redis ping failed: %v", err)
	}

	// Create test HTTP server
	handler := idempotency.NewHandler(pool, rc, nil)
	server := httptest.NewServer(handler)
	defer server.Close()

	idempotencyKey := fmt.Sprintf("concurrent-test-%s", uuid.NewString())
	const numGoroutines = 100

	var (
		wg            sync.WaitGroup
		successCount  atomic.Int64
		cachedHits    atomic.Int64
		newInserts    atomic.Int64
		jobIDs        sync.Map
		client        = &http.Client{Timeout: 10 * time.Second}
	)

	// Barrier channel to ensure all 100 goroutines launch simultaneously
	barrier := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-barrier // wait for release

			body := []byte(`{"job_type":"test","handler_name":"compute_handler","payload":{"worker":` + fmt.Sprintf("%d", idx) + `}}`)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
			if err != nil {
				t.Errorf("failed to create request: %v", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", idempotencyKey)

			resp, err := client.Do(req)
			if err != nil {
				t.Errorf("goroutine %d request failed: %v", idx, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Errorf("goroutine %d unexpected status: %d body: %s", idx, resp.StatusCode, string(bodyBytes))
				return
			}
			successCount.Add(1)

			var jobResp model.JobResponse
			if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
				t.Errorf("goroutine %d decode failed: %v", idx, err)
				return
			}

			jobIDs.Store(jobResp.JobID, true)

			if jobResp.Cached {
				cachedHits.Add(1)
			} else {
				newInserts.Add(1)
			}
		}(i)
	}

	// Release all 100 goroutines concurrently
	close(barrier)
	wg.Wait()

	if success := successCount.Load(); success != numGoroutines {
		t.Fatalf("expected %d successful requests, got %d", numGoroutines, success)
	}

	// Verify all returned job IDs are identical
	var distinctJobIDs []string
	jobIDs.Range(func(key, value any) bool {
		distinctJobIDs = append(distinctJobIDs, key.(string))
		return true
	})

	if len(distinctJobIDs) != 1 {
		t.Fatalf("expected exactly 1 unique job_id across all 100 responses, got %v", distinctJobIDs)
	}

	primaryJobID := distinctJobIDs[0]

	// Verify exactly 1 was marked as new (or inserted) and 99 cached
	inserts := newInserts.Load()
	cached := cachedHits.Load()
	t.Logf("Concurrency results: %d total, %d new inserts, %d cached responses, job_id=%s",
		numGoroutines, inserts, cached, primaryJobID)

	if inserts != 1 {
		t.Errorf("expected exactly 1 new insert, got %d", inserts)
	}
	if cached != 99 {
		t.Errorf("expected exactly 99 cached responses, got %d", cached)
	}

	// Direct PostgreSQL query verification
	checkJobSQL := `SELECT count(*) FROM jobs WHERE idempotency_key = $1`
	var count int
	row := pool.QueryRow(ctx, checkJobSQL, idempotencyKey)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("failed to query PostgreSQL jobs table: %v", err)
	}
	if count != 1 {
		t.Fatalf("database invariant violated: expected 1 row in jobs table, got %d", count)
	}

	// Verify tasks table has exactly 1 task
	checkTaskSQL := `SELECT count(*) FROM job_tasks WHERE job_id = $1`
	var taskCount int
	taskRow := pool.QueryRow(ctx, checkTaskSQL, primaryJobID)
	if err := taskRow.Scan(&taskCount); err != nil {
		t.Fatalf("failed to query PostgreSQL job_tasks table: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("database invariant violated: expected 1 task in job_tasks table, got %d", taskCount)
	}
}
