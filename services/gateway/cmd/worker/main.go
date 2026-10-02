// Worker is the high-performance Go task claim loop.
// It claims PENDING tasks via SELECT FOR UPDATE SKIP LOCKED, acquires Redis
// distributed lock leases, and manages heartbeat renewal via non-blocking tickers.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/cache"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/db"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/metrics"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/model"
)

const (
	batchSize         = 50
	heartbeatInterval = 10 * time.Second
	pollInterval      = 200 * time.Millisecond
	maxConcurrency    = 100 // max goroutines processing tasks simultaneously
)

var totalProcessed atomic.Int64

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbDSN := envOr("DATABASE_DSN", "postgres://postgres:postgres@localhost:5432/orchestrator")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	workerID := envOr("WORKER_ID", uuid.NewString())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.New(ctx, dbDSN)
	if err != nil {
		logger.Error("DB connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rc := cache.New(redisAddr)
	if err := rc.Ping(ctx); err != nil {
		logger.Error("Redis ping failed", "err", err)
		os.Exit(1)
	}
	defer rc.Close()

	logger.Info("Go worker started", "worker_id", workerID, "batch_size", batchSize, "concurrency", maxConcurrency)

	sem := make(chan struct{}, maxConcurrency) // concurrency limiter
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown: waiting for in-flight tasks")
			wg.Wait()
			logger.Info("worker stopped cleanly", "total_processed", totalProcessed.Load())
			return
		default:
		}

		tasks, err := pool.ClaimPendingTasks(ctx, workerID, batchSize)
		if err != nil {
			logger.Warn("claim failed", "err", err)
			time.Sleep(pollInterval)
			continue
		}
		if len(tasks) == 0 {
			time.Sleep(pollInterval)
			continue
		}

		metrics.WorkerTasksClaimed.Add(float64(len(tasks)))

		for _, t := range tasks {
			task := t // capture for goroutine
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				processTask(ctx, pool, rc, logger, workerID, task)
			}()
		}
	}
}

// processTask executes a single claimed task with a heartbeat ticker and Redis distributed lock.
func processTask(
	ctx context.Context,
	pool *db.Pool,
	rc *cache.Client,
	logger *slog.Logger,
	workerID string,
	task model.JobTask,
) {
	taskIDStr := task.ID.String()

	// Acquire Redis distributed lock (defense-in-depth against watchdog collision).
	acquired, err := rc.AcquireLock(ctx, taskIDStr, workerID)
	if err != nil || !acquired {
		logger.Warn("could not acquire Redis lock, skipping", "task_id", taskIDStr)
		return
	}
	defer rc.ReleaseLock(ctx, taskIDStr, workerID) //nolint:errcheck

	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	// Heartbeat ticker: renews DB heartbeat_at and Redis lock TTL every 10s.
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ticker.C:
				if hbErr := pool.RenewHeartbeat(ctx, task.ID); hbErr != nil {
					logger.Warn("heartbeat renewal failed", "task_id", taskIDStr, "err", hbErr)
				}
				if lockErr := rc.RenewLock(ctx, taskIDStr); lockErr != nil {
					logger.Warn("lock renewal failed", "task_id", taskIDStr, "err", lockErr)
				}
			}
		}
	}()
	defer close(heartbeatDone)

	// Simulate task execution (Go-native handler).
	execErr := executeHandler(ctx, task)

	// Use an independent context for status updates so in-flight tasks cleanly record outcome on shutdown.
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer writeCancel()

	if execErr == nil {
		if err := pool.CompleteTask(writeCtx, task.ID, task.JobID); err != nil {
			logger.Error("CompleteTask DB write failed", "task_id", taskIDStr, "err", err)
		}
		metrics.WorkerTasksCompleted.WithLabelValues("completed").Inc()
		count := totalProcessed.Add(1)
		if count%1000 == 0 {
			logger.Info("tasks draining progress", "completed", count)
		}
	} else {
		errMsg := execErr.Error()
		if task.RetryCount+1 >= task.MaxRetries {
			metrics.WorkerTasksCompleted.WithLabelValues("dead_letter").Inc()
			logger.Warn("task dead-lettered", "task_id", taskIDStr, "retries", task.RetryCount)
		} else {
			backoff := exponentialBackoff(task.RetryCount)
			time.Sleep(backoff)
			metrics.WorkerTasksCompleted.WithLabelValues("failed").Inc()
			logger.Warn("task failed, will retry", "task_id", taskIDStr, "retry", task.RetryCount+1)
		}
		if err := pool.FailTask(writeCtx, task.ID, errMsg, task.RetryCount, task.MaxRetries); err != nil {
			logger.Error("FailTask DB write failed", "task_id", taskIDStr, "err", err)
		}
	}
}

// executeHandler is the Go-native task dispatcher.
// In production this would match handler_name to registered Go handler functions.
// Unrecognised handlers are delegated to the shared Python worker queue via PostgreSQL status.
func executeHandler(_ context.Context, task model.JobTask) error {
	switch task.HandlerName {
	case "compute_handler":
		// Simulate compute work with configurable stage delay.
		time.Sleep(5 * time.Millisecond)
		return nil
	case "payment_handler":
		// Simulate payment processing latency.
		time.Sleep(10 * time.Millisecond)
		return nil
	default:
		// Unknown handler: return an error so the task remains available
		// for the Python worker pool to pick up.
		return fmt.Errorf("no Go handler registered for %q; delegating to Python worker pool", task.HandlerName)
	}
}

// exponentialBackoff returns min(60s, 2^retry + jitter).
func exponentialBackoff(retry int) time.Duration {
	base := math.Pow(2, float64(retry))
	jitter := rand.Float64() // [0, 1)
	secs := math.Min(60.0, base+jitter)
	return time.Duration(secs * float64(time.Second))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
