package worker_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/cache"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/db"
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

// TestWorkerSkipLockedContention verifies SELECT ... FOR UPDATE SKIP LOCKED
// across 10 concurrent worker goroutines contending for 50 seeded tasks.
// Asserts that no task is double-claimed (len(claimed) == len(unique)).
func TestWorkerSkipLockedContention(t *testing.T) {
	ctx := context.Background()

	pool, err := db.New(ctx, getTestDSN())
	if err != nil {
		t.Fatalf("connect DB failed: %v", err)
	}
	defer pool.Close()

	// Clean up any lingering pending tasks from prior runs to ensure test isolation
	_ = pool.Exec(ctx, `DELETE FROM jobs WHERE status = 'PENDING'`)

	// Seed 50 tasks under a unique parent job
	jobID := uuid.New()
	jobKey := fmt.Sprintf("worker-contention-%s", jobID)

	err = pool.Exec(ctx,
		`INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
		 VALUES ($1, $2, 'test_contention', 'PENDING', now())`,
		jobID, jobKey)
	if err != nil {
		t.Fatalf("failed to insert parent job: %v", err)
	}

	const totalTasks = 50
	expectedTaskIDs := make(map[uuid.UUID]bool)
	for i := 0; i < totalTasks; i++ {
		taskID := uuid.New()
		expectedTaskIDs[taskID] = true
		err = pool.Exec(ctx,
			`INSERT INTO job_tasks (id, job_id, handler_name, status, payload, retry_count, max_retries, created_at, updated_at)
			 VALUES ($1, $2, 'compute_handler', 'PENDING', '{}', 0, 3, now(), now())`,
			taskID, jobID)
		if err != nil {
			t.Fatalf("failed to insert task %d: %v", i, err)
		}
	}

	// 10 concurrent workers contending with SKIP LOCKED
	const numWorkers = 10
	var (
		wg         sync.WaitGroup
		claimMu    sync.Mutex
		allClaimed []model.JobTask
		barrier    = make(chan struct{})
	)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-goroutine-%d-%s", workerIndex, uuid.NewString())

			<-barrier // synchronize start

			for {
				tasks, err := pool.ClaimPendingTasks(ctx, workerID, 5)
				if err != nil {
					t.Errorf("worker %d claim failed: %v", workerIndex, err)
					return
				}
				if len(tasks) == 0 {
					// No more pending tasks available
					return
				}

				claimMu.Lock()
				allClaimed = append(allClaimed, tasks...)
				claimMu.Unlock()
			}
		}(w)
	}

	// Fire all workers simultaneously
	close(barrier)
	wg.Wait()

	// Filter tasks belonging to this test
	var seededClaimed []model.JobTask
	for _, task := range allClaimed {
		if expectedTaskIDs[task.ID] {
			seededClaimed = append(seededClaimed, task)
		}
	}

	// Verify all 50 seeded tasks were claimed
	if len(seededClaimed) != totalTasks {
		t.Fatalf("expected %d total claimed tasks, got %d", totalTasks, len(seededClaimed))
	}

	// Verify zero double-claims: len(seededClaimed) == len(unique claimed)
	seen := make(map[uuid.UUID]string) // taskID -> workerID
	for _, task := range seededClaimed {
		lockedBy := "unknown"
		if task.LockedBy != nil {
			lockedBy = *task.LockedBy
		}
		if prevWorker, alreadyClaimed := seen[task.ID]; alreadyClaimed {
			t.Fatalf("FATAL INVARIANT VIOLATION: task %s was double-claimed by %s and %s",
				task.ID, prevWorker, lockedBy)
		}
		seen[task.ID] = lockedBy
	}

	if len(seen) != totalTasks {
		t.Fatalf("expected %d unique claimed task IDs, got %d", totalTasks, len(seen))
	}

	t.Logf("SKIP LOCKED Contention PASS: %d tasks cleanly claimed across %d workers with 0 double-claims",
		totalTasks, numWorkers)

	// Clean up seeded rows
	_ = pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, jobID)
}

// TestRedisDistributedLockLeaseAndLuaRelease verifies Redis distributed locking:
// 1. Worker A acquires lock.
// 2. Worker B fails to acquire lock.
// 3. Worker B fails to release Worker A's lock via atomic Lua CAS.
// 4. Worker A successfully releases lock.
// 5. Worker B can now acquire the lock.
func TestRedisDistributedLockLeaseAndLuaRelease(t *testing.T) {
	ctx := context.Background()
	rc := cache.New(getTestRedisAddr())
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		t.Fatalf("Redis ping failed: %v", err)
	}

	taskID := fmt.Sprintf("task-lock-test-%s", uuid.NewString())
	workerA := fmt.Sprintf("worker-A-%s", uuid.NewString())
	workerB := fmt.Sprintf("worker-B-%s", uuid.NewString())

	// 1. Worker A acquires lock
	ok, err := rc.AcquireLock(ctx, taskID, workerA)
	if err != nil || !ok {
		t.Fatalf("Worker A failed to acquire lock: ok=%v, err=%v", ok, err)
	}

	// 2. Worker B fails to acquire same lock
	ok, err = rc.AcquireLock(ctx, taskID, workerB)
	if err != nil {
		t.Fatalf("Worker B acquire check error: %v", err)
	}
	if ok {
		t.Fatalf("Worker B acquired lock held by Worker A! Mutual exclusion violated.")
	}

	// 3. Worker B attempts to release Worker A's lock (should be rejected by Lua CAS)
	released, err := rc.ReleaseLock(ctx, taskID, workerB)
	if err != nil {
		t.Fatalf("Worker B release call error: %v", err)
	}
	if released {
		t.Fatalf("Worker B released Worker A's lock! Lua CAS token ownership failed.")
	}

	// 4. Worker A releases own lock (should succeed)
	released, err = rc.ReleaseLock(ctx, taskID, workerA)
	if err != nil || !released {
		t.Fatalf("Worker A failed to release own lock: released=%v, err=%v", released, err)
	}

	// 5. Worker B can now acquire lock
	ok, err = rc.AcquireLock(ctx, taskID, workerB)
	if err != nil || !ok {
		t.Fatalf("Worker B failed to acquire lock after release: ok=%v, err=%v", ok, err)
	}

	// Clean up
	_, _ = rc.ReleaseLock(ctx, taskID, workerB)
}

// TestWorkerHeartbeatTickerUpdatesDB verifies the background heartbeat ticker
// properly updates heartbeat_at in PostgreSQL and renews Redis lock lease.
func TestWorkerHeartbeatTickerUpdatesDB(t *testing.T) {
	ctx := context.Background()

	pool, err := db.New(ctx, getTestDSN())
	if err != nil {
		t.Fatalf("connect DB failed: %v", err)
	}
	defer pool.Close()

	rc := cache.New(getTestRedisAddr())
	defer rc.Close()

	jobID := uuid.New()
	taskID := uuid.New()

	// Insert task with stale heartbeat (20 seconds ago)
	staleTime := time.Now().UTC().Add(-20 * time.Second)
	err = pool.Exec(ctx,
		`INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
		 VALUES ($1, $2, 'heartbeat_test', 'PENDING', now())`,
		jobID, fmt.Sprintf("hb-test-%s", jobID))
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}
	defer func() {
		_ = pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, jobID)
	}()

	err = pool.Exec(ctx,
		`INSERT INTO job_tasks (id, job_id, handler_name, status, payload, retry_count, max_retries, locked_by, heartbeat_at, created_at, updated_at)
		 VALUES ($1, $2, 'compute_handler', 'RUNNING', '{}', 0, 3, 'worker-hb-test', $3, now(), now())`,
		taskID, jobID, staleTime)
	if err != nil {
		t.Fatalf("failed to insert task: %v", err)
	}

	// Set initial Redis lock
	taskIDStr := taskID.String()
	_, _ = rc.AcquireLock(ctx, taskIDStr, "worker-hb-test")
	defer func() {
		_ = rc.DeleteLock(ctx, taskIDStr)
	}()

	// Simulate background heartbeat ticker
	ticker := time.NewTicker(50 * time.Millisecond)
	tickerDone := make(chan struct{})
	var (
		tickCount atomic.Int64
		tickerWg  sync.WaitGroup
	)

	tickerWg.Add(1)
	go func() {
		defer tickerWg.Done()
		for {
			select {
			case <-tickerDone:
				return
			case <-ticker.C:
				_ = pool.RenewHeartbeat(ctx, taskID)
				_ = rc.RenewLock(ctx, taskIDStr)
				tickCount.Add(1)
			}
		}
	}()

	// Allow 3 ticks
	time.Sleep(200 * time.Millisecond)
	ticker.Stop()
	close(tickerDone)
	tickerWg.Wait()

	if tickCount.Load() == 0 {
		t.Fatalf("heartbeat ticker did not fire")
	}

	// Verify database heartbeat_at was updated past staleTime
	var updatedHeartbeat time.Time
	row := pool.QueryRow(ctx, `SELECT heartbeat_at FROM job_tasks WHERE id = $1`, taskID)
	if err := row.Scan(&updatedHeartbeat); err != nil {
		t.Fatalf("failed to query updated heartbeat: %v", err)
	}

	if !updatedHeartbeat.After(staleTime.Add(10 * time.Second)) {
		t.Fatalf("heartbeat_at was not updated! Stale=%v, Updated=%v", staleTime, updatedHeartbeat)
	}

	t.Logf("Heartbeat ticker PASS: updated timestamp from %v to %v across %d ticks",
		staleTime.Format(time.RFC3339), updatedHeartbeat.Format(time.RFC3339), tickCount)
}
