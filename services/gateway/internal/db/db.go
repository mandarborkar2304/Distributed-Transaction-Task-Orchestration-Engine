// Package db manages the pgxpool connection pool and all SQL operations.
package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mandarborkar2304/orchestrator/gateway/internal/model"
)

// Pool is the shared pgxpool instance.
type Pool struct {
	pool *pgxpool.Pool
}

// New creates and validates a pgxpool with tuned settings for 20M-scale throughput.
// MaxConns: 150 (auto-capped by server max_connections).
// MinConns: 25  — pre-warm connections to avoid cold-start latency spikes.
// MaxConnLifetime: 30m — recycle connections before postgres idle timeout.
// MaxConnIdleTime: 5m  — return idle connections to OS promptly.
func New(ctx context.Context, dsn string) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.ParseConfig: %w", err)
	}
	cfg.MaxConns = 150
	if v := os.Getenv("PGPOOL_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxConns = int32(n)
		}
	}
	cfg.MinConns = 25
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 60 * time.Second

	// Ensure MaxConns never exceeds PostgreSQL's actual max_connections (minus headroom)
	singleConn, err := pgx.Connect(ctx, dsn)
	if err == nil {
		var pgMaxConns int
		row := singleConn.QueryRow(ctx, "SHOW max_connections")
		if scanErr := row.Scan(&pgMaxConns); scanErr == nil && pgMaxConns > 0 {
			safeLimit := int32(pgMaxConns - 15) // reserve 15 for superuser/watchdog/other clients
			if safeLimit > 0 && cfg.MaxConns > safeLimit {
				cfg.MaxConns = safeLimit
			}
			if cfg.MinConns > cfg.MaxConns {
				cfg.MinConns = cfg.MaxConns / 2
			}
		}
		_ = singleConn.Close(ctx)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.NewWithConfig: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pgxpool ping: %w", err)
	}
	return &Pool{pool: pool}, nil
}

// Close shuts down the pool gracefully.
func (p *Pool) Close() {
	p.pool.Close()
}

// QueryRow wraps pgxpool.Pool.QueryRow.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

// Exec wraps pgxpool.Pool.Exec.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := p.pool.Exec(ctx, sql, args...)
	return err
}

// RawPool returns the underlying *pgxpool.Pool.
func (p *Pool) RawPool() *pgxpool.Pool {
	return p.pool
}

// UpsertJob inserts a new job row using INSERT ... ON CONFLICT DO NOTHING RETURNING.
// Returns (job, true, nil) when a new row was created.
// Returns (job, false, nil) when the idempotency key already existed.
func (p *Pool) UpsertJob(ctx context.Context, key, jobType, handlerName string, payload map[string]any) (model.Job, bool, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return model.Job{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return model.Job{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Transactional outbox: insert job with idempotency guard.
	insertJobSQL := `
		INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
		VALUES (gen_random_uuid(), $1, $2, 'PENDING', now())
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, idempotency_key, job_type, status, created_at`

	row := tx.QueryRow(ctx, insertJobSQL, key, jobType)
	var job model.Job
	err = row.Scan(&job.ID, &job.IdempotencyKey, &job.JobType, &job.Status, &job.CreatedAt)
	if err == pgx.ErrNoRows {
		// Conflict: idempotency key already exists — fetch the existing row.
		if err2 := tx.Rollback(ctx); err2 != nil {
			return model.Job{}, false, err2
		}
		existing, fetchErr := p.fetchJobByKey(ctx, key)
		return existing, false, fetchErr
	}
	if err != nil {
		return model.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	// Insert the associated task in the same transaction (transactional outbox).
	insertTaskSQL := `
		INSERT INTO job_tasks (id, job_id, handler_name, status, payload, retry_count, max_retries, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, 'PENDING', $3, 0, 3, now(), now())`

	_, err = tx.Exec(ctx, insertTaskSQL, job.ID, handlerName, payloadBytes)
	if err != nil {
		return model.Job{}, false, fmt.Errorf("insert task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return model.Job{}, false, fmt.Errorf("commit: %w", err)
	}
	return job, true, nil
}

// fetchJobByKey is used on idempotency conflict to return the existing job record.
func (p *Pool) fetchJobByKey(ctx context.Context, key string) (model.Job, error) {
	var job model.Job
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		row := p.pool.QueryRow(ctx,
			`SELECT id, idempotency_key, job_type, status, created_at FROM jobs WHERE idempotency_key = $1`, key)
		if err := row.Scan(&job.ID, &job.IdempotencyKey, &job.JobType, &job.Status, &job.CreatedAt); err == nil {
			return job, nil
		} else {
			lastErr = err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return model.Job{}, fmt.Errorf("fetchJobByKey: %w", lastErr)
}

// ClaimPendingTasks claims up to batchSize PENDING tasks using SELECT FOR UPDATE SKIP LOCKED.
// All claimed tasks are atomically flipped to RUNNING within the same transaction.
func (p *Pool) ClaimPendingTasks(ctx context.Context, workerID string, batchSize int) ([]model.JobTask, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT id, job_id, handler_name, status, payload, retry_count, max_retries,
		       locked_by, heartbeat_at, last_error, created_at, updated_at
		FROM job_tasks
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return nil, fmt.Errorf("query SKIP LOCKED: %w", err)
	}

	tasks, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.JobTask, error) {
		var t model.JobTask
		return t, row.Scan(
			&t.ID, &t.JobID, &t.HandlerName, &t.Status, &t.Payload,
			&t.RetryCount, &t.MaxRetries, &t.LockedBy, &t.HeartbeatAt,
			&t.LastError, &t.CreatedAt, &t.UpdatedAt,
		)
	})
	if err != nil {
		return nil, fmt.Errorf("collect rows: %w", err)
	}
	if len(tasks) == 0 {
		return nil, nil
	}

	// Atomically claim all fetched tasks.
	now := time.Now().UTC()
	ids := make([]uuid.UUID, len(tasks))
	for i, t := range tasks {
		ids[i] = t.ID
	}
	_, err = tx.Exec(ctx, `
		UPDATE job_tasks
		SET status = 'RUNNING', locked_by = $1, heartbeat_at = $2, updated_at = $2
		WHERE id = ANY($3)`, workerID, now, ids)
	if err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return tasks, nil
}

// CompleteTask marks a task as COMPLETED and its parent job as COMPLETED.
func (p *Pool) CompleteTask(ctx context.Context, taskID, jobID uuid.UUID) error {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx,
		`UPDATE job_tasks SET status='COMPLETED', locked_by=NULL, heartbeat_at=NULL, updated_at=now() WHERE id=$1`,
		taskID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET status='COMPLETED' WHERE id=$1`, jobID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FailTask increments retry_count; if exhausted, moves to DEAD_LETTER.
func (p *Pool) FailTask(ctx context.Context, taskID uuid.UUID, errMsg string, retryCount, maxRetries int) error {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if retryCount+1 >= maxRetries {
		_, err = tx.Exec(ctx,
			`UPDATE job_tasks SET status='DEAD_LETTER', last_error=$1, locked_by=NULL, heartbeat_at=NULL, updated_at=now() WHERE id=$2`,
			errMsg, taskID)
	} else {
		_, err = tx.Exec(ctx,
			`UPDATE job_tasks SET status='PENDING', retry_count=retry_count+1, last_error=$1, locked_by=NULL, heartbeat_at=NULL, updated_at=now() WHERE id=$2`,
			errMsg, taskID)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RenewHeartbeat updates heartbeat_at for a running task.
func (p *Pool) RenewHeartbeat(ctx context.Context, taskID uuid.UUID) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE job_tasks SET heartbeat_at=now(), updated_at=now() WHERE id=$1`, taskID)
	return err
}

// SeedTasks inserts count synthetic PENDING tasks via a batch pipeline for load testing.
func (p *Pool) SeedTasks(ctx context.Context, count int) error {
	batch := &pgx.Batch{}
	for i := 0; i < count; i++ {
		jobID := uuid.New()
		key := fmt.Sprintf("loadgen-%s", jobID)
		batch.Queue(
			`INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
			 VALUES ($1, $2, 'loadgen', 'PENDING', now())`,
			jobID, key,
		)
		batch.Queue(
			`INSERT INTO job_tasks (id, job_id, handler_name, status, payload, retry_count, max_retries, created_at, updated_at)
			 VALUES (gen_random_uuid(), $1, 'compute_handler', 'PENDING', '{}', 0, 3, now(), now())`,
			jobID,
		)
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for i := 0; i < count*2; i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("seed batch row %d: %w", i, err)
		}
	}
	return nil
}
