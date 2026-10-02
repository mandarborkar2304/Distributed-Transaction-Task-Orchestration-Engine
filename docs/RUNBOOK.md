# Runbook — SRE & Operational Guide

This runbook documents investigation procedures, diagnostic SQL queries, and recovery actions for the Distributed Transaction & Task Orchestration Engine in a production or staging environment.

All SQL is written for PostgreSQL 16. All Redis commands are written for `redis-cli`.

---

## Table of Contents

1. [Operational Procedures](#1-operational-procedures)
   - [Investigating Stuck Tasks](#11-investigating-stuck-tasks)
   - [Connection Pool Exhaustion](#12-connection-pool-exhaustion)
   - [Redis Failover & Eviction](#13-redis-failover--eviction)
   - [Diagnosing Long-Running Transactions](#14-diagnosing-long-running-transactions)
   - [Clearing Poison-Pill Tasks from the DLQ](#15-clearing-poison-pill-tasks-from-the-dlq)
2. [Diagnostic SQL Recipes](#2-diagnostic-sql-recipes)
   - [Stale Heartbeat Detection](#21-stale-heartbeat-detection)
   - [Worker Claim Contention](#22-worker-claim-contention)
   - [Failure & DLQ Distribution](#23-failure--dlq-distribution)
   - [Task Throughput Over Time](#24-task-throughput-over-time)
   - [Index Usage Verification](#25-index-usage-verification)
3. [Alert Response Playbooks](#3-alert-response-playbooks)

---

## 1. Operational Procedures

### 1.1 Investigating Stuck Tasks

**Symptom:** Tasks remain in `RUNNING` status for an extended period without a `heartbeat_at` update. The `orchestrator_reclaimed_orphans_total` counter is not incrementing, or it is incrementing repeatedly for the same tasks.

**Step 1 — Identify the stuck tasks:**

```sql
SELECT
    jt.id,
    jt.job_id,
    jt.handler_name,
    jt.status,
    jt.retry_count,
    jt.max_retries,
    jt.locked_by,
    jt.heartbeat_at,
    NOW() - jt.heartbeat_at AS staleness,
    jt.last_error
FROM job_tasks jt
WHERE jt.status = 'RUNNING'
ORDER BY jt.heartbeat_at ASC NULLS FIRST;
```

**Step 2 — Check if the locking worker is still alive:**

Using the `locked_by` UUID from the query above:

```sql
-- Check if any other tasks are running under the same worker_id
SELECT id, handler_name, heartbeat_at
FROM job_tasks
WHERE locked_by = '<worker_uuid_from_above>';
```

**Step 3 — Check Redis lock state:**

```bash
redis-cli GET "lock:task:<task_uuid>"
# Returns worker_id if lock is still held, or (nil) if expired
redis-cli TTL "lock:task:<task_uuid>"
# Returns remaining TTL in seconds; -2 = key does not exist
```

**Step 4 — Diagnose Watchdog:**

Verify the Watchdog daemon is running and check its logs. The Watchdog queries for tasks where `heartbeat_at < NOW() - 30s`. If a task has a stale heartbeat but the Watchdog is not reclaiming it:
- The Watchdog process may have crashed silently. Restart it.
- The Watchdog may be blocked waiting for a PostgreSQL connection (check connection pool exhaustion — see §1.2).

**Step 5 — Manual forced reclaim (last resort):**

Only execute this after confirming the worker process is dead and no Redis lock exists for the task:

```sql
BEGIN;

UPDATE job_tasks
SET
    status = 'PENDING',
    retry_count = retry_count + 1,
    locked_by = NULL,
    heartbeat_at = NULL,
    last_error = 'Manual reclaim by SRE: worker confirmed dead'
WHERE id = '<task_uuid>'
  AND status = 'RUNNING';

-- Verify one row affected before committing
COMMIT;
```

---

### 1.2 Connection Pool Exhaustion

**Symptom:** Application logs contain `asyncpg.exceptions.TooManyConnectionsError` or SQLAlchemy `TimeoutError: QueuePool limit of size 25 overflow 50 reached, connection timed out after 30 sec`. Prometheus shows elevated p99 latency.

**Step 1 — Check active PostgreSQL connections:**

```sql
SELECT
    state,
    wait_event_type,
    wait_event,
    COUNT(*) AS connection_count,
    MAX(NOW() - query_start) AS longest_running
FROM pg_stat_activity
WHERE datname = 'orchestrator'
GROUP BY state, wait_event_type, wait_event
ORDER BY connection_count DESC;
```

**Step 2 — Find long-running idle connections:**

```sql
SELECT
    pid,
    state,
    wait_event,
    query,
    NOW() - state_change AS idle_duration
FROM pg_stat_activity
WHERE datname = 'orchestrator'
  AND state = 'idle'
  AND NOW() - state_change > INTERVAL '5 minutes'
ORDER BY idle_duration DESC;
```

**Step 3 — Terminate stuck idle connections if needed:**

```sql
-- Terminate connections idle for more than 10 minutes
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'orchestrator'
  AND state = 'idle'
  AND NOW() - state_change > INTERVAL '10 minutes';
```

**Step 4 — Tune pool parameters if chronic:**

In [`src/database.py`](../src/database.py), the pool is configured as:
```python
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=25,
    max_overflow=50,
    pool_timeout=30.0,
)
```

With `N` Uvicorn workers, maximum connections = `N × 75`. If the PostgreSQL `max_connections` limit is being hit:

```sql
-- Check current max_connections setting
SHOW max_connections;

-- Increase it (requires PostgreSQL reload or restart)
ALTER SYSTEM SET max_connections = 500;
SELECT pg_reload_conf();
```

Or reduce `pool_size` and `max_overflow` in `src/database.py` and restart the application.

---

### 1.3 Redis Failover & Eviction

**Symptom:** Application logs contain `redis.exceptions.ConnectionError` or `redis.exceptions.ResponseError`. Idempotency cache hits drop to zero and all requests hit PostgreSQL.

**Immediate Impact Assessment:**

| Redis state | Effect on system |
|---|---|
| Redis unreachable | `POST /v1/jobs` falls through to PostgreSQL on every request (increased DB load). All idempotency cache writes fail silently (logged as warnings). Distributed locks cannot be acquired — worker processes skip tasks and log warnings. |
| Redis key eviction (OOM) | Idempotency keys are evicted; duplicate requests generate new `INSERT ... ON CONFLICT` sequences. PostgreSQL unique constraint prevents double-inserts — correctness is maintained but Redis cache benefits are lost. |

**Step 1 — Check Redis connectivity:**

```bash
redis-cli -h localhost -p 6379 PING
# Expected: PONG
```

**Step 2 — Check Redis memory pressure:**

```bash
redis-cli INFO memory | grep -E "used_memory_human|maxmemory_human|mem_fragmentation_ratio"
```

**Step 3 — Check eviction policy:**

```bash
redis-cli CONFIG GET maxmemory-policy
# For idempotency safety, prefer: allkeys-lru or volatile-lru
# Avoid: allkeys-random (evicts locks randomly, may cause task double-execution)
```

**Step 4 — On Redis reconnect, warm idempotency cache from PostgreSQL:**

If Redis was unavailable for an extended period, recent jobs may not have their idempotency keys cached. Clients retrying will hit PostgreSQL and encounter `ON CONFLICT DO NOTHING`, which is safe. No manual cache warming is required — the cache self-populates on the next unique submission for each key.

**Step 5 — Verify distributed locks after reconnect:**

After Redis reconnect, any `RUNNING` tasks whose Redis locks have expired will be reclaimed by the Watchdog on its next cycle (within 15 seconds). Monitor `orchestrator_reclaimed_orphans_total` for a spike immediately following Redis reconnection — this is expected and not an error.

---

### 1.4 Diagnosing Long-Running Transactions

**Symptom:** `SKIP LOCKED` queries are slower than expected, or the Watchdog is holding locks longer than its interval. Postgres shows blocked queries.

**Step 1 — Find long-running transactions:**

```sql
SELECT
    pid,
    now() - pg_stat_activity.xact_start AS duration,
    query,
    state,
    wait_event_type,
    wait_event
FROM pg_stat_activity
WHERE datname = 'orchestrator'
  AND state != 'idle'
  AND xact_start IS NOT NULL
ORDER BY duration DESC;
```

**Step 2 — Check for lock waits:**

```sql
SELECT
    blocked.pid AS blocked_pid,
    blocked.query AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.query AS blocking_query,
    now() - blocked.query_start AS blocked_duration
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking
    ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
WHERE datname = 'orchestrator';
```

**Step 3 — Cancel a blocking query:**

```sql
-- Soft cancel (sends SIGINT to the backend; query may retry)
SELECT pg_cancel_backend(<blocking_pid>);

-- Hard terminate (sends SIGTERM; connection dropped)
SELECT pg_terminate_backend(<blocking_pid>);
```

> **Important:** `SKIP LOCKED` is designed specifically to avoid blocking. If you see blocked queries, they are likely caused by long-running transactions outside the normal worker/watchdog flow (e.g., a `psql` session with an open transaction, or a schema migration).

---

### 1.5 Clearing Poison-Pill Tasks from the DLQ

**Symptom:** Tasks accumulate in `DEAD_LETTER` status. The `orchestrator_tasks_total{status="DEAD_LETTER"}` counter is growing. Some tasks need to be replayed; others need to be permanently discarded.

**Step 1 — Inspect the DLQ:**

```sql
SELECT
    jt.id AS task_id,
    jt.job_id,
    jt.handler_name,
    jt.retry_count,
    jt.max_retries,
    jt.last_error,
    jt.updated_at,
    j.idempotency_key,
    j.job_type
FROM job_tasks jt
JOIN jobs j ON j.id = jt.job_id
WHERE jt.status = 'DEAD_LETTER'
ORDER BY jt.updated_at DESC;
```

**Step 2a — Replay a dead-lettered task (re-queue it):**

This resets the task to `PENDING` and clears its retry counter, giving it a fresh `max_retries` budget. Only do this after the underlying bug that caused the failures has been fixed.

```sql
BEGIN;

UPDATE job_tasks
SET
    status = 'PENDING',
    retry_count = 0,
    locked_by = NULL,
    heartbeat_at = NULL,
    last_error = NULL
WHERE id = '<task_uuid>'
  AND status = 'DEAD_LETTER';

-- Also reset the parent job status so it doesn't remain FAILED
UPDATE jobs
SET status = 'PENDING'
WHERE id = (SELECT job_id FROM job_tasks WHERE id = '<task_uuid>');

COMMIT;
```

**Step 2b — Permanently discard a dead-lettered task:**

```sql
BEGIN;

DELETE FROM job_tasks WHERE id = '<task_uuid>' AND status = 'DEAD_LETTER';

-- If the parent job has no remaining tasks, optionally clean up the job row
DELETE FROM jobs
WHERE id = '<job_uuid>'
  AND NOT EXISTS (SELECT 1 FROM job_tasks WHERE job_id = '<job_uuid>');

COMMIT;
```

---

## 2. Diagnostic SQL Recipes

### 2.1 Stale Heartbeat Detection

Tasks currently `RUNNING` with a heartbeat older than the Watchdog timeout (30 seconds):

```sql
SELECT
    id,
    job_id,
    handler_name,
    locked_by,
    heartbeat_at,
    EXTRACT(EPOCH FROM (NOW() - heartbeat_at)) AS seconds_stale,
    retry_count,
    max_retries
FROM job_tasks
WHERE status = 'RUNNING'
  AND (
      heartbeat_at < NOW() - INTERVAL '30 seconds'
      OR heartbeat_at IS NULL
  )
ORDER BY heartbeat_at ASC NULLS FIRST;
```

This is exactly the condition the Watchdog scans. If this query returns rows and the Watchdog is running, it will reclaim them within 15 seconds (the next `interval` tick).

---

### 2.2 Worker Claim Contention

Distribution of tasks by the worker that claimed them (useful for identifying unbalanced workers):

```sql
SELECT
    locked_by AS worker_id,
    COUNT(*) AS tasks_held,
    MIN(heartbeat_at) AS oldest_heartbeat,
    MAX(heartbeat_at) AS newest_heartbeat
FROM job_tasks
WHERE status = 'RUNNING'
GROUP BY locked_by
ORDER BY tasks_held DESC;
```

Throughput per worker (completed tasks in the last hour):

```sql
SELECT
    locked_by AS worker_id,
    COUNT(*) AS tasks_completed_last_hour
FROM job_tasks
WHERE status = 'COMPLETED'
  AND updated_at > NOW() - INTERVAL '1 hour'
GROUP BY locked_by
ORDER BY tasks_completed_last_hour DESC;
```

---

### 2.3 Failure & DLQ Distribution

Overall task status distribution:

```sql
SELECT
    status,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM job_tasks
GROUP BY status
ORDER BY count DESC;
```

Failure distribution by handler and error message:

```sql
SELECT
    handler_name,
    last_error,
    COUNT(*) AS occurrences
FROM job_tasks
WHERE status IN ('DEAD_LETTER', 'FAILED')
GROUP BY handler_name, last_error
ORDER BY occurrences DESC;
```

Tasks that have been retried the maximum number of times and are now in DLQ:

```sql
SELECT
    handler_name,
    COUNT(*) AS dlq_count,
    MAX(updated_at) AS most_recent_dlq
FROM job_tasks
WHERE status = 'DEAD_LETTER'
  AND retry_count >= max_retries
GROUP BY handler_name
ORDER BY dlq_count DESC;
```

---

### 2.4 Task Throughput Over Time

Completed tasks per minute (last 30 minutes), broken down by handler:

```sql
SELECT
    DATE_TRUNC('minute', updated_at) AS minute_bucket,
    handler_name,
    COUNT(*) AS completed
FROM job_tasks
WHERE status = 'COMPLETED'
  AND updated_at > NOW() - INTERVAL '30 minutes'
GROUP BY minute_bucket, handler_name
ORDER BY minute_bucket DESC, handler_name;
```

Ingestion rate — new jobs per minute (last 30 minutes):

```sql
SELECT
    DATE_TRUNC('minute', created_at) AS minute_bucket,
    COUNT(*) AS jobs_created
FROM jobs
WHERE created_at > NOW() - INTERVAL '30 minutes'
GROUP BY minute_bucket
ORDER BY minute_bucket DESC;
```

---

### 2.5 Index Usage Verification

Confirm that the hot-path indexes are being used by the planner:

```sql
-- Verify SKIP LOCKED worker poll uses ix_job_tasks_pending_running
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM job_tasks
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT 10
FOR UPDATE SKIP LOCKED;
```

Expected: `Index Scan using ix_job_tasks_pending_running on job_tasks`.

```sql
-- Verify idempotency key lookup uses ix_jobs_idempotency_key
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM jobs
WHERE idempotency_key = 'test-key-001';
```

Expected: `Index Scan using ix_jobs_idempotency_key on jobs`.

```sql
-- Check actual index usage statistics
SELECT
    indexname,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE relname IN ('jobs', 'job_tasks')
ORDER BY relname, idx_scan DESC;
```

---

## 3. Alert Response Playbooks

### Alert: `orchestrator_reclaimed_orphans_total` Rate > 1/min

**Severity:** Warning  
**Likely cause:** Workers are crashing or losing PostgreSQL connectivity before completing tasks.

**Response:**
1. Check worker process logs for exceptions or `ConnectionError` messages.
2. Run §2.1 stale heartbeat query to see current orphan count.
3. Check PostgreSQL connection pool health (§1.2).
4. If isolated to one worker: restart that worker instance.
5. If systemic: investigate infrastructure (memory pressure, network partitioning).

---

### Alert: `orchestrator_tasks_total{status="DEAD_LETTER"}` Rate > 0

**Severity:** Critical (immediate review)  
**Likely cause:** A handler is throwing unretriable errors (bad payload, external API down, schema mismatch).

**Response:**
1. Run the failure distribution query (§2.3) to identify the handler and error pattern.
2. If the error is transient (e.g., external service briefly down): replay affected tasks (§1.5).
3. If the error is a code bug: fix the handler, deploy, then replay tasks.
4. If the payload is malformed: discard the task (§1.5) and identify the upstream source of bad data.

---

### Alert: `orchestrator_execution_duration_seconds` p99 > 10s

**Severity:** Warning  
**Likely cause:** Handler is running longer than the heartbeat interval, risking false watchdog reclamation.

**Response:**
1. Identify which handler (`payment_handler` vs `compute_handler`) is slow using:
   ```promql
   histogram_quantile(0.99,
     rate(orchestrator_execution_duration_seconds_bucket[5m])
   ) by (handler)
   ```
2. Profile the handler logic for blocking calls (synchronous I/O, CPU-bound work without `await`).
3. If the handler must legitimately take > 10s: reduce the heartbeat interval (`timeout=10.0` in `_heartbeat_loop`) or increase the Watchdog timeout (`Watchdog(timeout=60)`).
4. Do **not** simply increase the Watchdog timeout without also ensuring the heartbeat renewal runs more frequently — the gap must always be: `heartbeat_interval << watchdog_timeout`.
