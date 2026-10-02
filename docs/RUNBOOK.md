# Runbook — SRE & Operational Guide

This runbook documents operational procedures, troubleshooting workflows, diagnostic SQL recipes, and alert remediation playbooks for the polyglot (Go + Python) Distributed Transaction & Task Orchestration Engine.

---

## Table of Contents

1. [Operational Procedures](#1-operational-procedures)
   - [1.1 Diagnosing `pgxpool` Connection Starvation](#11-diagnosing-pgxpool-connection-starvation)
   - [1.2 Investigating Goroutine Leaks in the Go Gateway](#12-investigating-goroutine-leaks-in-the-go-gateway)
   - [1.3 Partition Maintenance: Attaching & Detaching Monthly Tables](#13-partition-maintenance-attaching--detaching-monthly-tables)
   - [1.4 Investigating Stuck Tasks & Cross-Runtime Worker Crashes](#14-investigating-stuck-tasks--cross-runtime-worker-crashes)
   - [1.5 Redis Failover & Cache Degraded State](#15-redis-failover--cache-degraded-state)
   - [1.6 Clearing & Replaying Dead-Lettered (DLQ) Tasks](#16-clearing--replaying-dead-lettered-dlq-tasks)
2. [Diagnostic SQL Recipes](#2-diagnostic-sql-recipes)
   - [2.1 Partition Health & Row Distribution](#21-partition-health--row-distribution)
   - [2.2 Active Worker Leases (Go vs. Python Runtimes)](#22-active-worker-leases-go-vs-python-runtimes)
   - [2.3 Stale Heartbeat Detection (>30s)](#23-stale-heartbeat-detection-30s)
   - [2.4 Lock Contention & Worker Throughput](#24-lock-contention--worker-throughput)
   - [2.5 Index Scan Verification](#25-index-scan-verification)
3. [Alert Response Playbooks](#3-alert-response-playbooks)

---

## 1. Operational Procedures

### 1.1 Diagnosing `pgxpool` Connection Starvation

**Symptom:**
- HTTP ingestion latency on `:8080` spikes above 2,000 ms.
- Go Gateway logs contain `context deadline exceeded` or `conn pool acquisition timeout`.
- Prometheus metric `gateway_latency_seconds{handler="create_job"}` p95 tail shifts toward 4–5 seconds.

**Root Cause:**
The Go Gateway is configured with `pgxpool.Config{MaxConns: 150, MinConns: 25}`. If concurrent client load exceeds available connections and transactions take longer than normal (e.g. slow disk I/O, heavy lock contention), requests queue up in-memory waiting for an available connection lease.

**Diagnosis Steps:**

1. **Check PostgreSQL Active Connections vs. Backend Limit**:
   ```sql
   SELECT count(*) AS total_conns,
          current_setting('max_connections')::int AS max_conns,
          round(100.0 * count(*) / current_setting('max_connections')::int, 2) AS pct_used
   FROM pg_stat_activity;
   ```

2. **Inspect Connection States for the `orchestrator` Database**:
   ```sql
   SELECT state, wait_event_type, wait_event, count(*)
   FROM pg_stat_activity
   WHERE datname = 'orchestrator'
   GROUP BY state, wait_event_type, wait_event
   ORDER BY count(*) DESC;
   ```

3. **Identify Queries Holding Open Transactions**:
   ```sql
   SELECT pid, now() - xact_start AS xact_age, query
   FROM pg_stat_activity
   WHERE datname = 'orchestrator'
     AND state != 'idle'
     AND xact_start IS NOT NULL
   ORDER BY xact_age DESC
   LIMIT 5;
   ```

**Remediation:**
- If transactions are stalled on a locks: soft-cancel the blocking backend with `SELECT pg_cancel_backend(<pid>);`.
- If connection pool demand is legitimately exceeding 150 connections due to traffic spikes:
  1. Increase `max_connections` in PostgreSQL if server RAM allows:
     ```sql
     ALTER SYSTEM SET max_connections = 750;
     SELECT pg_reload_conf();
     ```
  2. Scale horizontally by provisioning an additional Go gateway instance behind an external load balancer rather than inflating a single node's `MaxConns` beyond 200.

---

### 1.2 Investigating Goroutine Leaks in the Go Gateway

**Symptom:**
- Process memory (RSS) climbs monotonically without stabilizing post-garbage collection.
- `go_goroutines` metric on `:8080/metrics` continuously increases without returning to baseline (baseline is ~10–20 idle goroutines).

**Diagnosis Steps:**

1. **Inspect Active Goroutine Count via Prometheus**:
   ```promql
   go_goroutines{job="gateway"}
   ```

2. **Capture Goroutine Stack Dump**:
   Execute a SIGABRT or curl the diagnostic debug endpoint (if enabled) or use GDB/Delve:
   ```bash
   # Check thread and file descriptor counts for the binary:
   ps -u $USER -L -o pid,tid,class,rtprio,ni,pri,psr,stat,wchan:14,comm
   ls -la /proc/$(pgrep gateway)/fd | wc -l
   ```

3. **Verify Channel and Ticker Closure**:
   Ensure all `time.NewTicker` instances have corresponding `defer ticker.Stop()` and `heartbeatDone` channel close events. In `services/gateway/cmd/worker/main.go`, verify:
   ```go
   heartbeatDone := make(chan struct{})
   defer close(heartbeatDone)
   ```
   A missing `close(heartbeatDone)` will leave the heartbeat ticker goroutine running indefinitely after task completion.

---

### 1.3 Partition Maintenance: Attaching & Detaching Monthly Tables

When [`docs/migrations/001_partition_job_tasks.sql`](migrations/001_partition_job_tasks.sql) is active, monthly partitions must be provisioned ahead of time to avoid routing traffic to `job_tasks_default`.

#### 1. Provision New Future Monthly Partition
Run this at least 7 days before the start of a new calendar month:

```sql
BEGIN;

-- Example: Provision partition for January 2027
CREATE TABLE IF NOT EXISTS job_tasks_2027_01 PARTITION OF job_tasks
    FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');

COMMIT;
```

#### 2. Detach Old Historical Partition (Data Retention Archival)
To archive or truncate completed tasks older than 6 months without locking the active table:

```sql
BEGIN;

-- Detach partition concurrently (leaves table standalone without dropping data)
ALTER TABLE job_tasks DETACH PARTITION job_tasks_2026_01 CONCURRENTLY;

-- Optional: Export to cold storage or drop table to reclaim NVMe storage
DROP TABLE job_tasks_2026_01;

COMMIT;
```

---

### 1.4 Investigating Stuck Tasks & Cross-Runtime Worker Crashes

**Symptom:**
- A task remains in `status = 'RUNNING'` for over 60 seconds.
- Neither Go nor Python workers are making progress on it.

**Step 1 — Identify the Stalled Task & Responsible Worker**:
```sql
SELECT id, job_id, handler_name, locked_by, heartbeat_at,
       EXTRACT(EPOCH FROM (NOW() - heartbeat_at)) AS seconds_stale,
       retry_count, max_retries
FROM job_tasks
WHERE status = 'RUNNING'
ORDER BY heartbeat_at ASC NULLS FIRST
LIMIT 10;
```

**Step 2 — Inspect Redis Distributed Lock**:
```bash
redis-cli GET "lock:task:<task_uuid>"
# Returns worker UUID if lock lease is active; (nil) if expired
redis-cli TTL "lock:task:<task_uuid>"
# Returns seconds remaining; -2 indicates key expired/deleted
```

**Step 3 — Watchdog Verification**:
The autonomous Python Watchdog sweeps every 15 seconds. If `heartbeat_at < NOW() - INTERVAL '30 seconds'`, the Watchdog automatically:
1. Deletes the Redis lock key `lock:task:<task_uuid>`.
2. Resets the task to `PENDING` (if `retry_count + 1 < max_retries`).
3. Clears `locked_by` and `heartbeat_at`.

If the Watchdog is stopped or disconnected, manually trigger recovery:
```sql
BEGIN;

UPDATE job_tasks
SET status = 'PENDING',
    retry_count = retry_count + 1,
    locked_by = NULL,
    heartbeat_at = NULL,
    last_error = 'Manual recovery: worker crash confirmed by SRE'
WHERE id = '<task_uuid>'
  AND status = 'RUNNING';

COMMIT;
```

---

### 1.5 Redis Failover & Cache Degraded State

**Symptom:**
- Gateway logs show `redis cache write error` or `dial tcp :6379: connect: connection refused`.
- Idempotency cache hit rate drops to 0%.

**Operational Resilience:**
The Go Gateway implements resilient fallthrough:
- Redis socket timeouts or connection refusals trigger a structured WARN log.
- Ingestion falls through to the PostgreSQL transactional outbox (`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`).
- Zero HTTP 500 errors are returned to clients.
- When Redis recovers, sub-millisecond fast-path deduplication restores immediately without restarting services.

---

### 1.6 Clearing & Replaying Dead-Lettered (DLQ) Tasks

**Step 1 — Inspect Dead-Letter Queue**:
```sql
SELECT jt.id AS task_id, jt.job_id, jt.handler_name, jt.retry_count, jt.last_error, jt.updated_at
FROM job_tasks jt
WHERE jt.status = 'DEAD_LETTER'
ORDER BY jt.updated_at DESC
LIMIT 50;
```

**Step 2 — Replay Fixed Tasks**:
After deploying a bug fix or recovering an external dependency:
```sql
BEGIN;

UPDATE job_tasks
SET status = 'PENDING',
    retry_count = 0,
    locked_by = NULL,
    heartbeat_at = NULL,
    last_error = NULL
WHERE id = '<task_uuid>'
  AND status = 'DEAD_LETTER';

UPDATE jobs
SET status = 'PENDING'
WHERE id = (SELECT job_id FROM job_tasks WHERE id = '<task_uuid>');

COMMIT;
```

---

## 2. Diagnostic SQL Recipes

### 2.1 Partition Health & Row Distribution
Checks row count distribution across all monthly partitions:

```sql
SELECT
    inhrelid::regclass AS partition_name,
    c.reltuples::bigint AS estimated_row_count,
    pg_size_pretty(pg_total_relation_size(inhrelid)) AS total_size
FROM pg_inherits
JOIN pg_class c ON c.oid = inhrelid
WHERE inhparent = 'job_tasks'::regclass
ORDER BY partition_name;
```

### 2.2 Active Worker Leases (Go vs. Python Runtimes)
Monitors how many tasks each worker instance currently holds in flight:

```sql
SELECT
    locked_by,
    count(*) AS active_tasks,
    min(heartbeat_at) AS oldest_heartbeat,
    max(heartbeat_at) AS newest_heartbeat,
    round(extract(epoch from (now() - min(heartbeat_at)))::numeric, 1) AS max_staleness_sec
FROM job_tasks
WHERE status = 'RUNNING'
GROUP BY locked_by
ORDER BY active_tasks DESC;
```

### 2.3 Stale Heartbeat Detection (>30s)
Identifies tasks that have missed 3 consecutive heartbeat cycles:

```sql
SELECT id, job_id, handler_name, locked_by, heartbeat_at,
       round(extract(epoch from (now() - heartbeat_at))::numeric, 1) AS seconds_stale
FROM job_tasks
WHERE status = 'RUNNING'
  AND (heartbeat_at < now() - interval '30 seconds' OR heartbeat_at IS NULL)
ORDER BY heartbeat_at ASC NULLS FIRST;
```

### 2.4 Lock Contention & Worker Throughput
Measures task completion throughput per minute over the last 15 minutes:

```sql
SELECT
    date_trunc('minute', updated_at) AS minute,
    count(*) AS completed_tasks,
    count(DISTINCT locked_by) AS active_workers
FROM job_tasks
WHERE status = 'COMPLETED'
  AND updated_at > now() - interval '15 minutes'
GROUP BY 1
ORDER BY minute DESC;
```

### 2.5 Index Scan Verification
Verifies that query planner is actively using partial hot-path indexes:

```sql
SELECT
    indexrelname AS index_name,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE relname IN ('jobs', 'job_tasks')
ORDER BY idx_scan DESC;
```

---

## 3. Alert Response Playbooks

### Alert: `GatewayLatencyHigh` (p95 > 2.0s over 5m)
- **Severity**: Warning
- **Playbook**:
  1. Check `pg_stat_activity` for connection pool saturation (§1.1).
  2. Verify Redis fast-path hit rate. If hit rate dropped from ~70% to 0%, check Redis health (§1.5).
  3. Verify PostgreSQL storage I/O and CPU utilization.

### Alert: `WatchdogOrphanReclamationSpike` (> 0.033 / sec over 5m)
- **Severity**: Critical
- **Playbook**:
  1. Execute Stale Heartbeat query (§2.3) to see which workers are dropping tasks.
  2. Inspect worker host logs for out-of-memory (OOM) kills or segmentation faults.
  3. Verify network connectivity between worker hosts and PostgreSQL.

### Alert: `DeadLetterQueueGrowth` (> 5 / min)
- **Severity**: Critical
- **Playbook**:
  1. Query DLQ failure reasons:
     ```sql
     SELECT last_error, count(*) FROM job_tasks WHERE status = 'DEAD_LETTER' GROUP BY last_error ORDER BY count(*) DESC;
     ```
  2. If errors cite third-party upstream API timeouts (e.g. payment gateway), check third-party status.
  3. If errors cite code exceptions, page backend engineering team with stack trace from `last_error`.
