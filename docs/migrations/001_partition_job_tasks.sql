-- ============================================================================
-- Migration: Partitioning Readiness for job_tasks at 20M+ Lifetime Rows
-- ============================================================================
-- Run this migration when the job_tasks table exceeds ~5M rows or when
-- p95 worker poll latency begins to climb above 10ms.
--
-- Strategy: Range partitioning by created_at (monthly buckets) allows
-- partition pruning on the most common filter: WHERE status IN ('PENDING',
-- 'RUNNING') ORDER BY created_at, which naturally isolates to the current
-- and previous month's partitions.
--
-- Alternative: Hash partitioning on job_id distributes write load more
-- evenly but cannot prune on created_at range scans.
--
-- IMPORTANT: PostgreSQL declarative partitioning requires the table to be
-- empty before conversion. Run this during a maintenance window or use
-- pg_partman for zero-downtime partitioning.
-- ============================================================================

BEGIN;

-- Step 1: Rename the existing table to a staging name.
ALTER TABLE job_tasks RENAME TO job_tasks_legacy;

-- Step 2: Create the new partitioned parent table (same schema).
CREATE TABLE job_tasks (
    id            UUID        NOT NULL,
    job_id        UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    handler_name  VARCHAR(64) NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'PENDING',
    payload       JSONB       NOT NULL DEFAULT '{}',
    retry_count   INTEGER     NOT NULL DEFAULT 0,
    max_retries   INTEGER     NOT NULL DEFAULT 3,
    locked_by     VARCHAR(64),
    heartbeat_at  TIMESTAMPTZ,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)   -- partition key must be part of PK
) PARTITION BY RANGE (created_at);

-- Step 3: Create monthly partitions (extend as needed via pg_partman or cron).
CREATE TABLE job_tasks_2026_01 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE job_tasks_2026_02 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE job_tasks_2026_03 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE job_tasks_2026_04 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE job_tasks_2026_05 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE job_tasks_2026_06 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE job_tasks_2026_07 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE job_tasks_2026_08 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE job_tasks_2026_09 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE job_tasks_2026_10 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE TABLE job_tasks_2026_11 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

CREATE TABLE job_tasks_2026_12 PARTITION OF job_tasks
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- Catch-all partition for overflow (prevents inserts from failing on boundary miss).
CREATE TABLE job_tasks_default PARTITION OF job_tasks DEFAULT;

-- Step 4: Rebuild all operational indexes on the partitioned table.
-- Each index is created globally; PostgreSQL propagates to all partitions.

CREATE INDEX ix_job_tasks_status_created_at
    ON job_tasks (status, created_at);

CREATE INDEX ix_job_tasks_pending_running
    ON job_tasks (status, created_at)
    WHERE status IN ('PENDING', 'RUNNING');

CREATE INDEX ix_job_tasks_job_id
    ON job_tasks (job_id);

CREATE INDEX ix_job_tasks_heartbeat_at
    ON job_tasks (heartbeat_at)
    WHERE heartbeat_at IS NOT NULL;

-- Step 5: Migrate existing data from the legacy table.
INSERT INTO job_tasks SELECT * FROM job_tasks_legacy;

-- Step 6: Drop the legacy table after verifying row counts match.
-- Uncomment and execute manually after COUNT(*) verification:
-- DROP TABLE job_tasks_legacy;

COMMIT;

-- ============================================================================
-- Verification queries (run after migration):
-- ============================================================================
-- SELECT COUNT(*) FROM job_tasks_legacy;
-- SELECT COUNT(*) FROM job_tasks;
-- SELECT tableoid::regclass, count(*) FROM job_tasks GROUP BY 1 ORDER BY 1;
-- ============================================================================
