#!/usr/bin/env bash
# run_pgbench_hardened.sh
# Hardened pgbench suite: 100-client queue contention & outbox saturation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"
DB_USER="${PGUSER:-postgres}"
DB_NAME="${PGDATABASE:-orchestrator}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

CLIENTS=100
THREADS=4
DURATION=60

echo "================================================================="
echo "   Hardened PostgreSQL Contention & Saturation Benchmark"
echo "================================================================="
echo "Host: ${DB_HOST}:${DB_PORT} | DB: ${DB_NAME} | User: ${DB_USER}"
echo "Concurrency: ${CLIENTS} clients across ${THREADS} threads | Duration: ${DURATION}s"
echo "Timestamp: $(date -u '+%Y-%m-%d %H:%M:%SZ')"
echo ""

# ─────────────────────────────────────────────────────────────────
# PREPARATION: Seed pending tasks for Queue Contention Test
# ─────────────────────────────────────────────────────────────────
echo "==> Preparing database: Seeding 100,000 PENDING tasks for contention benchmark..."
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" << 'EOF'
-- Clean previous benchmark artifacts
DELETE FROM jobs WHERE job_type IN ('pgbench_contention', 'outbox_bench');

-- Seed 100,000 parent jobs
INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
SELECT gen_random_uuid(), 'contention_job_' || g, 'pgbench_contention', 'PENDING', clock_timestamp()
FROM generate_series(1, 100000) g
ON CONFLICT (idempotency_key) DO NOTHING;

-- Seed 100,000 corresponding job_tasks
INSERT INTO job_tasks (id, job_id, handler_name, status, payload, retry_count, max_retries, created_at, updated_at)
SELECT gen_random_uuid(), id, 'compute_handler', 'PENDING', '{"contention":true}'::jsonb, 0, 3, clock_timestamp(), clock_timestamp()
FROM jobs WHERE job_type = 'pgbench_contention'
ON CONFLICT DO NOTHING;
EOF

PENDING_COUNT=$(psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM job_tasks WHERE status = 'PENDING';")
echo "  Seeded tasks ready. Total PENDING tasks: ${PENDING_COUNT// /}"

# ─────────────────────────────────────────────────────────────────
# TEST 1: Aggressive Queue Worker Contention (SKIP LOCKED + UPDATE)
# ─────────────────────────────────────────────────────────────────
echo ""
echo "================================================================="
echo "TEST 1: 100 Concurrent Clients — Queue Contention (SKIP LOCKED)"
echo "Query: SELECT ... FOR UPDATE SKIP LOCKED + UPDATE to RUNNING"
echo "================================================================="

SCRIPT_T1=$(mktemp /tmp/pgbench_t1_XXXXXX.sql)
cat << 'EOF' > "${SCRIPT_T1}"
\set worker_id random(1, 100)
BEGIN;
UPDATE job_tasks
SET status = 'RUNNING',
    locked_by = 'worker_' || :worker_id,
    heartbeat_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE id = (
    SELECT id FROM job_tasks
    WHERE status = 'PENDING'
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
);
COMMIT;
EOF

OUT_T1="${RESULTS_DIR}/pgbench_contention_100clients.txt"
pgbench -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
  -f "${SCRIPT_T1}" \
  -c "${CLIENTS}" \
  -j "${THREADS}" \
  -T "${DURATION}" \
  -P 5 \
  -r \
  2>&1 | tee "${OUT_T1}"

rm -f "${SCRIPT_T1}"

# Check post-test task status
CLAIMED_COUNT=$(psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -t -c "SELECT count(*) FROM job_tasks WHERE status = 'RUNNING' AND locked_by LIKE 'worker_%';")
echo "  Total tasks claimed and transitioned to RUNNING: ${CLAIMED_COUNT// /}"

# ─────────────────────────────────────────────────────────────────
# TEST 2: Outbox Ingestion Saturation (Multi-Table Atomic Write)
# ─────────────────────────────────────────────────────────────────
echo ""
echo "================================================================="
echo "TEST 2: 100 Concurrent Clients — Outbox Ingestion Saturation"
echo "Query: Atomic INSERT INTO jobs ON CONFLICT + INSERT INTO job_tasks"
echo "================================================================="

SCRIPT_T2=$(mktemp /tmp/pgbench_t2_XXXXXX.sql)
cat << 'EOF' > "${SCRIPT_T2}"
\set idemp random(1, 20000000)
BEGIN;
INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
VALUES (gen_random_uuid(), 'bench_outbox_' || :idemp, 'outbox_bench', 'PENDING', clock_timestamp())
ON CONFLICT (idempotency_key) DO NOTHING;

INSERT INTO job_tasks (id, job_id, handler_name, status, payload, retry_count, max_retries, created_at, updated_at)
SELECT gen_random_uuid(), id, 'compute_handler', 'PENDING', '{"load":true}'::jsonb, 0, 3, clock_timestamp(), clock_timestamp()
FROM jobs WHERE idempotency_key = 'bench_outbox_' || :idemp
ON CONFLICT DO NOTHING;
COMMIT;
EOF

OUT_T2="${RESULTS_DIR}/pgbench_outbox_saturation_100clients.txt"
pgbench -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
  -f "${SCRIPT_T2}" \
  -c "${CLIENTS}" \
  -j "${THREADS}" \
  -T "${DURATION}" \
  -P 5 \
  -r \
  2>&1 | tee "${OUT_T2}"

rm -f "${SCRIPT_T2}"

# ─────────────────────────────────────────────────────────────────
# DATABASE HEALTH AUDIT: Deadlocks, Conflicts, Rollbacks
# ─────────────────────────────────────────────────────────────────
echo ""
echo "================================================================="
echo "POST-BENCHMARK POSTGRESQL STATS & INTEGRITY AUDIT"
echo "================================================================="
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" << 'EOF'
SELECT 
    datname,
    numbackends AS active_connections,
    xact_commit AS total_commits,
    xact_rollback AS total_rollbacks,
    conflicts AS total_conflicts,
    deadlocks AS total_deadlocks
FROM pg_stat_database 
WHERE datname = 'orchestrator';

SELECT 
    schemaname,
    relname AS table_name,
    n_tup_ins AS inserted_tuples,
    n_tup_upd AS updated_tuples,
    n_tup_hot_upd AS hot_updated_tuples
FROM pg_stat_user_tables 
WHERE relname IN ('jobs', 'job_tasks');
EOF

echo ""
echo "Hardened pgbench suite completed successfully. Results saved to: ${RESULTS_DIR}"
