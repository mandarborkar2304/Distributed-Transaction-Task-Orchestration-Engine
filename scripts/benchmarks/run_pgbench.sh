#!/usr/bin/env bash
set -euo pipefail

DB_CONTAINER=$(docker ps -q -f ancestor=postgres:16-alpine | head -n 1)
if [ -z "$DB_CONTAINER" ]; then
    echo "Error: postgres:16-alpine container is not running!"
    exit 1
fi

echo "=========================================================="
echo " Running pgbench suite against live PostgreSQL container "
echo "=========================================================="

echo ""
echo "--- Scenario 1: Idempotency Key Index Lookup Benchmark (50 clients) ---"
docker exec -i "$DB_CONTAINER" bash -c 'cat << "EOF" > /tmp/pgbench_idemp.sql
\set key_id random(1, 5000)
SELECT id, status FROM jobs WHERE idempotency_key = '\''bench-key-'\'' || :key_id;
EOF
pgbench -U postgres -c 50 -j 2 -T 10 -f /tmp/pgbench_idemp.sql orchestrator
'

echo ""
echo "--- Scenario 2: Worker SKIP LOCKED Claiming Contention (40 clients) ---"
docker exec -i "$DB_CONTAINER" bash -c 'cat << "EOF" > /tmp/pgbench_skip_locked.sql
BEGIN;
SELECT id FROM job_tasks WHERE status = '\''PENDING'\'' ORDER BY created_at ASC LIMIT 10 FOR UPDATE SKIP LOCKED;
COMMIT;
EOF
pgbench -U postgres -c 40 -j 2 -T 10 -f /tmp/pgbench_skip_locked.sql orchestrator
'

echo "=========================================================="
echo " pgbench suite completed successfully! "
echo "=========================================================="
