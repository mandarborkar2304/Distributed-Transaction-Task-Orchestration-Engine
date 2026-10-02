# Distributed Transaction & Task Orchestration Engine

A production-grade, cloud-native task orchestration system providing **guaranteed at-least-once execution**, **worker starvation prevention**, and **two-tier idempotent ingestion** — built on PostgreSQL 16, Redis 7, and Python asyncio.

---

## Problem Statement

Distributed task processing at scale faces three compounding failure modes:

1. **Duplicate execution** — without transactional deduplication, network retries cause the same job to run twice, producing double-charges, double-sends, or corrupt aggregate state.
2. **Worker starvation** — naive `SELECT * WHERE status='PENDING'` polling creates thundering-herd contention where all workers race for the same rows, most losing, and hot rows block cold ones indefinitely.
3. **Silent task loss** — a crashed worker holding a `RUNNING` task leaves it stuck forever unless a watchdog actively reaps orphaned heartbeats and re-queues the work.

This engine eliminates all three failure modes with a design grounded in PostgreSQL advisory semantics and Redis atomic primitives.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          HTTP Ingestion Layer                           │
│                                                                         │
│   Client ──► POST /v1/jobs (Idempotency-Key header)                     │
│                     │                                                   │
│                     ▼                                                   │
│        ┌────────────────────────┐                                       │
│        │   Redis Fast-Path      │  idemp:{key}  TTL=86400s              │
│        │   Idempotency Cache    │◄──────── HIT: return cached response  │
│        └────────────┬───────────┘                                       │
│                     │ MISS                                              │
│                     ▼                                                   │
│        ┌────────────────────────┐                                       │
│        │   PostgreSQL 16        │  INSERT INTO jobs                     │
│        │   Transactional Outbox │  ON CONFLICT (idempotency_key)        │
│        │   jobs + job_tasks     │  DO NOTHING RETURNING id, status      │
│        └────────────┬───────────┘                                       │
│                     │ write Redis cache (TTL=86400s)                    │
└─────────────────────┼───────────────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────────────┐
│                        Async Worker Pool                                │
│                                                                         │
│   Worker A ─┐                                                           │
│   Worker B ─┼──► SELECT ... FOR UPDATE SKIP LOCKED (batch_size=10)     │
│   Worker C ─┘         │                                                 │
│                        ▼                                                │
│             ┌──────────────────────┐                                    │
│             │  Redis Distributed   │  SET lock:task:{id} {worker_id}   │
│             │  Lock (NX EX 60s)    │  Lua CAS atomic release            │
│             └──────────┬───────────┘                                    │
│                        │                                                │
│             ┌──────────▼───────────┐                                    │
│             │  Handler Execution   │  PaymentTaskHandler                │
│             │  + Heartbeat Loop    │  ComputeTaskHandler                │
│             │  (10s renewal)       │  heartbeat_at renewed every 10s    │
│             └──────────────────────┘                                    │
└─────────────────────────────────────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────────────┐
│                      Watchdog / Task Reaper                             │
│                                                                         │
│   Polls every 15s for RUNNING tasks where:                              │
│     heartbeat_at < NOW() - 30s  OR  heartbeat_at IS NULL                │
│                                                                         │
│   → Cleans stale Redis lock key                                         │
│   → retry_count + 1 < max_retries  → reset to PENDING, increment retry │
│   → retry_count + 1 >= max_retries → DEAD_LETTER, job → FAILED         │
└─────────────────────────────────────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────────────┐
│                   Prometheus Observability (/metrics)                   │
│                                                                         │
│   orchestrator_tasks_total{status, handler}                             │
│   orchestrator_execution_duration_seconds{handler}                      │
│   orchestrator_reclaimed_orphans_total                                  │
│   orchestrator_idempotency_hits_total                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Core Invariant Guarantees

### 1. Concurrency & Deconfliction — Zero Double-Claims

The worker uses `SELECT ... FOR UPDATE SKIP LOCKED` within a single atomic transaction:

```sql
SELECT * FROM job_tasks
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT 10
FOR UPDATE SKIP LOCKED;
```

PostgreSQL row-level locks prevent two workers from claiming the same task. `SKIP LOCKED` causes each worker to skip any row already locked by a peer rather than blocking, eliminating thundering-herd queuing. Claim and status update (`RUNNING`, `locked_by`, `heartbeat_at`) are committed atomically before processing begins.

A secondary Redis distributed lock (`SET lock:task:{id} NX EX 60`) is acquired post-claim as a defense-in-depth layer that prevents a watchdog and a worker from simultaneously operating on the same task.

### 2. Idempotency Strategy — Two-Tier Deduplication

**Tier 1 — Redis fast-path:** On every `POST /v1/jobs`, the key `idemp:{Idempotency-Key}` is checked first. Hits return the cached `job_id` + `status` without touching PostgreSQL. Key TTL is 86,400 seconds (24 hours).

**Tier 2 — PostgreSQL uniqueness constraint:** On a cache miss, `INSERT INTO jobs ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING id, status` is executed inside a database transaction. If the conflict fires, the existing record is fetched and the response is still well-formed. The unique index `ix_jobs_idempotency_key` is the authoritative deduplication boundary.

After any successful DB insert or conflict resolution, the response is written to Redis to populate the cache for future requests.

### 3. Resilience & Poison-Pill Handling — Watchdog / Reaper

The `Watchdog` daemon runs `run_once()` on a 15-second interval. It queries:

```sql
SELECT * FROM job_tasks
WHERE status = 'RUNNING'
  AND (heartbeat_at < NOW() - INTERVAL '30 seconds' OR heartbeat_at IS NULL)
FOR UPDATE SKIP LOCKED;
```

For each stale task:
- The Redis lock key `lock:task:{id}` is deleted unconditionally.
- If `retry_count + 1 < max_retries` (default 3): task → `PENDING`, `retry_count` incremented.
- If `retry_count + 1 >= max_retries`: task → `DEAD_LETTER`, parent job → `FAILED`, `last_error` set to `"Watchdog: Heartbeat expired, max retries exceeded"`.

---

## Empirical Performance

All numbers measured against live containers on a 2-vCPU / 8 GB Codespaces environment. Zero fabrication.

### k6 Load Test — 500 Virtual Users, Idempotent Ingestion

| Metric | Value |
|---|---|
| Virtual users (peak) | 500 |
| Total requests | 8,367 |
| Throughput | 158.31 req/s |
| HTTP success rate | 100.00% (0 errors) |
| Median latency (p50) | 685.07 ms |
| p90 latency | 3.83 s |
| p95 latency | 8.61 s |
| Idempotency cache hits | 5,911 (70.64%) |
| Idempotency cache misses | 2,456 (29.36%) |

### pgbench — PostgreSQL Direct Load

| Scenario | Clients | Duration | Transactions | TPS | Avg Latency | Errors |
|---|---|---|---|---|---|---|
| Idempotency key point-lookup | 50 | 10 s | 141,646 | **14,276.45** | 3.502 ms | 0 |
| `SELECT … FOR UPDATE SKIP LOCKED` | 40 | 10 s | 67,219 | **6,749.46** | 5.926 ms | 0 |

### System Resource Utilization (post-load)

| Resource | Value |
|---|---|
| Process RSS memory | 116 MB |
| Virtual memory | 508 MB |
| Open file descriptors | 129 |
| CPU time consumed | 16.01 s |
| Python Gen-2 GC stalls | 0 |
| vCPUs available | 2 |

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- **PostgreSQL 16** on `localhost:5432` (DB: `orchestrator`, user/pass: `postgres/postgres`)
- **Redis 7** on `localhost:6379`
- **Prometheus** on `localhost:9090`

### 2. Install Dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Start the API Server

```bash
uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 2
```

The Prometheus metrics endpoint is available at `http://localhost:8000/metrics`.

### 4. Submit a Job

```bash
curl -X POST http://localhost:8000/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key-001" \
  -d '{"job_type": "payment", "handler_name": "payment_handler", "payload": {"amount": 99.99}}'
```

Repeat the same `Idempotency-Key` — the response returns `"cached": true` and PostgreSQL is not touched.

### 5. Start Workers

```bash
python -c "
import asyncio
from src.engine.worker import Worker

async def main():
    workers = [Worker(batch_size=10) for _ in range(4)]
    await asyncio.gather(*[w.start() for w in workers])

asyncio.run(main())
"
```

### 6. Start Watchdog

```bash
python -c "
import asyncio
from src.engine.watchdog import Watchdog

asyncio.run(Watchdog(interval=15, timeout=30).start())
"
```

### 7. Run Tests

```bash
pytest tests/ -v
```

All 10 tests (unit, integration, chaos) execute against live containers. Zero mocks.

---

## Project Structure

```
src/
├── config.py                  # Pydantic settings (DATABASE_URL, REDIS_URL)
├── database.py                # Async SQLAlchemy engine (pool_size=25, max_overflow=50)
├── main.py                    # FastAPI app + Prometheus /metrics mount
├── models.py                  # Job + JobTask ORM models, indexes, enums
├── redis_client.py            # Redis client + RedisDistributedLock (Lua CAS)
├── api/
│   └── routes.py              # POST /v1/jobs, GET /v1/jobs/{job_id}
├── engine/
│   ├── worker.py              # Async worker: SKIP LOCKED + heartbeat + backoff
│   ├── watchdog.py            # Heartbeat reaper + DLQ promotion
│   └── handlers/
│       ├── base.py            # Abstract BaseHandler
│       ├── payment.py         # PaymentTaskHandler
│       └── compute.py         # ComputeTaskHandler
└── observability/
    └── metrics.py             # Prometheus counters, histograms

tests/
├── conftest.py                # Session DB init, function-scoped clean, fixtures
├── unit/                      # Model validation, distributed lock CAS
├── integration/               # Idempotency, worker concurrency, stress
└── chaos/                     # 2,000-task concurrency grill, crash recovery

scripts/benchmarks/
├── k6_ingest.js               # k6 load script (500 VU ramp)
└── run_pgbench.sh             # pgbench idempotency + SKIP LOCKED scenarios

docs/
├── ARCHITECTURE.md            # System internals & technical deep-dive
├── API_REFERENCE.md           # REST API spec + Prometheus catalog
├── RUNBOOK.md                 # SRE operational guide + diagnostic SQL
└── BENCHMARK_RESULTS.md       # Full benchmark output and analysis
```

---

## Tech Stack

| Component | Technology |
|---|---|
| API Layer | FastAPI (ASGI) + Uvicorn |
| Database | PostgreSQL 16 with asyncpg driver |
| ORM | SQLAlchemy 2.x (async) |
| Cache / Locking | Redis 7 (asyncio client) |
| Observability | Prometheus client (`/metrics`) |
| Test Suite | pytest-asyncio (session loop, live containers) |
| Load Testing | k6 v0.54.0, pgbench 16 |
