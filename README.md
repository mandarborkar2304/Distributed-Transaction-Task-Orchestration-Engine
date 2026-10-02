[![CI](https://github.com/mandarborkar2304/Distributed-Transaction-Task-Orchestration-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/mandarborkar2304/Distributed-Transaction-Task-Orchestration-Engine/actions/workflows/ci.yml)

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
│                      Hybrid Ingestion Tier                              │
│                                                                         │
│   Client ──► POST /v1/jobs (Idempotency-Key header)                     │
│                │                                                        │
│                ├───────────────────────────────┐                        │
│                ▼ (:8080)                       ▼ (:8000)                │
│        ┌────────────────────────┐      ┌────────────────────────┐       │
│        │   Go Gateway           │      │   Python API (FastAPI) │       │
│        │   (Goroutines+pgxpool) │      │   (Compatibility tier) │       │
│        └───────────┬────────────┘      └───────────┬────────────┘       │
│                    │                               │                    │
│                    ▼                               ▼                    │
│        ┌────────────────────────────────────────────────────────┐       │
│        │   Redis 7 Fast-Path Idempotency (idemp:{key} TTL=24h)  │       │
│        │   ◄─────── HIT: return cached JSON (sub-millisecond)   │       │
│        └───────────────────────────┬────────────────────────────┘       │
│                                    │ MISS                               │
│                                    ▼                                    │
│        ┌────────────────────────────────────────────────────────┐       │
│        │   PostgreSQL 16 Transactional Outbox                   │       │
│        │   jobs + job_tasks (Range Partitioning Ready)          │       │
│        │   INSERT ... ON CONFLICT (idempotency_key) DO NOTHING  │       │
│        └───────────────────────────┬────────────────────────────┘       │
└────────────────────────────────────┼────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────┐
│                    Polyglot Worker Pool Tier                            │
│                                                                         │
│   Go Worker (cmd/worker) ─┐                                             │
│   Python Worker ──────────┼──► SELECT ... FOR UPDATE SKIP LOCKED        │
│                           │         │                                   │
│                           │         ▼                                   │
│             ┌─────────────┴────────────────────────┐                    │
│             │  Redis Distributed Lock (NX EX 60s)  │                    │
│             │  Atomic Lua CAS Unlock               │                    │
│             └──────────────────────┬───────────────┘                    │
│                                    │                                    │
│             ┌──────────────────────▼───────────────┐                    │
│             │  Task Execution + Heartbeat Ticker   │                    │
│             │  - Go native compute / payment       │                    │
│             │  - Delegated Python handlers         │                    │
│             │  (Heartbeat renewed every 10s)       │                    │
│             └──────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────┐
│                      Watchdog / Task Reaper                             │
│   Polls every 15s for RUNNING tasks where:                              │
│     heartbeat_at < NOW() - 30s  OR  heartbeat_at IS NULL                │
│   → Cleans stale Redis lock key                                         │
│   → retry_count < max_retries  → reset to PENDING, retry++              │
│   → retry_count >= max_retries → DEAD_LETTER, parent job → FAILED      │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────┐
│                   Prometheus Observability (/metrics)                   │
│   Go Gateway (:8080): gateway_requests_total, gateway_latency_seconds   │
│   Python API (:8000): orchestrator_tasks_total, execution_duration      │
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

### Comparative Performance: Python Gateway vs. Go Gateway (500 Concurrent VUs)

| Metric | Python Gateway (FastAPI / asyncpg) | Go Gateway (pgxpool / goroutines) | Architectural Improvement |
|---|---|---|---|
| **Peak Virtual Users (VUs)** | 500 | 500 | Enterprise Load Parity |
| **Sustained Throughput** | 158.31 req/s | **1,133.55 req/s** | **7.16x higher throughput** |
| **HTTP Success Rate** | 100.00% (0 errors) | **100.00% (0 errors)** | Zero failure rate |
| **Median Latency (p50)** | 685.07 ms | **366.01 ms** | **1.87x lower latency** |
| **Tail Latency (p95)** | 8,610 ms (8.61 s) | **966.86 ms (0.96 s)** | **8.91x lower tail latency** |
| **Tail Latency (p99)** | >10.0 s | **1,752.21 ms (1.75 s)** | **Sub-2s p99 under stress** |
| **Process RSS Memory** | 116 MB | **~15 MB** | **7.7x smaller memory footprint** |
| **User Keyspace Tested** | 500 static keys | **20,000,000 synthetic users** | Enterprise 20M+ footprint |
| **Idempotency Hit Rate** | 70.64% | **65.68%** | Fast-path deduplication |
| **Worker Queue Drain Rate**| ~40 tasks/s | **560 tasks/s (2 Go workers)** | **14x faster queue drain** |

### Go Gateway 20M User Scale Load Test (500 VUs)

| Metric | Value | Notes |
|---|---|---|
| Duration | 20 s | Sustained high-concurrency ingestion |
| Virtual Users | 500 | Concurrent goroutines |
| User Address Space | 20,000,000 | Synthetic keyspace (`user_<1-20000000>`) |
| Total Requests Handled | 22,671 | Ingestion calls |
| Throughput | **1,133.55 RPS** | Raw HTTP + DB outbox + Redis cache |
| HTTP Success Rate | **100.00%** | Zero 5xx, zero 4xx |
| Cache Hits (Redis) | 14,891 | 65.68% served sub-millisecond |
| New Jobs Inserted (PostgreSQL) | 7,780 | Transactional outbox rows |
| Latency p50 / p95 / p99 | 366.01 ms / 966.86 ms / 1.75 s | Sub-second p95 tail |

### Baseline k6 Load Test — Python Gateway (500 VUs)

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

### 3. Start Ingestion Tier

**Option A — High-Throughput Go Gateway (Port 8080, Recommended for Production):**
```bash
cd services/gateway
go run ./cmd/gateway/
```
Metrics available at `http://localhost:8080/metrics`.

**Option B — Python FastAPI Server (Port 8000, Compatibility Tier):**
```bash
uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 2
```
Metrics available at `http://localhost:8000/metrics`.

### 4. Submit a Job

```bash
# Ingest via Go Gateway
curl -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-unique-key-001" \
  -d '{"job_type": "payment", "handler_name": "payment_handler", "payload": {"amount": 99.99}}'
```

Repeat the same `Idempotency-Key` — the response returns `"cached": true` sub-millisecond from Redis without touching PostgreSQL.

### 5. Start Workers

**Option A — High-Performance Go Worker Pool (SKIP LOCKED + Redis Lock Leases):**
```bash
cd services/gateway
go run ./cmd/worker/
```

**Option B — Asyncio Python Worker Pool:**
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

### 8. Run 20M Scale Load Simulation

```bash
go run scripts/benchmarks/loadgen.go \
  -target http://localhost:8080 \
  -vus 500 \
  -duration 20s \
  -users 20000000 \
  -collision-rate 0.70 \
  -hot-pool 1000
```

---

## Project Structure

```
services/gateway/              # High-Throughput Go Layer
├── cmd/
│   ├── gateway/main.go        # HTTP Ingestion Gateway (goroutines, pgxpool, go-redis)
│   └── worker/main.go         # Go Worker claim loop (SKIP LOCKED, Redis locks, tickers)
├── internal/
│   ├── cache/cache.go         # Redis fast-path idempotency & Lua CAS lock
│   ├── db/db.go               # pgxpool connection pool (150 max conns), outbox SQL
│   ├── metrics/metrics.go     # Prometheus metrics (requests, latency, hits)
│   └── model/model.go         # Domain types mirroring PostgreSQL schema
├── Dockerfile.gateway         # Multi-stage scratch build (~5MB images)
└── go.mod, go.sum

src/                           # Python Coordination Layer
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
├── loadgen.go                 # 20M user scale Go load generator
├── k6_ingest.js               # k6 load script (500 VU ramp)
└── run_pgbench.sh             # pgbench idempotency + SKIP LOCKED scenarios

docs/
├── ARCHITECTURE.md            # System internals & technical deep-dive
├── API_REFERENCE.md           # REST API spec + Prometheus catalog
├── RUNBOOK.md                 # SRE operational guide + diagnostic SQL
├── BENCHMARK_RESULTS.md       # Empirical benchmark output and comparative analysis
└── migrations/
    └── 001_partition_job_tasks.sql # Range partitioning for 20M+ lifetime rows
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
