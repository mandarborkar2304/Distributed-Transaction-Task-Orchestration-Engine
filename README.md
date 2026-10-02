[![CI](https://github.com/mandarborkar2304/Distributed-Transaction-Task-Orchestration-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/mandarborkar2304/Distributed-Transaction-Task-Orchestration-Engine/actions/workflows/ci.yml)

# Distributed Transaction & Task Orchestration Engine

A production-grade, polyglot (Go + Python) cloud-native task orchestration system engineered for **20,000,000+ user scale**. Delivers **guaranteed at-least-once execution**, **worker starvation prevention**, and **two-tier idempotent deduplication** — powered by PostgreSQL 16, Redis 7, Go concurrency runtimes, and Python asyncio.

---

## Problem Statement

Distributed task processing architectures at enterprise scale face three compounding failure modes:

1. **Duplicate Execution**: Without atomic deduplication at the boundary, network retries and webhook replay storms trigger duplicate job creation, causing duplicate charges, double notification sends, or corrupt downstream state.
2. **Worker Starvation & Lock Contention**: Naive polling (`SELECT * WHERE status='PENDING'`) causes severe lock contention where hundreds of worker threads contend for identical rows, causing database serialization failures, deadlocks, and worker starvation.
3. **Silent Task Loss**: A worker that crashes, encounters an OOM event, or is partitioned mid-execution leaves tasks stranded in `RUNNING` status indefinitely unless an autonomous watchdog detects stale heartbeats, reclaims distributed locks, and routes poison tasks to a Dead-Letter Queue (DLQ).

This engine eliminates all three failure modes with a hardened architecture combining PostgreSQL row-level advisory semantics (`SELECT ... FOR UPDATE SKIP LOCKED`), Redis Lua compare-and-swap (CAS) distributed locks, and a high-performance Go acceleration gateway.

---

## Dual-Plane System Architecture

```mermaid
flowchart TD
    subgraph ClientLayer [External Clients & Webhook Emitters]
        Client["Client / Webhook Emitters<br/><i>(POST /v1/jobs with Idempotency-Key)</i>"]
    end

    subgraph IngestionPlane [Ingestion Plane]
        GoGW["Go Ingestion Gateway (:8080)<br/>• Goroutine-per-request M:N scheduling<br/>• pgxpool (Max: 150, Min: 25)<br/>• go-redis multiplexed pool (500 conns)"]
        PyAPI["Python FastAPI Gateway (:8000)<br/>• Uvicorn multi-worker cluster<br/>• SQLAlchemy asyncpg pool<br/>• Compatibility & Admin Tier"]
    end

    subgraph StorageState [State & Storage Plane]
        Redis["Redis 7 (Standalone / Cluster)<br/>• Fast-Path Deduplication (idemp:{key} TTL 24h)<br/>• Distributed Lock Leases (lock:task:{id} EX 60s)<br/>• Atomic Lua CAS Unlock Script"]
        Postgres["PostgreSQL 16 Engine<br/>• Transactional Outbox (jobs + job_tasks)<br/>• Unique Index: ix_jobs_idempotency_key<br/>• Partial Index: ix_job_tasks_pending_running<br/>• Monthly Range Partitioning (20M+ rows)"]
    end

    subgraph WorkerPlane [Worker Plane]
        GoWorkers["Go Worker Pool (cmd/worker)<br/>• Batch Claim (50 tasks): SKIP LOCKED<br/>• Autonomous time.NewTicker Heartbeats<br/>• Native Handlers (Compute / Payment)"]
        PyWorkers["Python Worker Sandbox<br/>• Asyncio claim loop (10 tasks): SKIP LOCKED<br/>• Heartbeat coroutine (10s intervals)<br/>• Delegated Python Handlers"]
    end

    subgraph RecoveryPlane [Observability & Recovery Plane]
        Watchdog["Python Watchdog / Reaper Daemon<br/>• Sweeps every 15s for stale heartbeats (>30s)<br/>• Unconditional Redis lock eviction<br/>• Exponential backoff & DLQ promotion"]
        Prometheus["Prometheus Observability Engine (:9090)<br/>• Scrapes Go Gateway (:8080/metrics)<br/>• Scrapes Python API (:8000/metrics)"]
    end

    Client -->|High Throughput| GoGW
    Client -->|Admin / Compatibility| PyAPI

    GoGW -->|1. Check idemp:{key}| Redis
    GoGW -->|2. Miss: Atomic Outbox Write| Postgres
    PyAPI -->|1. Check idemp:{key}| Redis
    PyAPI -->|2. Miss: Atomic Outbox Write| Postgres

    GoWorkers -->|Atomic Batch Claim| Postgres
    GoWorkers -->|Lease Lock & Heartbeat| Redis
    PyWorkers -->|Atomic Batch Claim| Postgres
    PyWorkers -->|Lease Lock & Heartbeat| Redis

    Watchdog -->|Reap Stale Tasks| Postgres
    Watchdog -->|Evict Stale Locks| Redis

    Prometheus -.->|Scrape Metrics| GoGW
    Prometheus -.->|Scrape Metrics| PyAPI
```

---

## Core Invariant Guarantees

### 1. Cross-Runtime Deconfliction — Zero Double-Claims

Both the Go Worker pool (`cmd/worker`) and Python Workers (`src/engine/worker.py`) interoperate concurrently over the same PostgreSQL `job_tasks` table without conflicts or double-processing:

```sql
SELECT id, job_id, handler_name, status, payload, retry_count, max_retries
FROM job_tasks
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

- **Row-Level Mutual Exclusion**: PostgreSQL row locks prevent any two workers (Go or Python) from claiming the same task.
- **Starvation & Queueing Elimination**: `SKIP LOCKED` skips rows currently locked by competing workers instantly, ensuring workers never block each other.
- **Atomic Transition**: Claim and transition to `RUNNING` (with `locked_by = worker_id` and `heartbeat_at = NOW()`) occur in the same database transaction.
- **Secondary Redis Lock Lease**: Upon database claim, an atomic distributed lock lease is acquired in Redis (`SET lock:task:{id} {worker_id} NX EX 60`).

### 2. Two-Tier Idempotency — Zero Duplicate Ingestion

- **Tier 1 — Redis Fast-Path Deduplication**: On every `POST /v1/jobs`, the gateway performs a sub-millisecond lookup on `idemp:{Idempotency-Key}`. Cache hits return the cached `job_id` and `status` (`cached: true`) in sub-millisecond latency without querying PostgreSQL. TTL is 86,400 seconds (24 hours).
- **Tier 2 — PostgreSQL Uniqueness Constraint**: On a cache miss, an atomic transactional outbox write executes:
  ```sql
  INSERT INTO jobs (id, idempotency_key, job_type, status, created_at)
  VALUES ($1, $2, $3, 'PENDING', NOW())
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id, status;
  ```
  If another concurrent request inserted the key in the race window, `RETURNING` produces zero rows. The gateway fetches the existing job row and populates the Redis cache. The unique index `ix_jobs_idempotency_key` serves as the authoritative source of truth.

### 3. Distributed Lock Compare-and-Swap (CAS) Protection

Lock releases in both Go and Python runtimes utilize an atomic Lua script:

```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```

This guarantees that if Worker A stalls and its lease expires, and Worker B acquires the task, Worker A will not evict Worker B's active lock upon delayed completion.

### 4. Cross-Runtime Watchdog Reaper & DLQ Promotion

The Python Watchdog daemon sweeps PostgreSQL every 15 seconds for orphaned tasks left by crashed workers (Go or Python):

```sql
SELECT * FROM job_tasks
WHERE status = 'RUNNING'
  AND (heartbeat_at < NOW() - INTERVAL '30 seconds' OR heartbeat_at IS NULL)
FOR UPDATE SKIP LOCKED;
```

For each orphaned task:
1. **Lock Eviction**: The Redis lock key `lock:task:{id}` is deleted.
2. **Retry Evaluation**:
   - If `retry_count + 1 < max_retries`: The task is reset to `PENDING`, `retry_count` is incremented, and claim fields (`locked_by`, `heartbeat_at`) are cleared.
   - If `retry_count + 1 >= max_retries`: The task is promoted to `DEAD_LETTER`, the parent job is marked `FAILED`, and `last_error` is recorded as `"Watchdog: Heartbeat expired, max retries exceeded"`.

---

## Empirical Benchmark Performance

All metrics are anchored in reproducible runs against live containerized services (PostgreSQL 16, Redis 7, Prometheus) on an Ubuntu 24.04 LTS host (2 vCPUs, 8 GB RAM):

### Empirical Comparison: Python Gateway vs. Go Gateway (500 Concurrent VUs)

| Metric | Python Gateway (FastAPI / asyncpg) | Go Gateway (pgxpool / goroutines) | Architectural Improvement |
|---|---|---|---|
| **Peak Virtual Users (VUs)** | 500 | 500 | Enterprise Parity |
| **Sustained Ingestion Throughput** | 158.31 req/s | **1,133.55 req/s** | **+616% (7.16x throughput)** |
| **HTTP Success Rate** | 100.00% (0 errors) | **100.00% (0 errors)** | Zero dropped requests |
| **Median Latency (p50)** | 685.07 ms | **366.01 ms** | **46.6% lower latency** |
| **Tail Latency (p95)** | 8,610 ms (8.61 s) | **966.86 ms (0.96 s)** | **88.8% lower tail latency (8.91x)** |
| **Tail Latency (p99)** | >10.0 s | **1,752.21 ms (1.75 s)** | **Sub-2s p99 under saturation** |
| **Process RSS Memory** | 116 MB | **~15 MB** | **7.7x smaller memory footprint** |
| **Tested User Keyspace** | 500 static keys | **20,000,000 synthetic keys** | Enterprise 20M+ footprint |
| **Idempotency Cache Hit Rate** | 70.64% | **65.68%** | Fast-path sub-millisecond hit |
| **Queue Draining Speed** | ~40 tasks/s | **560 tasks/s (2 Go workers)** | **14x faster queue drain** |
| **Automated Test Validation** | 14/14 Pytest passed | **100% Go tests passed (`-race`)** | Zero race conditions |

### Hardened Stress Battery (1,500 VUs & Collision Chaos)

As documented in [`docs/BENCHMARK_RESULTS.md`](docs/BENCHMARK_RESULTS.md#8-hardened-concurrency-saturation--fault-injection-battery):
- **Scenario A (1,500 VUs Pool Saturation)**: 64,690 requests, 646.82 RPS, 100% 202 Accepted, p50=1.97s, max=4.49s (cleanly queued in-memory within 5.0s context timeout; 0 errors).
- **Scenario B (1,000 VUs Collision Storm + 3.42s Redis Container Pause)**: 153,148 requests, 2,552.32 RPS, 97.99% cache hits. The gateway gracefully fell back to PostgreSQL `ON CONFLICT` during the pause and recovered immediately without memory or goroutine leaks (11 -> 14 -> 10 goroutines; RSS steady at 82.5 MB).
- **Scenario C (Dynamic 1-50 KB Payloads)**: 18,207 requests, 480 MB sent. Exactly 193 400 Bad Requests (1.06%, matching the 1% injected malformed requests), zero 5xx errors.
- **Deep pgbench (100 Clients)**: 8,496 `SKIP LOCKED` batch claim transactions processed at 142.66 TPS with zero deadlocks.

---

## Quickstart

### 1. Prerequisites

- Docker & Docker Compose
- Go 1.22+ (or Go 1.27+ toolchain)
- Python 3.11+

### 2. Launch Infrastructure Services

```bash
docker compose up -d postgres redis prometheus
```

This provisions:
- **PostgreSQL 16**: `localhost:5432` (`orchestrator` / `postgres:postgres`, max 500 connections)
- **Redis 7**: `localhost:6379` (LRU memory eviction policy)
- **Prometheus**: `localhost:9090` (scraping `:8080/metrics` and `:8000/metrics`)

Initialize database schema via SQLAlchemy metadata:
```bash
python3 -c "
import asyncio
from src.database import engine, Base
import src.models

async def init():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

asyncio.run(init())
print('Database tables initialized successfully.')
"
```

### 3. Run Ingestion Services

**Production Go Ingestion Gateway (Port 8080):**
```bash
cd services/gateway
DATABASE_DSN="postgres://postgres:postgres@localhost:5432/orchestrator" \
REDIS_ADDR="localhost:6379" \
GATEWAY_ADDR=":8080" \
go run ./cmd/gateway/
```
Metrics endpoint: `http://localhost:8080/metrics`

**Python FastAPI Compatibility Service (Port 8000):**
```bash
uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 2
```
Metrics endpoint: `http://localhost:8000/metrics`

### 4. Run Worker Pools & Watchdog

**Go Worker Pool (`cmd/worker`):**
```bash
cd services/gateway
DATABASE_DSN="postgres://postgres:postgres@localhost:5432/orchestrator" \
REDIS_ADDR="localhost:6379" \
WORKER_CONCURRENCY=10 \
BATCH_SIZE=50 \
go run ./cmd/worker/
```

**Python Worker Sandbox:**
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

**Python Watchdog Reaper:**
```bash
python -c "
import asyncio
from src.engine.watchdog import Watchdog

asyncio.run(Watchdog(interval=15, timeout=30).start())
"
```

### 5. Submit an Idempotent Job

```bash
# Ingestion via Go Gateway
curl -i -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: enterprise-tx-99901" \
  -d '{
    "job_type": "payment",
    "handler_name": "payment_handler",
    "payload": {"amount": 250.00, "currency": "USD"}
  }'
```

Replaying the exact same request returns:
```json
{
  "job_id": "8c593f6b-1d74-4b53-90d1-6c2e7428f521",
  "status": "PENDING",
  "cached": true
}
```
Sub-millisecond fast-path response served directly from Redis without touching PostgreSQL.

### 6. Run the Test Suites

**Execute Go Native Tests with Race Detector:**
```bash
cd services/gateway
go test -race -v ./internal/...
go vet ./internal/...
```

**Execute Python Integration & Chaos Tests:**
```bash
pytest tests/ -v
```

---

## Project Structure

```
services/gateway/              # High-Throughput Go Acceleration Layer
├── cmd/
│   ├── gateway/main.go        # HTTP Ingestion Gateway (goroutines, pgxpool, go-redis)
│   └── worker/main.go         # Go Worker claim loop (SKIP LOCKED, Redis locks, tickers)
├── internal/
│   ├── cache/cache.go         # Redis fast-path idempotency & Lua CAS lock
│   ├── db/db.go               # pgxpool connection pool (150 max conns), outbox SQL
│   ├── idempotency/           # Fast-path handler & 100-goroutine collision test
│   ├── metrics/metrics.go     # Prometheus metrics (requests, latency, hits)
│   ├── model/model.go         # Domain types mirroring PostgreSQL schema
│   └── worker/                # Worker pool, batch claims, and ticker heartbeat tests
├── Dockerfile.gateway         # Multi-stage scratch build (~15MB image)
└── go.mod, go.sum

src/                           # Python Coordination & Admin Layer
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
├── conftest.py                # Live PostgreSQL + Redis fixtures, DB clean hooks
├── unit/                      # Model validation, distributed lock CAS
├── integration/               # Idempotency, worker concurrency, stress
└── chaos/                     # Polyglot concurrency grill, watchdog recovery

scripts/benchmarks/
├── k6_hardened.js             # Hardened k6 load profile (Scenarios A, B, C)
├── run_chaos_scenario_b.sh    # 1,000-VU storm + 3.4s Redis Docker pause
├── run_pgbench_hardened.sh    # 100-client SKIP LOCKED & outbox saturation
├── loadgen.go                 # 20M user scale Go load generator
└── results/                   # Structured JSON & raw logs of benchmark runs

docs/
├── ARCHITECTURE.md            # System internals, pipeline, and partitioning
├── API_REFERENCE.md           # Dual HTTP contract & Prometheus metrics catalog
├── RUNBOOK.md                 # SRE troubleshooting, pool starvation, & maintenance
├── BENCHMARK_RESULTS.md       # Empirical benchmark output and comparative analysis
└── migrations/
    └── 001_partition_job_tasks.sql # Range partitioning for 20M+ lifetime rows
```

---

## Tech Stack

| Component | Technology | Role |
|---|---|---|
| **Ingestion Engine** | Go 1.22+ (goroutines + `pgxpool/v5`) | Sub-millisecond HTTP ingestion, raw binary outbox writes |
| **Worker Engine** | Go + Python Asyncio | Dual-runtime queue claiming via `SELECT ... FOR UPDATE SKIP LOCKED` |
| **Database** | PostgreSQL 16-Alpine | ACID outbox, row-level locks, declarative range partitioning |
| **Cache & Locks** | Redis 7-Alpine | Fast-path idempotency caching, distributed lock leases with Lua CAS |
| **Compatibility API** | FastAPI + Uvicorn | Python workflow orchestration, query introspection, admin API |
| **Observability** | Prometheus client (`/metrics`) | Cross-runtime request latency, cache hit ratios, worker metrics |
| **Testing & Chaos** | Pytest-asyncio + Go test (`-race`) | Polyglot concurrency validation, watchdog fault-injection suites |
| **Benchmarking** | k6 v0.54.0 + pgbench 16 | 1,500-VU saturation stress, Redis chaos injection, lock contention |
