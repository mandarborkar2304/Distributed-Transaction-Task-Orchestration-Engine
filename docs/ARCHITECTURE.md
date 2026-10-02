# Architecture — System Internals & Technical Deep-Dive

This document details the internal mechanics of every subsystem in the Distributed Transaction & Task Orchestration Engine. All field names, index names, query shapes, and behavioral constants are taken directly from the source code.

---

## Table of Contents

1. [Data Models & State Transitions](#1-data-models--state-transitions)
2. [Worker Pool & Concurrency Engine](#2-worker-pool--concurrency-engine)
3. [Distributed Locking Implementation](#3-distributed-locking-implementation)
4. [Watchdog / Task Reaper Daemon](#4-watchdog--task-reaper-daemon)
5. [Connection Pool Configuration](#5-connection-pool-configuration)

---

## 1. Data Models & State Transitions

### Job State Machine

```
                     POST /v1/jobs
                          │
                          ▼
                       PENDING
                          │
            Worker claims via SKIP LOCKED
                          │
                          ▼
                       RUNNING ──────────────────────────────┐
                          │                                  │
               handler.execute() completes                   │ Watchdog: heartbeat
                          │                                  │ expired, retry < max
                          ▼                                  ▼
                      COMPLETED                           PENDING (retry++)
                                                             │
                                         retry_count + 1 >= max_retries
                                                             │
                                                             ▼
                                                        DEAD_LETTER
                                                    (job.status → FAILED)
```

`TaskStatus` is a Python `str` enum with values: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD_LETTER`.

### Table: `jobs`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY`, `DEFAULT uuid4()` | Auto-generated |
| `idempotency_key` | `VARCHAR(128)` | `UNIQUE NOT NULL` | Client-supplied dedup key |
| `job_type` | `VARCHAR(64)` | `NOT NULL` | Logical classification |
| `status` | `ENUM(TaskStatus)` | `NOT NULL`, `DEFAULT 'PENDING'` | Mirrors task rollup |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL`, `DEFAULT utc_now()` | Insertion timestamp |

**Indexes on `jobs`:**

| Index Name | Columns | Type |
|---|---|---|
| `ix_jobs_status_created_at` | `(status, created_at)` | Composite B-tree |
| `ix_jobs_idempotency_key` | `(idempotency_key)` | Unique B-tree |

### Table: `job_tasks`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| `id` | `UUID` | `PRIMARY KEY`, `DEFAULT uuid4()` | Auto-generated |
| `job_id` | `UUID` | `FK → jobs.id ON DELETE CASCADE NOT NULL` | Parent job |
| `handler_name` | `VARCHAR(64)` | `NOT NULL` | Dispatched handler key |
| `status` | `ENUM(TaskStatus)` | `NOT NULL`, `DEFAULT 'PENDING'` | Execution state |
| `payload` | `JSONB` | `NOT NULL`, `DEFAULT '{}'` | Arbitrary handler input |
| `retry_count` | `INTEGER` | `NOT NULL`, `DEFAULT 0` | Incremented on each retry |
| `max_retries` | `INTEGER` | `NOT NULL`, `DEFAULT 3` | DLQ threshold |
| `locked_by` | `VARCHAR(64)` | `NULLABLE` | Worker UUID that holds this task |
| `heartbeat_at` | `TIMESTAMPTZ` | `NULLABLE` | Last heartbeat; NULL if not running |
| `last_error` | `TEXT` | `NULLABLE` | Exception or watchdog message |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL`, `DEFAULT utc_now()` | Insertion timestamp |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL`, `DEFAULT utc_now()`, `ONUPDATE utc_now()` | Last mutation |

**Indexes on `job_tasks`:**

| Index Name | Columns | Type | Notes |
|---|---|---|---|
| `ix_job_tasks_status_created_at` | `(status, created_at)` | Composite B-tree | Full-table worker polls |
| `ix_job_tasks_pending_running` | `(status, created_at) WHERE status IN ('PENDING', 'RUNNING')` | Partial B-tree | Watchdog + worker hot path |
| `ix_job_tasks_job_id` | `(job_id)` | B-tree | Cascade deletes, task enumeration |

The partial index `ix_job_tasks_pending_running` is the most critical index in the system. It covers only `PENDING` and `RUNNING` rows — in a mature deployment where `COMPLETED` rows dominate, this index remains small and cache-hot regardless of total table size.

### ORM Relationships

```python
# jobs → job_tasks (one-to-many)
Job.tasks = relationship("JobTask", back_populates="job",
                         cascade="all, delete-orphan", lazy="selectin")

# job_tasks → jobs (many-to-one)
JobTask.job = relationship("Job", back_populates="tasks", lazy="selectin")
```

`lazy="selectin"` is required on both sides to avoid `MissingGreenlet` errors when relationship attributes are accessed inside async coroutines after `session.commit()`.

---

## 2. Worker Pool & Concurrency Engine

### Claim Loop — `Worker.run_once()`

```python
async def run_once(self):
    async with AsyncSessionLocal() as session:
        stmt = (
            select(JobTask)
            .where(JobTask.status == TaskStatus.PENDING)
            .order_by(JobTask.created_at.asc())
            .limit(self.batch_size)          # default: 10
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        tasks = result.scalars().all()

        if not tasks:
            return 0

        now = datetime.now(timezone.utc)
        for t in tasks:
            t.status = TaskStatus.RUNNING
            t.locked_by = self.worker_id
            t.heartbeat_at = now

        await session.commit()      # ← atomic: lock + status in one transaction

    coros = [self._process_task(t.id) for t in tasks]
    await asyncio.gather(*coros)    # ← process all claimed tasks concurrently

    return len(tasks)
```

**Key properties:**
- The `FOR UPDATE SKIP LOCKED` lock and the status transition to `RUNNING` happen inside the **same transaction**. No worker can claim a task another worker has already locked.
- `SKIP LOCKED` means a worker reading 10 rows gets 10 *immediately available* rows; it never waits for another worker's lock to clear. This eliminates thundering-herd queuing.
- `asyncio.gather` processes the full batch concurrently. Each coroutine operates in its own database session.
- When the queue is empty (`tasks == []`), the worker sleeps for 1 second before polling again.

### Task Execution — `Worker._process_task(task_id)`

Execution is structured around two orthogonal coroutines that run concurrently per task:

1. **Handler coroutine** — runs the business logic via `HANDLERS[handler_name].execute(task, payload)`.
2. **Heartbeat coroutine** — runs `_heartbeat_loop(task_id, cancel_event)`, renewing the DB timestamp and Redis lock TTL every 10 seconds.

```
asyncio.create_task(_heartbeat_loop) ──► runs every 10s while handler runs
       │
       │  handler finishes or raises
       ▼
cancel_event.set()
await heartbeat_task           ← drain final heartbeat iteration
await lock.release()           ← atomic Lua CAS release
```

The heartbeat is cancelled via `asyncio.Event` rather than `task.cancel()` to allow a clean final iteration, avoiding a race where the heartbeat is mid-write when cancelled.

### Heartbeat Renewal

```python
async def _heartbeat_loop(self, task_id, cancel_event: asyncio.Event):
    while not cancel_event.is_set():
        try:
            await asyncio.wait_for(cancel_event.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            # Renew PostgreSQL heartbeat
            async with AsyncSessionLocal() as session:
                stmt = update(JobTask).where(JobTask.id == task_id).values(
                    heartbeat_at=datetime.now(timezone.utc)
                )
                await session.execute(stmt)
                await session.commit()

            # Renew Redis lock TTL
            await redis_client.expire(f"lock:task:{task_id}", 60)
```

- `asyncio.wait_for(cancel_event.wait(), timeout=10.0)` blocks for up to 10 seconds or returns early when the event is set. On `TimeoutError`, it was a normal 10-second tick; the renewal proceeds.
- The Redis lock TTL is renewed to 60 seconds on every heartbeat tick. Since ticks are 10 seconds and the Watchdog timeout is 30 seconds, a healthy task maintains headroom of at least 30 seconds before the lock expires.

### Retry & Backoff

On handler failure:

```python
def get_backoff(self, retry_count: int) -> float:
    return min(60.0, (2 ** retry_count) + random.uniform(0, 1))
```

| `retry_count` | Backoff (deterministic base) | Range with jitter |
|---|---|---|
| 0 | 1s | 1.0 – 2.0 s |
| 1 | 2s | 2.0 – 3.0 s |
| 2 | 4s | 4.0 – 5.0 s |
| 3+ | capped at 60s | 60.0 s |

After backoff sleep, `status` is reset to `PENDING` and `retry_count` is incremented, making the task visible to any worker in the next polling cycle.

When `retry_count + 1 >= max_retries` (default `max_retries=3`), the task transitions directly to `DEAD_LETTER` without sleeping.

### Graceful Shutdown

`Worker.stop()` sets `self._running = False`. The `start()` loop checks this flag at the top of each iteration:

```python
async def start(self):
    self._running = True
    while self._running:
        processed = await self.run_once()
        if processed == 0:
            await asyncio.sleep(1)
```

In-flight tasks are not interrupted; `stop()` takes effect after the current `run_once()` call and all its `asyncio.gather` coroutines have completed. For signal-based shutdown (`SIGINT`, `SIGTERM`), the caller is responsible for calling `worker.stop()` and then awaiting the currently running coroutine.

---

## 3. Distributed Locking Implementation

### Redis Lock Key Format

```
lock:task:{task_id}
```

`task_id` is the UUID of the `job_tasks` row (e.g., `lock:task:3f2a1c8b-4e7d-...`).

### Acquisition — Atomic `SET NX EX`

```python
result = await self.client.set(
    self.key,         # lock:task:{task_id}
    self.owner_id,    # worker UUID
    nx=True,          # only set if NOT EXISTS
    ex=self.ttl_seconds  # initial TTL = 60s
)
```

The `SET NX EX` command is atomic at the Redis server level: check-and-set cannot be interleaved with another client's operation. The lock value is the `worker_id` UUID string, establishing ownership.

### Release — Lua CAS Script

```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```

This Lua script is executed atomically on the Redis server. It releases the lock **only if the caller's `owner_id` matches the stored value**. This prevents a scenario where:
1. Worker A's lock expires (task appears stale).
2. Watchdog deletes the lock and re-queues the task.
3. Worker B acquires a new lock on the same task ID.
4. Worker A finishes and calls `release()` — **the Lua script returns 0** instead of deleting Worker B's lock.

Without this CAS guarantee, Worker A's `DEL` would silently evict Worker B's lock, leaving the task unprotected.

The Python invocation:
```python
res = await self.client.eval(RELEASE_LOCK_LUA, 1, self.key, self.owner_id)
# res == 1: lock was held by caller and released
# res == 0: lock was not held by caller; no action taken
```

### Heartbeat TTL Renewal

Every 10 seconds (in `_heartbeat_loop`), the lock TTL is extended:
```python
await redis_client.expire(f"lock:task:{task_id}", 60)
```

This maintains a 60-second forward window without re-acquiring the lock.

### Context Manager Interface

```python
async with RedisDistributedLock("lock:task:{id}", owner_id=worker_id) as lock:
    # lock guaranteed acquired; RuntimeError if not
    ...
# lock released atomically on exit, even on exception
```

`__aexit__` always calls `release()`, ensuring no leaked locks even if the handler raises an unhandled exception.

---

## 4. Watchdog / Task Reaper Daemon

### Polling Cadence

```python
class Watchdog:
    def __init__(self, interval: int = 15, timeout: int = 30):
```

- **`interval=15`** — seconds between each `run_once()` sweep.
- **`timeout=30`** — a `RUNNING` task whose `heartbeat_at` is more than 30 seconds in the past is considered orphaned.

Since workers renew heartbeats every 10 seconds and the timeout window is 30 seconds, a task must miss at least 3 consecutive heartbeat ticks before the watchdog reclaims it.

### Stale Task Query

```python
stmt = (
    select(JobTask)
    .options(selectinload(JobTask.job))
    .where(JobTask.status == TaskStatus.RUNNING)
    .where(
        or_(
            JobTask.heartbeat_at < threshold,   # threshold = NOW() - 30s
            JobTask.heartbeat_at.is_(None)       # NULL heartbeat = never renewed
        )
    )
    .with_for_update(skip_locked=True)
)
```

`with_for_update(skip_locked=True)` on the watchdog query prevents two watchdog instances from simultaneously reclaiming the same task, and prevents interference with a worker that is in the process of completing a task and updating its heartbeat concurrently.

### Reclamation Logic

For each stale task:

```python
# 1. Remove the Redis lock unconditionally
await redis_client.delete(f"lock:task:{task.id}")

# 2. DLQ or re-queue
if task.retry_count + 1 >= task.max_retries:
    task.status = TaskStatus.DEAD_LETTER
    task.last_error = 'Watchdog: Heartbeat expired, max retries exceeded'
    if task.job:
        task.job.status = TaskStatus.FAILED
else:
    task.status = TaskStatus.PENDING
    task.retry_count += 1

# 3. Clear claim fields in all cases
task.locked_by = None
task.heartbeat_at = None
```

The Redis lock is deleted unconditionally because by the time the watchdog identifies the task as stale, any legitimate lock held by the original worker has already expired (Redis TTL enforcement) or the worker is genuinely dead. Deleting a non-existent key is a no-op in Redis.

### DLQ Semantics

A task in `DEAD_LETTER` status is permanently terminal. No worker or watchdog will touch it. Operational recovery requires a manual SQL update or a dedicated dead-letter replay pipeline. The `last_error` field contains the diagnostic message:
- Worker failure path: the exception string from `handler.execute()`.
- Watchdog path: `"Watchdog: Heartbeat expired, max retries exceeded"`.

### Error Isolation in `start()`

```python
async def start(self):
    self._running = True
    while self._running:
        try:
            await self.run_once()
        except Exception as e:
            logger.error("Watchdog encountered error during run_once: %s", e)
        await asyncio.sleep(self.interval)
```

Any exception inside `run_once()` (e.g., PostgreSQL connection loss) is caught, logged, and the watchdog continues after sleeping. A crashed watchdog would leave stale tasks indefinitely — the `try/except` wrapper ensures the daemon never crashes silently.

---

## 5. Connection Pool Configuration

### PostgreSQL (asyncpg via SQLAlchemy)

```python
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=25,        # persistent connections always kept open
    max_overflow=50,     # temporary burst connections (25 + 50 = 75 total max)
    pool_timeout=30.0,   # raise TimeoutError after 30s of waiting for a connection
    echo=False
)
```

With 2 uvicorn workers, the maximum connection count to PostgreSQL is `2 × 75 = 150`. The PostgreSQL container is configured with `max_connections=500`, providing headroom for pgbench, psql shells, and monitoring connections.

### Redis

```python
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True, max_connections=1000)
```

`max_connections=1000` allows the worker pool (40+ concurrent coroutines × multiple awaits per task) and the API layer to coexist without `MaxConnectionsError`. Redis connections are much lighter than PostgreSQL connections (no authentication handshake on each request).

---

## 6. Hybrid Go + Python Polyglot Architecture (20M+ Scale)

To sustain high-throughput ingestion and queue-claiming for an enterprise footprint of 20M+ users without being constrained by the Python GIL or asyncio event loop overhead, the architecture introduces a native Go acceleration layer while retaining Python for specialized handler orchestration.

### 6.1 Go Ingestion Gateway (`services/gateway/cmd/gateway`)

The Go gateway provides native goroutine-per-request concurrency and sub-millisecond idempotency deduplication:

1. **Lightweight Concurrency**: Each incoming `POST /v1/jobs` request is handled by an independent goroutine, eliminating event loop head-of-line blocking.
2. **Sub-Millisecond Redis Fast-Path**: Using `github.com/redis/go-redis/v9` with a connection pool of 500 connections (`PoolSize: 500, MinIdleConns: 25`), idempotency checks (`idemp:{key}`) return cached responses in <1ms without database access.
3. **Connection-Pooled PostgreSQL Writes (`pgxpool`)**:
   ```go
   cfg.MaxConns = 150
   cfg.MinConns = 25
   cfg.MaxConnLifetime = 30 * time.Minute
   cfg.MaxConnIdleTime = 5 * time.Minute
   cfg.HealthCheckPeriod = 60 * time.Second
   ```
   `pgx/v5` raw binary protocol execution bypasses ORM serialization overhead, writing directly to the transactional outbox (`jobs` + `job_tasks`) using atomic `INSERT ... ON CONFLICT DO NOTHING RETURNING` transactions.
4. **Prometheus Instrumentation**: Exposes `gateway_requests_total`, `gateway_latency_seconds` histogram buckets, and `gateway_idempotency_hits_total` on `GET /metrics`.

### 6.2 High-Performance Go Worker Pool (`services/gateway/cmd/worker`)

The Go worker implements an autonomous task consumption loop:

1. **Atomic Batch Claiming**: Executes `SELECT ... FOR UPDATE SKIP LOCKED` with a batch size of 50 tasks in a single Read Committed transaction, updating status to `RUNNING`, assigning `locked_by = worker_id`, and setting initial `heartbeat_at = now()`.
2. **Redis Lock Lease**: For each claimed task, an atomic `SET lock:task:{id} {worker_id} NX EX 60` lock is acquired.
3. **Non-Blocking Heartbeat Ticker**: An autonomous goroutine runs a `time.NewTicker(10 * time.Second)` that renews both the database `heartbeat_at` timestamp and the Redis lock TTL every 10 seconds until task completion.
4. **Graceful Shutdown**: Intercepts `SIGINT` / `SIGTERM`, waits for in-flight tasks via `sync.WaitGroup`, and executes state updates using an independent shutdown context.

### 6.3 Polyglot Coordination Contract

The Go and Python layers share a unified PostgreSQL and Redis contract:

```
┌────────────────────────────────────────────────────────┐
│             Shared Database & Queue Contract           │
├────────────────────────────────────────────────────────┤
│ PostgreSQL:                                            │
│   jobs(id, idempotency_key, job_type, status)          │
│   job_tasks(id, job_id, handler_name, status, payload) │
│                                                        │
│ Redis:                                                 │
│   idemp:{idempotency_key} -> cached JSON (TTL=86400s)  │
│   lock:task:{task_id}     -> worker_id   (TTL=60s)     │
└────────────────────────────────────────────────────────┘
```

- **Go-Native Tasks**: Handlers registered in Go (`compute_handler`, `payment_handler`) execute with native speed and sub-millisecond dispatch.
- **Python Delegation**: If a task requires Python libraries (e.g. data science, legacy integrations), the Go worker detects an unrecognised handler and leaves it in `PENDING` for the Python worker pool, or the job is routed to the Python API (`:8000`).

### 6.4 Declarative Range Partitioning Strategy (20M+ Rows)

At 20,000,000+ lifetime tasks, a single B-Tree index on `job_tasks` exceeds memory cache capacity, leading to cache eviction and degraded worker poll performance.

The engine includes declarative range partitioning ready for production deployment:
- **Partition Key**: `RANGE (created_at)` with monthly partition tables (`job_tasks_YYYY_MM`).
- **Pruning Efficiency**: Worker polls filtering on `WHERE status IN ('PENDING', 'RUNNING') ORDER BY created_at` naturally isolate to the active month's partition.
- **Index Optimization**:
  - Partial index `ix_job_tasks_pending_running` indexes *only* active tasks (`WHERE status IN ('PENDING', 'RUNNING')`), keeping the hot index under a few megabytes regardless of millions of completed rows.
  - Partial index `ix_job_tasks_heartbeat_at` indexes *only* non-null heartbeats (`WHERE heartbeat_at IS NOT NULL`), preventing index bloat from completed/pending tasks.
- **Migration Script**: Documented in [`docs/migrations/001_partition_job_tasks.sql`](migrations/001_partition_job_tasks.sql).

