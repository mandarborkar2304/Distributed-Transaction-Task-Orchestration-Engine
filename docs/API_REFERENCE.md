# API Reference — Interface Specifications & Metrics Catalog

All request and response formats are as implemented in [`src/api/routes.py`](../src/api/routes.py). All Prometheus metric names, label sets, and types are as declared in [`src/observability/metrics.py`](../src/observability/metrics.py).

---

## Table of Contents

1. [REST API](#1-rest-api)
   - [POST /v1/jobs — Submit a Job](#post-v1jobs--submit-a-job)
   - [GET /v1/jobs/{job_id} — Retrieve a Job](#get-v1jobsjob_id--retrieve-a-job)
2. [Prometheus Observability Catalog](#2-prometheus-observability-catalog)

---

## 1. REST API

The FastAPI application is mounted at `http://localhost:8000`. All job routes are prefixed with `/v1/jobs`.

---

### `POST /v1/jobs` — Submit a Job

Submits a new job for asynchronous processing. The request is idempotent: sending the same `Idempotency-Key` a second time returns the original response without creating a duplicate job or task.

#### Request

**Method:** `POST`  
**Path:** `/v1/jobs`  
**Content-Type:** `application/json`

**Required Headers:**

| Header | Type | Required | Description |
|---|---|---|---|
| `Idempotency-Key` | `string` | **Yes** | Client-generated unique key (max 128 characters) scoping this request. Repeated submissions with the same key return the original response. |
| `Content-Type` | `string` | Yes | Must be `application/json`. |

**Request Body:**

```json
{
  "job_type": "payment",
  "handler_name": "payment_handler",
  "payload": {
    "amount": 99.99,
    "currency": "USD"
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `job_type` | `string` | Yes | Logical classification of the job (e.g., `"payment"`, `"compute"`). Stored on the `jobs` row; not used for dispatch. |
| `handler_name` | `string` | Yes | The handler key used to dispatch the `job_tasks` row. Valid values: `"payment_handler"`, `"compute_handler"`. |
| `payload` | `object` | No | Arbitrary JSON passed verbatim to the handler's `execute()` method. Defaults to `{}`. |

#### Responses

**`202 Accepted` — Job accepted (new submission)**

```json
{
  "job_id": "3f2a1c8b-4e7d-4b3a-9f1c-abc123def456",
  "status": "PENDING",
  "cached": false
}
```

**`200 OK` — Idempotent hit (duplicate submission)**

> Note: The HTTP status code for a deduplicated response is `202` (the endpoint's declared status code). The `cached: true` field in the response body is the semantic indicator of deduplication. The Redis fast-path returns the stored JSON, and the status code matches the original request's status code.

```json
{
  "job_id": "3f2a1c8b-4e7d-4b3a-9f1c-abc123def456",
  "status": "PENDING",
  "cached": true
}
```

**Response Body Fields:**

| Field | Type | Description |
|---|---|---|
| `job_id` | `string` (UUID) | The UUID of the `jobs` row. |
| `status` | `string` | Current job status at ingestion time. One of: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD_LETTER`. |
| `cached` | `boolean` | `true` if this response was served from the Redis fast-path without touching PostgreSQL. `false` on first submission. |

#### Error Responses

| Status | Condition | Body |
|---|---|---|
| `400 Bad Request` | `Idempotency-Key` header is absent | `{"detail": "Idempotency-Key header is missing"}` |
| `500 Internal Server Error` | Duplicate key conflict in DB but record cannot be found (should never occur under normal operation) | `{"detail": "Failed to locate existing job record"}` |

#### Idempotency Mechanics (Internal)

1. Compute Redis key: `idemp:{Idempotency-Key}`.
2. `GET idemp:{key}` from Redis.
   - **Hit**: Deserialize JSON, increment `orchestrator_idempotency_hits_total`, return `cached=true`. PostgreSQL is **not** contacted.
   - **Miss**: Proceed to step 3.
3. `INSERT INTO jobs (idempotency_key, job_type) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id, status` inside a transaction.
   - If `RETURNING` yields a row: a new `JobTask` record is inserted in the same transaction.
   - If `RETURNING` yields nothing (conflict): the existing `jobs` row is fetched.
4. `SET idemp:{key} <json> EX 86400` to populate the Redis cache for 24 hours.
5. Return response with `cached=false`.

---

### `GET /v1/jobs/{job_id}` — Retrieve a Job

Fetches the current state of a job by its UUID.

#### Request

**Method:** `GET`  
**Path:** `/v1/jobs/{job_id}`

| Path Parameter | Type | Description |
|---|---|---|
| `job_id` | `string` (UUID) | The UUID of the job returned from `POST /v1/jobs`. |

#### Responses

**`200 OK`**

```json
{
  "id": "3f2a1c8b-4e7d-4b3a-9f1c-abc123def456",
  "idempotency_key": "my-unique-key-001",
  "job_type": "payment",
  "status": "COMPLETED",
  "created_at": "2026-10-02T11:00:00.123456+00:00"
}
```

| Field | Type | Description |
|---|---|---|
| `id` | `string` (UUID) | Job UUID. |
| `idempotency_key` | `string` | The original idempotency key used to create the job. |
| `job_type` | `string` | The `job_type` value from the creation request. |
| `status` | `string` | Current status. One of: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD_LETTER`. |
| `created_at` | `string` (ISO 8601) | UTC creation timestamp with timezone offset. |

#### Error Responses

| Status | Condition | Body |
|---|---|---|
| `400 Bad Request` | `job_id` is not a valid UUID format | `{"detail": "Invalid job ID format"}` |
| `404 Not Found` | No `jobs` row with that UUID exists | `{"detail": "Job not found"}` |

---

## 2. Prometheus Observability Catalog

The Prometheus metrics endpoint is served at `GET http://localhost:8000/metrics` by a mounted ASGI sub-application (`prometheus_client.make_asgi_app()`). The endpoint returns the standard Prometheus text exposition format.

All metric names and label keys are defined verbatim in [`src/observability/metrics.py`](../src/observability/metrics.py).

---

### `orchestrator_tasks_total`

**Type:** Counter  
**Description:** Total number of task execution outcomes.

**Labels:**

| Label | Values | Description |
|---|---|---|
| `status` | `COMPLETED`, `FAILED`, `DEAD_LETTER` | Final outcome of the task execution attempt. |
| `handler` | `payment_handler`, `compute_handler` | The handler that processed the task. |

**When incremented:**
- `COMPLETED`: handler's `execute()` returned without exception.
- `FAILED`: handler raised an exception and `retry_count + 1 < max_retries` (task will be retried).
- `DEAD_LETTER`: handler raised an exception and `retry_count + 1 >= max_retries` (task moved to DLQ).

**Example query — failure rate by handler:**
```promql
rate(orchestrator_tasks_total{status="FAILED"}[5m])
  / rate(orchestrator_tasks_total[5m])
```

**Example query — DLQ accumulation rate:**
```promql
rate(orchestrator_tasks_total{status="DEAD_LETTER"}[1h])
```

---

### `orchestrator_execution_duration_seconds`

**Type:** Histogram  
**Description:** Wall-clock duration of each task's handler execution in seconds. Measured from immediately before `handler.execute(task, payload)` is called to immediately after it returns or raises.

**Labels:**

| Label | Values | Description |
|---|---|---|
| `handler` | `payment_handler`, `compute_handler` | The handler that was timed. |

**Default buckets:** Prometheus client default histogram buckets (`.005`, `.01`, `.025`, `.05`, `.075`, `.1`, `.25`, `.5`, `.75`, `1.0`, `2.5`, `5.0`, `7.5`, `10.0`).

**Example query — p95 execution latency per handler:**
```promql
histogram_quantile(0.95,
  rate(orchestrator_execution_duration_seconds_bucket[5m])
)
```

**Operational threshold:** If p99 execution duration exceeds the worker's heartbeat renewal interval (10 seconds), the watchdog may reclaim still-running tasks. Alert if:
```promql
histogram_quantile(0.99,
  rate(orchestrator_execution_duration_seconds_bucket[5m])
) > 10
```

---

### `orchestrator_reclaimed_orphans_total`

**Type:** Counter  
**Description:** Total number of stale `RUNNING` tasks reclaimed by the Watchdog daemon. Incremented once per task per watchdog cycle, before the task is reset to `PENDING` or moved to `DEAD_LETTER`.

**Labels:** None.

**When incremented:** Inside `Watchdog.run_once()` for each task matching:
- `status = 'RUNNING'`
- `heartbeat_at < NOW() - 30s` OR `heartbeat_at IS NULL`

**Example query — orphan reclamation rate (indicates worker crashes or network partitions):**
```promql
rate(orchestrator_reclaimed_orphans_total[10m])
```

**Operational threshold:** A sustained rate above 1 orphan/minute is a signal of systemic worker instability. Alert if:
```promql
rate(orchestrator_reclaimed_orphans_total[5m]) > 0.017
```

---

### `orchestrator_idempotency_hits_total`

**Type:** Counter  
**Description:** Total number of requests served from the Redis idempotency cache, bypassing PostgreSQL entirely.

**Labels:** None.

**When incremented:** In `POST /v1/jobs`, when `GET idemp:{key}` returns a non-null value from Redis.

**Example query — cache hit rate:**
```promql
rate(orchestrator_idempotency_hits_total[5m])
  / rate(http_requests_total{handler="create_job"}[5m])
```

Under the k6 benchmark (500 VUs, 70/30 collision ratio), this counter reached a **70.64% hit rate** (5,911 hits out of 8,367 total requests), confirming that the Redis fast-path absorbs the majority of retry traffic without PostgreSQL interaction.

---

### Standard Prometheus Metrics

In addition to the application-specific metrics above, the `prometheus_client` library automatically exposes the following process and Python runtime metrics:

| Metric | Type | Description |
|---|---|---|
| `process_resident_memory_bytes` | Gauge | RSS memory usage (116 MB observed post-load) |
| `process_virtual_memory_bytes` | Gauge | Virtual memory usage (508 MB observed) |
| `process_open_fds` | Gauge | Open file descriptor count (129 observed post-load) |
| `process_cpu_seconds_total` | Counter | Total CPU time consumed |
| `python_gc_collections_total{generation}` | Counter | GC collection counts by generation |
| `python_info` | Gauge | Python version metadata |
