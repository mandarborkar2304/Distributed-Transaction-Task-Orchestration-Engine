# API Reference — Dual-Plane HTTP Contracts & Prometheus Catalog

This document specifies the HTTP API interfaces and Prometheus metric catalogs for both the high-throughput Go Ingestion Gateway (`:8080`) and the Python Coordination Service (`:8000`).

---

## Table of Contents

1. [Dual-Plane Ingestion Architecture](#1-dual-plane-ingestion-architecture)
2. [HTTP Ingestion Contract (`POST /v1/jobs`)](#2-http-ingestion-contract-post-v1jobs)
3. [Job Introspection Contract (`GET /v1/jobs/{job_id}`)](#3-job-introspection-contract-get-v1jobsjob_id)
4. [Health Check Probes](#4-health-check-probes)
5. [Go Gateway Prometheus Catalog (`:8080/metrics`)](#5-go-gateway-prometheus-catalog-8080metrics)
6. [Python Engine Prometheus Catalog (`:8000/metrics`)](#6-python-engine-prometheus-catalog-8000metrics)

---

## 1. Dual-Plane Ingestion Architecture

| Tier | Service | Port | Primary Purpose | Concurrency Model | Target Throughput |
|---|---|---|---|---|---|
| **High-Throughput Gateway** | Go Gateway (`services/gateway`) | `:8080` | High-volume external webhooks, payment ingestion, and rapid deduplication | Goroutines + `pgxpool` | **1,000+ req/s** (observed 1,133 RPS) |
| **Coordination & Admin** | Python FastAPI (`src/`) | `:8000` | Administrative control, complex DAG queries, workflow orchestration | Uvicorn Workers + asyncio | **150+ req/s** |

Both endpoints share the identical JSON schema, idempotency headers, and deduplication behavior.

---

## 2. HTTP Ingestion Contract (`POST /v1/jobs`)

Submits a new job for asynchronous orchestration. Deduplication is strictly guaranteed across both endpoints via the `Idempotency-Key` header.

### 2.1 Request Specification

- **Method**: `POST`
- **Endpoints**:
  - `http://localhost:8080/v1/jobs` (Go Gateway, Recommended)
  - `http://localhost:8000/v1/jobs` (Python Compatibility API)
- **Headers**:
  | Header | Type | Required | Description |
  |---|---|---|---|
  | `Idempotency-Key` | `string` | **Yes** | Client-generated UUID or business transaction key (max 128 bytes). Submissions with the same key return the original cached response. |
  | `Content-Type` | `string` | **Yes** | Must be `application/json`. |

#### Request Body Schema

```json
{
  "job_type": "payment",
  "handler_name": "payment_handler",
  "payload": {
    "account_id": "acc_8829103",
    "amount": 249.95,
    "currency": "USD"
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `job_type` | `string` | **Yes** | Logical category stored on `jobs` (e.g., `"payment"`, `"compute"`, `"order"`). Max 64 chars. |
| `handler_name` | `string` | **Yes** | Dispatched handler key assigned to `job_tasks`. Valid handlers: `"payment_handler"`, `"compute_handler"`. Max 64 chars. |
| `payload` | `object` | No | Arbitrary JSON payload passed directly to task handler. Defaults to `{}`. |

---

### 2.2 Response Specifications

#### `202 Accepted` — Initial Job Submission (Cold Outbox Write)

Returned on the first submission of a unique `Idempotency-Key`. The job and task records have been atomically committed to PostgreSQL.

```json
{
  "job_id": "4a719d28-0c2b-42b7-a3d8-e16e6d1c8172",
  "status": "PENDING",
  "cached": false
}
```

#### `200 OK` / `202 Accepted` — Idempotent Duplicate Submission (Fast-Path Hit)

Returned when the `Idempotency-Key` has already been processed within the 24-hour cache TTL. This response is served directly from Redis in sub-millisecond latency without querying PostgreSQL.

```json
{
  "job_id": "4a719d28-0c2b-42b7-a3d8-e16e6d1c8172",
  "status": "PENDING",
  "cached": true
}
```

#### Response Body Field Definitions

| Field | Type | Description |
|---|---|---|
| `job_id` | `string` (UUIDv4) | The authoritative UUID assigned to the `jobs` row in PostgreSQL. |
| `status` | `string` | Current lifecycle state: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, or `DEAD_LETTER`. |
| `cached` | `boolean` | `true` indicates the response was served from Redis cache; `false` indicates a new transactional outbox insert. |

---

### 2.3 Error Responses

| Status Code | Reason | Response Body |
|---|---|---|
| `400 Bad Request` | `Idempotency-Key` header is missing | `{"error": "Idempotency-Key header is required"}` |
| `400 Bad Request` | Payload is not valid JSON or exceeds size limit | `{"error": "invalid character in numeric literal"}` |
| `500 Internal Server Error` | Unhandled database or pool timeout error | `{"error": "database connection timeout"}` |

---

## 3. Job Introspection Contract (`GET /v1/jobs/{job_id}`)

Retrieves the current execution status and metadata of an existing job by its UUID.

- **Method**: `GET`
- **Endpoint**: `http://localhost:8000/v1/jobs/{job_id}` (Served by Python API)
- **Parameters**:
  | Parameter | Location | Type | Description |
  |---|---|---|---|
  | `job_id` | Path | `string` (UUID) | The UUID of the job returned during ingestion. |

### Response Schema (`200 OK`)

```json
{
  "id": "4a719d28-0c2b-42b7-a3d8-e16e6d1c8172",
  "idempotency_key": "enterprise-tx-99901",
  "job_type": "payment",
  "status": "COMPLETED",
  "created_at": "2026-10-02T13:00:30.124592+00:00"
}
```

### Error Responses

| Status Code | Condition | Response Body |
|---|---|---|
| `400 Bad Request` | `job_id` path parameter is not a valid UUID format | `{"detail": "Invalid job ID format"}` |
| `404 Not Found` | No job matching the specified UUID exists in PostgreSQL | `{"detail": "Job not found"}` |

---

## 4. Health Check Probes

Readiness and liveness probes for container orchestrators (Kubernetes / Docker Compose):

| Service | Path | Success Response | Status Code |
|---|---|---|---|
| **Go Gateway** | `GET http://localhost:8080/healthz` | `ok` | `200 OK` |
| **Python API** | `GET http://localhost:8000/health` | `{"status": "healthy"}` | `200 OK` |

---

## 5. Go Gateway Prometheus Catalog (`:8080/metrics`)

Exposed by the Go runtime at `http://localhost:8080/metrics` using the official `prometheus/client_golang` package.

### 5.1 Ingestion Metrics

#### `gateway_requests_total`
- **Type**: Counter
- **Description**: Total count of HTTP requests handled by the Go gateway.
- **Labels**: `method`, `status_code`
- **Example PromQL**:
  ```promql
  # Request rate per second over 5 minutes:
  sum(rate(gateway_requests_total[5m]))
  ```

#### `gateway_latency_seconds`
- **Type**: Histogram
- **Description**: End-to-end HTTP request duration including Redis lookups and PostgreSQL writes.
- **Buckets**: `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0`
- **Labels**: `handler`
- **Example PromQL**:
  ```promql
  # p95 ingestion latency in seconds:
  histogram_quantile(0.95, sum(rate(gateway_latency_seconds_bucket[5m])) by (le))
  ```

#### `gateway_idempotency_hits_total`
- **Type**: Counter
- **Description**: Count of requests deduplicated via the Redis fast-path cache.
- **Labels**: None
- **Example PromQL**:
  ```promql
  # Idempotency fast-path hit ratio percentage:
  (rate(gateway_idempotency_hits_total[5m]) / sum(rate(gateway_requests_total{status_code="202"}[5m]))) * 100
  ```

---

### 5.2 Worker Pool Metrics

#### `gateway_worker_tasks_claimed_total`
- **Type**: Counter
- **Description**: Cumulative tasks claimed by Go worker goroutines via `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Labels**: None

#### `gateway_worker_tasks_completed_total`
- **Type**: Counter
- **Description**: Cumulative tasks completed by the Go worker pool, partitioned by terminal outcome.
- **Labels**: `outcome` (`completed`, `failed`, `dead_letter`)
- **Example PromQL**:
  ```promql
  # Task drain rate per second:
  rate(gateway_worker_tasks_completed_total{outcome="completed"}[1m])
  ```

#### `gateway_active_workers`
- **Type**: Gauge
- **Description**: Number of worker goroutines currently executing tasks in parallel.
- **Labels**: None

---

## 6. Python Engine Prometheus Catalog (`:8000/metrics`)

Exposed by the Python runtime at `http://localhost:8000/metrics` via `prometheus_client`.

### 6.1 Coordination & Reaper Metrics

#### `orchestrator_tasks_total`
- **Type**: Counter
- **Description**: Task execution completions within the Python worker sandbox.
- **Labels**: `status` (`COMPLETED`, `FAILED`, `DEAD_LETTER`), `handler`

#### `orchestrator_execution_duration_seconds`
- **Type**: Histogram
- **Description**: Task execution duration inside Python handlers.
- **Labels**: `handler`

#### `orchestrator_reclaimed_orphans_total`
- **Type**: Counter
- **Description**: Total count of orphaned `RUNNING` tasks reclaimed by the Python Watchdog daemon.
- **Labels**: None
- **Example PromQL (Worker Crash Alert)**:
  ```promql
  # Alert if watchdog is reclaiming more than 2 orphaned tasks/min:
  rate(orchestrator_reclaimed_orphans_total[5m]) > 0.033
  ```

---

### 6.2 Process & System Metrics

Both services expose runtime process metrics automatically:

| Metric | Type | Description |
|---|---|---|
| `process_resident_memory_bytes` | Gauge | Physical RAM footprint in bytes (RSS). |
| `process_virtual_memory_bytes` | Gauge | Virtual memory allocation. |
| `process_open_fds` | Gauge | Count of open file descriptors and TCP sockets. |
| `process_cpu_seconds_total` | Counter | Cumulative CPU processing time in seconds. |
| `go_goroutines` | Gauge | Number of active goroutines (Go Gateway only). |
| `go_memstats_alloc_bytes` | Gauge | Go heap memory actively allocated in bytes. |
