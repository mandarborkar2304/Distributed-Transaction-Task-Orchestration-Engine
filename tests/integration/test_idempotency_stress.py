import asyncio
import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select, func

from src.database import AsyncSessionLocal
from src.main import app
from src.models import Job, JobTask


@pytest.mark.asyncio
async def test_high_throughput_idempotency_10k_benchmark(db_setup):
    """
    High-Throughput Idempotency Benchmark:
    Hammer ingestion endpoint with 10,000 requests containing duplicated
    idempotency keys across 50 concurrent tasks.
    Confirm that exactly the expected number of unique records exist in PostgreSQL
    and that duplicate requests return the cached original response.
    """
    num_unique_keys = 100
    requests_per_key = 100
    total_requests = num_unique_keys * requests_per_key  # 10,000 requests
    num_concurrent_workers = 50

    unique_keys = [f"stress_key_{i}_{uuid.uuid4()}" for i in range(num_unique_keys)]
    
    # Interleave requests so duplicates are hammered concurrently
    all_keys = [unique_keys[i % num_unique_keys] for i in range(total_requests)]

    queue = asyncio.Queue()
    for k in all_keys:
        queue.put_nowait(k)

    url = "/v1/jobs"
    payload = {
        "job_type": "payment",
        "handler_name": "payment_handler",
        "payload": {"amount": 250}
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async def client_worker():
            results = []
            while True:
                try:
                    key = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                resp = await client.post(
                    url,
                    json=payload,
                    headers={"Idempotency-Key": key}
                )
                data = resp.json()
                results.append((resp.status_code, key, data.get("job_id"), data.get("cached")))
            return results

        workers = [asyncio.create_task(client_worker()) for _ in range(num_concurrent_workers)]
        worker_results = await asyncio.gather(*workers)

    flat_results = [r for sublist in worker_results for r in sublist]
    assert len(flat_results) == total_requests

    # Verify all responses are 202 Accepted
    assert all(code == 202 for code, _, _, _ in flat_results)

    # Group responses by idempotency key
    key_to_job_ids = {}
    cached_hits = 0
    for _, key, job_id, is_cached in flat_results:
        key_to_job_ids.setdefault(key, set()).add(job_id)
        if is_cached is True:
            cached_hits += 1

    # Verify each idempotency key only produced exactly ONE job_id across all calls
    for key, job_ids in key_to_job_ids.items():
        assert len(job_ids) == 1, f"Idempotency violation for {key}: multiple job_ids {job_ids}"

    # Verify vast majority returned cached=True (at least 95% of repeated requests)
    assert cached_hits >= (total_requests - num_unique_keys * 2)

    # Verify PostgreSQL exact record counts
    async with AsyncSessionLocal() as session:
        jobs_count = await session.scalar(select(func.count(Job.id)))
        tasks_count = await session.scalar(select(func.count(JobTask.id)))

        assert jobs_count == num_unique_keys
        assert tasks_count == num_unique_keys
