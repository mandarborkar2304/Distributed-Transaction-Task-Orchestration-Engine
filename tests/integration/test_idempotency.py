import asyncio
import pytest
import uuid
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select, func
from src.database import AsyncSessionLocal
from src.models import Job, JobTask
from src.main import app
from src.redis_client import redis_client


@pytest.mark.asyncio
async def test_idempotent_ingestion(db_setup):
    idempotency_key = f"idemp_test_{uuid.uuid4()}"
    url = "/v1/jobs"
    headers = {"Idempotency-Key": idempotency_key}
    payload = {
        "job_type": "payment",
        "handler_name": "payment_handler",
        "payload": {"amount": 100}
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Fire 100 concurrent requests
        tasks = [
            client.post(url, json=payload, headers=headers)
            for _ in range(100)
        ]
        
        responses = await asyncio.gather(*tasks)

        # Check responses
        assert len(responses) == 100
        
        status_codes = [r.status_code for r in responses]
        assert all(code == 202 for code in status_codes)

        # Subsequent call hits Redis cache fast-path
        cached_resp = await client.post(url, json=payload, headers=headers)
        assert cached_resp.status_code == 202
        assert cached_resp.json().get("cached") is True

    # Validate live Redis key TTL
    ttl = await redis_client.ttl(f"idemp:{idempotency_key}")
    assert ttl > 0

    async with AsyncSessionLocal() as session:
        jobs_count = await session.scalar(select(func.count(Job.id)))
        tasks_count = await session.scalar(select(func.count(JobTask.id)))
        
        assert jobs_count == 1
        assert tasks_count == 1
