import asyncio
import uuid
from datetime import datetime, timezone
import pytest
from sqlalchemy import insert, select, func
from src.database import AsyncSessionLocal
from src.models import Job, JobTask, TaskStatus
from src.engine.worker import Worker


@pytest.mark.asyncio
async def test_concurrency_grill_skip_locked(db_setup):
    """
    Concurrency Hardening Test:
    Seed 2,000 pending tasks and launch 40 concurrent async workers
    executing SELECT ... FOR UPDATE SKIP LOCKED simultaneously.
    Verify that len(processed_ids) == len(set(processed_ids)) == 2000
    with zero race conditions, double processing, or deadlocks.
    """
    total_tasks = 2000
    num_workers = 40
    batch_size = 25

    job_ids = [uuid.uuid4() for _ in range(total_tasks)]
    task_ids = [uuid.uuid4() for _ in range(total_tasks)]
    now = datetime.now(timezone.utc)

    jobs = [
        {
            "id": job_ids[i],
            "idempotency_key": f"grill_{i}_{uuid.uuid4()}",
            "job_type": "compute",
            "status": TaskStatus.PENDING,
            "created_at": now,
        }
        for i in range(total_tasks)
    ]
    tasks = [
        {
            "id": task_ids[i],
            "job_id": job_ids[i],
            "handler_name": "compute_handler",
            "status": TaskStatus.PENDING,
            "payload": {"stages": 0, "stage_delay": 0},
            "created_at": now,
            "updated_at": now,
        }
        for i in range(total_tasks)
    ]

    async with AsyncSessionLocal() as session:
        await session.execute(insert(Job), jobs)
        await session.execute(insert(JobTask), tasks)
        await session.commit()

    processed_ids = []
    processed_lock = asyncio.Lock()

    class GrillWorker(Worker):
        async def _process_task(self, task_id):
            async with processed_lock:
                processed_ids.append(task_id)
            await super()._process_task(task_id)

    workers = [
        GrillWorker(batch_size=batch_size, worker_id=f"grill-worker-{i}")
        for i in range(num_workers)
    ]

    async def worker_loop(w):
        idle_cycles = 0
        while idle_cycles < 3 and len(processed_ids) < total_tasks:
            n = await w.run_once()
            if n == 0:
                idle_cycles += 1
                await asyncio.sleep(0.02)
            else:
                idle_cycles = 0

    await asyncio.gather(*(worker_loop(w) for w in workers))

    # Verification: zero double processing, zero deadlocks
    assert len(processed_ids) == total_tasks
    assert len(processed_ids) == len(set(processed_ids)), "Race condition: duplicate task processing detected!"

    # Verify PostgreSQL final state
    async with AsyncSessionLocal() as session:
        completed = await session.scalar(
            select(func.count(JobTask.id)).where(JobTask.status == TaskStatus.COMPLETED)
        )
        pending = await session.scalar(
            select(func.count(JobTask.id)).where(JobTask.status == TaskStatus.PENDING)
        )
        assert completed == total_tasks
        assert pending == 0
