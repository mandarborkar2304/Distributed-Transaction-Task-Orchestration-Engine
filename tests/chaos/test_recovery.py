import pytest
import uuid
from datetime import datetime, timezone, timedelta
from src.database import AsyncSessionLocal
from src.models import Job, JobTask, TaskStatus
from src.engine.watchdog import Watchdog


@pytest.mark.asyncio
async def test_watchdog_recovery(db_setup):
    async with AsyncSessionLocal() as session:
        job = Job(idempotency_key=f"chaos_test_{uuid.uuid4()}", job_type="payment")
        session.add(job)
        await session.flush()
        
        # 45 seconds in the past
        past_time = datetime.now(timezone.utc) - timedelta(seconds=45)
        
        task = JobTask(
            job_id=job.id, 
            handler_name="payment_handler", 
            status=TaskStatus.RUNNING,
            locked_by="dead_worker",
            heartbeat_at=past_time,
            retry_count=0
        )
        session.add(task)
        await session.commit()
        task_id = task.id

    watchdog = Watchdog()
    await watchdog.run_once()
    
    async with AsyncSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.status == TaskStatus.PENDING
        assert t.retry_count == 1
        assert t.locked_by is None
        assert t.heartbeat_at is None


@pytest.mark.asyncio
async def test_watchdog_dead_letter(db_setup):
    async with AsyncSessionLocal() as session:
        job = Job(idempotency_key=f"chaos_dead_{uuid.uuid4()}", job_type="payment")
        session.add(job)
        await session.flush()
        
        past_time = datetime.now(timezone.utc) - timedelta(seconds=45)
        
        task = JobTask(
            job_id=job.id, 
            handler_name="payment_handler", 
            status=TaskStatus.RUNNING,
            locked_by="dead_worker",
            heartbeat_at=past_time,
            retry_count=2,  # next is 3 which is max_retries (3)
            max_retries=3
        )
        session.add(task)
        await session.commit()
        task_id = task.id

    watchdog = Watchdog()
    await watchdog.run_once()
    
    async with AsyncSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.status == TaskStatus.DEAD_LETTER
        assert 'Watchdog: Heartbeat expired' in t.last_error
        assert t.locked_by is None
        assert t.heartbeat_at is None
