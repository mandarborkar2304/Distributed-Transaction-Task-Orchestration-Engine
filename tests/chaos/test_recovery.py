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


@pytest.mark.asyncio
async def test_watchdog_recovers_crashed_go_worker(db_setup):
    """Simulates a hard Go worker failure (SIGKILL/crash) where a task is RUNNING
    with an active Redis distributed lock lease and a stale heartbeat.
    Validates that the Watchdog:
    1. Detects stale heartbeat (heartbeat_at < NOW() - 30s).
    2. Reclaims task back to PENDING with retry_count incremented.
    3. Cleans up the orphaned Redis distributed lock key.
    """
    from src.redis_client import redis_client

    async with AsyncSessionLocal() as session:
        job = Job(idempotency_key=f"go_worker_crash_{uuid.uuid4()}", job_type="compute")
        session.add(job)
        await session.flush()

        stale_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=45)
        go_worker_id = f"go-worker-pid-8849-{uuid.uuid4()}"

        task = JobTask(
            job_id=job.id,
            handler_name="compute_handler",
            status=TaskStatus.RUNNING,
            locked_by=go_worker_id,
            heartbeat_at=stale_heartbeat,
            retry_count=0,
            max_retries=3
        )
        session.add(task)
        await session.commit()
        task_id = task.id

    # Simulate Go worker acquiring Redis distributed lock before crashing
    redis_lock_key = f"lock:task:{task_id}"
    await redis_client.set(redis_lock_key, go_worker_id, ex=60)
    assert await redis_client.exists(redis_lock_key) == 1

    # Run Watchdog reaper
    watchdog = Watchdog(interval=1, timeout=30)
    reclaimed = await watchdog.run_once()
    assert reclaimed >= 1

    # Verify task state in database
    async with AsyncSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.status == TaskStatus.PENDING
        assert t.retry_count == 1
        assert t.locked_by is None
        assert t.heartbeat_at is None

    # Verify orphaned Redis lock was deleted by Watchdog
    assert await redis_client.exists(redis_lock_key) == 0


@pytest.mark.asyncio
async def test_watchdog_dlq_crashed_go_worker_exhausted_retries(db_setup):
    """Validates that if a Go worker crashes on a task that has already reached
    max_retries, the Watchdog routes it to DEAD_LETTER and marks the parent job FAILED.
    """
    from src.redis_client import redis_client

    async with AsyncSessionLocal() as session:
        job = Job(idempotency_key=f"go_worker_poison_{uuid.uuid4()}", job_type="compute")
        session.add(job)
        await session.flush()
        job_id = job.id

        stale_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=45)
        go_worker_id = f"go-worker-crashed-{uuid.uuid4()}"

        task = JobTask(
            job_id=job.id,
            handler_name="compute_handler",
            status=TaskStatus.RUNNING,
            locked_by=go_worker_id,
            heartbeat_at=stale_heartbeat,
            retry_count=2,  # next attempt (3) reaches max_retries (3)
            max_retries=3
        )
        session.add(task)
        await session.commit()
        task_id = task.id

    redis_lock_key = f"lock:task:{task_id}"
    await redis_client.set(redis_lock_key, go_worker_id, ex=60)

    watchdog = Watchdog(interval=1, timeout=30)
    await watchdog.run_once()

    async with AsyncSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.status == TaskStatus.DEAD_LETTER
        assert "Watchdog: Heartbeat expired" in t.last_error
        assert t.locked_by is None
        assert t.heartbeat_at is None

        parent_job = await session.get(Job, job_id)
        assert parent_job.status == TaskStatus.FAILED

    assert await redis_client.exists(redis_lock_key) == 0

