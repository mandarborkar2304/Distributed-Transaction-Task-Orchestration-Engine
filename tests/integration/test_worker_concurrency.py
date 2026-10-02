import asyncio
import pytest
import pytest_asyncio
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select, func
from src.database import Base
from src.models import Job, JobTask, TaskStatus
from src.engine.worker import Worker

TEST_DB_URL = "postgresql+asyncpg://postgres:password@localhost:5432/orchestrator"
engine = create_async_engine(TEST_DB_URL, echo=False)
TestingSessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

@pytest_asyncio.fixture(scope="function")
async def db_setup():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
    except (ConnectionRefusedError, OSError) as e:
        pytest.skip(f"Database not available: {e}")
        
    yield

    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
    except Exception:
        pass


@pytest.mark.asyncio
async def test_worker_concurrency(db_setup):
    async with TestingSessionLocal() as session:
        # Seed 50 jobs
        jobs = []
        tasks = []
        for i in range(50):
            job = Job(idempotency_key=f"worker_test_{uuid.uuid4()}", job_type="payment")
            jobs.append(job)
        session.add_all(jobs)
        await session.flush()
        
        for job in jobs:
            task = JobTask(job_id=job.id, handler_name="payment_handler", payload={"amount": 100})
            tasks.append(task)
        session.add_all(tasks)
        await session.commit()

    workers = [Worker(batch_size=5) for _ in range(5)]
    
    async def run_worker(w):
        for _ in range(3): # run 3 cycles
            await w.run_once()
            await asyncio.sleep(0.1)

    await asyncio.gather(*(run_worker(w) for w in workers))
    
    async with TestingSessionLocal() as session:
        completed = await session.scalar(
            select(func.count(JobTask.id)).where(JobTask.status == TaskStatus.COMPLETED)
        )
        assert completed == 50


@pytest.mark.asyncio
async def test_worker_failure_backoff(db_setup):
    async with TestingSessionLocal() as session:
        job = Job(idempotency_key=f"worker_fail_{uuid.uuid4()}", job_type="payment")
        session.add(job)
        await session.flush()
        
        task = JobTask(job_id=job.id, handler_name="payment_handler", payload={"fail": True})
        session.add(task)
        await session.commit()
        task_id = task.id

    w = Worker(batch_size=1)
    
    # 1st attempt -> Fails, sleeps, set to PENDING
    await w.run_once()
    
    async with TestingSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.retry_count == 1
        assert t.status == TaskStatus.PENDING

    # 2nd attempt -> Fails
    await w.run_once()
    
    # 3rd attempt -> Fails, max retries reached, set to DEAD_LETTER
    await w.run_once()
    
    async with TestingSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.status == TaskStatus.DEAD_LETTER
        assert t.retry_count == 2
