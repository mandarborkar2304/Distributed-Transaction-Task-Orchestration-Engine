import pytest
import pytest_asyncio
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.database import Base
from src.models import Job, JobTask, TaskStatus
from src.engine.watchdog import Watchdog

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
async def test_watchdog_recovery(db_setup):
    async with TestingSessionLocal() as session:
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
    
    async with TestingSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.status == TaskStatus.PENDING
        assert t.retry_count == 1
        assert t.locked_by is None
        assert t.heartbeat_at is None


@pytest.mark.asyncio
async def test_watchdog_dead_letter(db_setup):
    async with TestingSessionLocal() as session:
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
    
    async with TestingSessionLocal() as session:
        t = await session.get(JobTask, task_id)
        assert t.status == TaskStatus.DEAD_LETTER
        assert 'Watchdog: Heartbeat expired' in t.last_error
        assert t.locked_by is None
        assert t.heartbeat_at is None
