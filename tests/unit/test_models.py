import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.exc import DBAPIError
from src.database import Base
from src.models import Job, JobTask, TaskStatus
import uuid
import asyncpg

TEST_DB_URL = "postgresql+asyncpg://postgres:password@localhost:5432/orchestrator"

engine = create_async_engine(TEST_DB_URL, echo=False)
TestingSessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

@pytest_asyncio.fixture(scope="function")
async def db_session():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
    except (ConnectionRefusedError, OSError) as e:
        pytest.skip(f"Database not available: {e}")
    
    async with TestingSessionLocal() as session:
        yield session

    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
    except Exception:
        pass

@pytest.mark.asyncio
async def test_job_and_task_creation(db_session):
    idempotency_key = f"idemp_key_{uuid.uuid4()}"
    job = Job(
        idempotency_key=idempotency_key,
        job_type="payment",
        status=TaskStatus.PENDING
    )
    
    task = JobTask(
        handler_name="payment_handler",
        status=TaskStatus.PENDING,
        payload={"amount": 100},
        job=job
    )
    
    db_session.add(job)
    await db_session.commit()
    
    await db_session.refresh(job)
    
    assert job.id is not None
    assert job.idempotency_key == idempotency_key
    assert job.status == TaskStatus.PENDING
    
    assert len(job.tasks) == 1
    assert job.tasks[0].id is not None
    assert job.tasks[0].handler_name == "payment_handler"
    assert job.tasks[0].payload == {"amount": 100}

@pytest.mark.asyncio
async def test_job_cascade_delete(db_session):
    job = Job(
        idempotency_key=f"del_{uuid.uuid4()}",
        job_type="compute"
    )
    task = JobTask(
        handler_name="compute_handler",
        job=job
    )
    db_session.add(job)
    await db_session.commit()
    
    task_id = task.id
    
    await db_session.delete(job)
    await db_session.commit()
    
    task_in_db = await db_session.get(JobTask, task_id)
    assert task_in_db is None
