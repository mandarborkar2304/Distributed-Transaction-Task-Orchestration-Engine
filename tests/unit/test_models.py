import pytest
import uuid
from src.models import Job, JobTask, TaskStatus


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
