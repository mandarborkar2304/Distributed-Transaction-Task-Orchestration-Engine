import json
from typing import Any, Dict
from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError

from src.database import get_db
from src.redis_client import redis_client
from src.models import Job, JobTask, TaskStatus

router = APIRouter(prefix="/v1/jobs")

class JobCreateRequest(BaseModel):
    job_type: str
    handler_name: str
    payload: Dict[str, Any] = {}

class JobResponse(BaseModel):
    job_id: str
    status: str
    cached: bool

@router.post("", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job(
    request: JobCreateRequest,
    idempotency_key: str = Header(None, alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db)
):
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is missing")

    redis_key = f"idemp:{idempotency_key}"
    
    # Fast-Path: Check Redis
    try:
        cached_val = await redis_client.get(redis_key)
        if cached_val:
            from src.observability.metrics import idempotency_hits
            idempotency_hits.inc()
            data = json.loads(cached_val)
            return JobResponse(job_id=data["job_id"], status=data["status"], cached=True)
    except Exception:
        # Ignore Redis errors during fast-path for resilience
        pass

    # Atomic Outbox
    async with db.begin():
        from sqlalchemy.dialects.postgresql import insert
        
        stmt = insert(Job).values(
            idempotency_key=idempotency_key, 
            job_type=request.job_type
        ).on_conflict_do_nothing(
            index_elements=['idempotency_key']
        ).returning(Job.id, Job.status)
        
        result = await db.execute(stmt)
        inserted_job = result.first()

        if inserted_job:
            job_id = str(inserted_job.id)
            job_status = inserted_job.status.value
            
            new_task = JobTask(
                job_id=inserted_job.id,
                handler_name=request.handler_name,
                payload=request.payload
            )
            db.add(new_task)
        else:
            # It already existed, let's fetch it
            sel_stmt = select(Job).where(Job.idempotency_key == idempotency_key)
            sel_result = await db.execute(sel_stmt)
            existing_job = sel_result.scalars().first()
            job_id = str(existing_job.id)
            job_status = existing_job.status.value

    # Set Redis key with 24-hr TTL
    response_payload = {"job_id": job_id, "status": job_status}
    try:
        await redis_client.set(redis_key, json.dumps(response_payload), ex=86400)
    except Exception:
        pass # If redis fails, we still accepted the job

    return JobResponse(job_id=job_id, status=job_status, cached=False)


@router.get("/{job_id}", response_model=Dict[str, Any])
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == job_id)
    result = await db.execute(stmt)
    job = result.scalars().first()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return {
        "id": str(job.id),
        "idempotency_key": job.idempotency_key,
        "job_type": job.job_type,
        "status": job.status.value,
        "created_at": job.created_at.isoformat() if job.created_at else None
    }
