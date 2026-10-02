import asyncio
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from src.database import AsyncSessionLocal
from src.models import JobTask, TaskStatus
from src.redis_client import redis_client, RedisDistributedLock
from src.engine.handlers.payment import PaymentTaskHandler
from src.engine.handlers.compute import ComputeTaskHandler

logger = logging.getLogger(__name__)

HANDLERS = {
    "payment_handler": PaymentTaskHandler(),
    "compute_handler": ComputeTaskHandler()
}


class Worker:
    def __init__(self, batch_size=10, worker_id=None):
        self.worker_id = worker_id or str(uuid.uuid4())
        self.batch_size = batch_size
        self._running = False
        
    async def _heartbeat_loop(self, task_id, cancel_event: asyncio.Event):
        while not cancel_event.is_set():
            try:
                # 10s wait, using wait_for allows quick cancellation
                await asyncio.wait_for(cancel_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                # Renew heartbeat in PostgreSQL
                try:
                    async with AsyncSessionLocal() as session:
                        stmt = update(JobTask).where(JobTask.id == task_id).values(
                            heartbeat_at=datetime.now(timezone.utc)
                        )
                        await session.execute(stmt)
                        await session.commit()
                except Exception as e:
                    logger.warning("Failed to renew DB heartbeat for task %s: %s", task_id, e)

                # Renew Redis lock TTL
                try:
                    await redis_client.expire(f"lock:task:{task_id}", 60)
                except Exception as e:
                    logger.warning("Failed to renew Redis lock TTL for task %s: %s", task_id, e)
            except asyncio.CancelledError:
                break
                
    def get_backoff(self, retry_count: int) -> float:
        return min(60.0, (2 ** retry_count) + random.uniform(0, 1))
        
    async def run_once(self):
        async with AsyncSessionLocal() as session:
            # Poll and claim
            stmt = (
                select(JobTask)
                .where(JobTask.status == TaskStatus.PENDING)
                .order_by(JobTask.created_at.asc())
                .limit(self.batch_size)
                .with_for_update(skip_locked=True)
            )
            result = await session.execute(stmt)
            tasks = result.scalars().all()
            
            if not tasks:
                return 0

            # Claim atomically in DB
            now = datetime.now(timezone.utc)
            for t in tasks:
                t.status = TaskStatus.RUNNING
                t.locked_by = self.worker_id
                t.heartbeat_at = now
                
            await session.commit()
            
        # Process claimed tasks concurrently
        coros = [self._process_task(t.id) for t in tasks]
        await asyncio.gather(*coros)
        
        return len(tasks)

    async def _process_task(self, task_id):
        # Acquire Redis distributed lock with worker_id ownership
        lock = RedisDistributedLock(f"lock:task:{task_id}", owner_id=self.worker_id, ttl_seconds=60)
        acquired = await lock.acquire()
        if not acquired:
            logger.warning("Worker %s could not acquire Redis lock for task %s, skipping", self.worker_id, task_id)
            return

        cancel_event = asyncio.Event()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(task_id, cancel_event))
        
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(JobTask).options(selectinload(JobTask.job)).where(JobTask.id == task_id)
                )
                task = result.scalars().first()
                if not task:
                    return
                
                handler = HANDLERS.get(task.handler_name)
                success = False
                error_msg = None
                
                from src.observability.metrics import tasks_total, execution_duration
                start_time = time.time()
                
                if handler:
                    try:
                        await handler.execute(task, task.payload)
                        success = True
                    except Exception as e:
                        error_msg = str(e)
                else:
                    error_msg = f"Unknown handler: {task.handler_name}"
                    
                duration = time.time() - start_time
                execution_duration.labels(handler=task.handler_name).observe(duration)
                    
                if success:
                    task.status = TaskStatus.COMPLETED
                    task.locked_by = None
                    task.heartbeat_at = None
                    
                    if task.job:
                        task.job.status = TaskStatus.COMPLETED
                        
                    tasks_total.labels(status="COMPLETED", handler=task.handler_name).inc()
                else:
                    if task.retry_count + 1 >= task.max_retries:
                        task.status = TaskStatus.DEAD_LETTER
                        task.last_error = error_msg
                        task.locked_by = None
                        task.heartbeat_at = None
                        
                        if task.job:
                            task.job.status = TaskStatus.FAILED
                            
                        tasks_total.labels(status="DEAD_LETTER", handler=task.handler_name).inc()
                    else:
                        tasks_total.labels(status="FAILED", handler=task.handler_name).inc()
                        
                        backoff = self.get_backoff(task.retry_count)
                        task.last_error = error_msg
                        await session.commit()
                        
                        await asyncio.sleep(backoff)
                        
                        task.status = TaskStatus.PENDING
                        task.retry_count += 1
                        task.locked_by = None
                        task.heartbeat_at = None
                        
                await session.commit()
                
        finally:
            cancel_event.set()
            await heartbeat_task
            await lock.release()

    async def start(self):
        self._running = True
        while self._running:
            processed = await self.run_once()
            if processed == 0:
                await asyncio.sleep(1)

    def stop(self):
        self._running = False
