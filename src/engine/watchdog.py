import asyncio
import logging
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, or_
from sqlalchemy.orm import selectinload

from src.database import AsyncSessionLocal
from src.models import JobTask, TaskStatus
from src.redis_client import redis_client

logger = logging.getLogger(__name__)


class Watchdog:
    def __init__(self, interval: int = 15, timeout: int = 30):
        self.interval = interval
        self.timeout = timeout
        self._running = False
        
    async def run_once(self):
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(seconds=self.timeout)
        
        async with AsyncSessionLocal() as session:
            stmt = (
                select(JobTask)
                .options(selectinload(JobTask.job))
                .where(JobTask.status == TaskStatus.RUNNING)
                .where(
                    or_(
                        JobTask.heartbeat_at < threshold,
                        JobTask.heartbeat_at.is_(None)
                    )
                )
                .with_for_update(skip_locked=True)
            )
            
            result = await session.execute(stmt)
            tasks = result.scalars().all()
            
            for task in tasks:
                from src.observability.metrics import reclaimed_orphans
                reclaimed_orphans.inc()
                
                # Delete any stale redis distributed lock for this task
                try:
                    await redis_client.delete(f"lock:task:{task.id}")
                except Exception as e:
                    logger.warning("Failed to delete redis lock for task %s: %s", task.id, e)

                if task.retry_count + 1 >= task.max_retries:
                    task.status = TaskStatus.DEAD_LETTER
                    task.last_error = 'Watchdog: Heartbeat expired, max retries exceeded'
                    if task.job:
                        task.job.status = TaskStatus.FAILED
                else:
                    task.status = TaskStatus.PENDING
                    task.retry_count += 1
                
                task.locked_by = None
                task.heartbeat_at = None
                
            await session.commit()
            return len(tasks)
            
    async def start(self):
        self._running = True
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Watchdog encountered error during run_once: %s", e)
            await asyncio.sleep(self.interval)
            
    def stop(self):
        self._running = False
