import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.database import AsyncSessionLocal
from src.models import JobTask, TaskStatus

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
                .where(JobTask.heartbeat_at < threshold)
                .with_for_update(skip_locked=True)
            )
            
            result = await session.execute(stmt)
            tasks = result.scalars().all()
            
            for task in tasks:
                from src.observability.metrics import reclaimed_orphans
                reclaimed_orphans.inc()
                
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
            await self.run_once()
            await asyncio.sleep(self.interval)
            
    def stop(self):
        self._running = False
