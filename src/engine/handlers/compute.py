import asyncio
import logging
from typing import Any, Dict
from src.models import JobTask
from src.engine.handlers.base import BaseHandler

logger = logging.getLogger(__name__)


class ComputeTaskHandler(BaseHandler):
    async def execute(self, task: JobTask, payload: Dict[str, Any]) -> None:
        stages = payload.get("stages", 3)
        stage_delay = payload.get("stage_delay", 0.1)
        for _ in range(stages):
            if stage_delay > 0:
                await asyncio.sleep(stage_delay)
            
    async def rollback(self, task: JobTask, payload: Dict[str, Any]) -> None:
        logger.info("Executing cleanup rollback for compute task %s", task.id)
