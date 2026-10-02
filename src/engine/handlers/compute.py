import asyncio
from typing import Any, Dict
from src.models import JobTask
from src.engine.handlers.base import BaseHandler

class ComputeTaskHandler(BaseHandler):
    async def execute(self, task: JobTask, payload: Dict[str, Any]) -> None:
        stages = payload.get("stages", 3)
        for i in range(stages):
            # simulate stage progress
            await asyncio.sleep(0.1)
            
    async def rollback(self, task: JobTask, payload: Dict[str, Any]) -> None:
        pass
