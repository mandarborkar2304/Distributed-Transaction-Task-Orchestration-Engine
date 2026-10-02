import asyncio
import logging
from typing import Any, Dict
from src.models import JobTask
from src.engine.handlers.base import BaseHandler

logger = logging.getLogger(__name__)


class PaymentTaskHandler(BaseHandler):
    async def execute(self, task: JobTask, payload: Dict[str, Any]) -> None:
        fail_simulation = payload.get("fail", False)
        # Gateway latency simulation (configurable via payload)
        latency = payload.get("latency", 0.1)
        if latency > 0:
            await asyncio.sleep(latency)
        if fail_simulation:
            raise Exception("Payment Gateway Error Simulated")
            
    async def rollback(self, task: JobTask, payload: Dict[str, Any]) -> None:
        logger.info("Executing refund rollback for payment task %s", task.id)
