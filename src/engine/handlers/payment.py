import asyncio
from typing import Any, Dict
from src.models import JobTask
from src.engine.handlers.base import BaseHandler

class PaymentTaskHandler(BaseHandler):
    async def execute(self, task: JobTask, payload: Dict[str, Any]) -> None:
        fail_simulation = payload.get("fail", False)
        await asyncio.sleep(0.1) # Simulate gateway latency
        if fail_simulation:
            raise Exception("Payment Gateway Error Simulated")
            
    async def rollback(self, task: JobTask, payload: Dict[str, Any]) -> None:
        pass # Optional refund logic
