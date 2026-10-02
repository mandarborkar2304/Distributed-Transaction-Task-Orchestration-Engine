from abc import ABC, abstractmethod
from typing import Any, Dict
from src.models import JobTask

class BaseHandler(ABC):
    @abstractmethod
    async def execute(self, task: JobTask, payload: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def rollback(self, task: JobTask, payload: Dict[str, Any]) -> None:
        pass
