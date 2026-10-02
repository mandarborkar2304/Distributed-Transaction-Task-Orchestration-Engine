from abc import ABC, abstractmethod
from typing import Any, Dict
from src.models import JobTask


class BaseHandler(ABC):
    @abstractmethod
    async def execute(self, task: JobTask, payload: Dict[str, Any]) -> None:
        """Execute the task handler logic."""
        raise NotImplementedError

    @abstractmethod
    async def rollback(self, task: JobTask, payload: Dict[str, Any]) -> None:
        """Rollback the task handler actions upon failure."""
        raise NotImplementedError
