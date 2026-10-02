import logging
import uuid
from typing import Optional
import redis.asyncio as redis
from src.config import settings

logger = logging.getLogger(__name__)

redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True, max_connections=1000)

# Atomic compare-and-swap Lua script: releases the lock only if the current value matches owner_id
RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class RedisDistributedLock:
    """Distributed lock using Redis with atomic Lua script for release."""

    def __init__(
        self,
        key: str,
        owner_id: Optional[str] = None,
        ttl_seconds: int = 30,
        client: Optional[redis.Redis] = None
    ):
        self.key = key
        self.owner_id = owner_id or str(uuid.uuid4())
        self.ttl_seconds = ttl_seconds
        self.client = client or redis_client
        self._acquired = False

    async def acquire(self) -> bool:
        """Acquire lock using atomic SET NX EX."""
        try:
            result = await self.client.set(
                self.key,
                self.owner_id,
                nx=True,
                ex=self.ttl_seconds
            )
            self._acquired = bool(result)
            return self._acquired
        except Exception as e:
            logger.warning("Error acquiring distributed lock for %s: %s", self.key, e)
            return False

    async def release(self) -> bool:
        """Release lock atomically using Lua script compare-and-swap."""
        try:
            res = await self.client.eval(RELEASE_LOCK_LUA, 1, self.key, self.owner_id)
            return bool(res == 1)
        except Exception as e:
            logger.warning("Error releasing distributed lock for %s: %s", self.key, e)
            return False
        finally:
            self._acquired = False

    async def is_locked(self) -> bool:
        """Check if lock key is currently set in Redis."""
        try:
            return bool(await self.client.exists(self.key))
        except Exception as e:
            logger.warning("Error checking lock state for %s: %s", self.key, e)
            return False

    async def __aenter__(self):
        acquired = await self.acquire()
        if not acquired:
            raise RuntimeError(f"Could not acquire distributed lock for {self.key}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
