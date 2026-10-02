import pytest
from src.redis_client import redis_client, RedisDistributedLock


@pytest.mark.asyncio
async def test_redis_distributed_lock_cas_lua():
    lock_key = "lock:test:resource"
    worker_1 = "worker_alpha"
    worker_2 = "worker_beta"

    lock1 = RedisDistributedLock(lock_key, owner_id=worker_1, ttl_seconds=10)
    lock2 = RedisDistributedLock(lock_key, owner_id=worker_2, ttl_seconds=10)

    # Worker 1 acquires lock
    assert await lock1.acquire() is True
    assert await lock1.is_locked() is True

    # Worker 2 cannot acquire while held
    assert await lock2.acquire() is False

    # Worker 2 cannot release Worker 1's lock (Lua CAS fails)
    # Simulate worker 2 attempting to release lock_key with its own owner_id
    released_by_wrong_worker = await redis_client.eval(
        """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """,
        1,
        lock_key,
        worker_2
    )
    assert released_by_wrong_worker == 0
    # Lock is still held by worker 1
    assert await lock1.is_locked() is True
    assert await redis_client.get(lock_key) == worker_1

    # Worker 1 releases its own lock (Lua CAS succeeds)
    assert await lock1.release() is True
    assert await lock1.is_locked() is False

    # Now Worker 2 can acquire
    assert await lock2.acquire() is True
    assert await redis_client.get(lock_key) == worker_2
    assert await lock2.release() is True
