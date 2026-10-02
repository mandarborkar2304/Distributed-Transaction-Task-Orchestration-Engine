import pytest
import pytest_asyncio
from sqlalchemy import text

from src.database import engine, Base, AsyncSessionLocal
from src.redis_client import redis_client


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def init_database():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


@pytest_asyncio.fixture(scope="function", autouse=True)
async def clean_database():
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE job_tasks, jobs CASCADE;"))
    await redis_client.flushdb()
    
    yield
    
    try:
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE job_tasks, jobs CASCADE;"))
        await redis_client.flushdb()
    finally:
        await redis_client.connection_pool.disconnect()


@pytest_asyncio.fixture(scope="function")
async def db_session():
    async with AsyncSessionLocal() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def db_setup():
    yield
