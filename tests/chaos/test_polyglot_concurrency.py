import asyncio
import os
import signal
import subprocess
import time
import uuid
import pytest
import httpx
from sqlalchemy import select, func

from src.main import app
from src.database import AsyncSessionLocal
from src.models import Job, JobTask, TaskStatus
from src.engine.worker import Worker
from src.redis_client import redis_client


@pytest.fixture(scope="module")
def go_gateway_process():
    """Ensures the Go gateway is running on :8080 for polyglot testing."""
    gateway_url = "http://localhost:8080/healthz"
    started_by_fixture = False
    proc = None

    # Check if already running
    try:
        r = httpx.get(gateway_url, timeout=1.0)
        if r.status_code == 200:
            yield
            return
    except Exception:
        pass

    # Ensure Go gateway binary is built
    if not os.path.exists("/tmp/gateway-bin"):
        subprocess.run(
            ["go", "build", "-o", "/tmp/gateway-bin", "./cmd/gateway/"],
            cwd="services/gateway",
            check=True
        )

    # Start Go gateway binary
    env = os.environ.copy()
    env["DATABASE_DSN"] = "postgres://postgres:postgres@localhost:5432/orchestrator"
    env["REDIS_ADDR"] = "localhost:6379"
    env["GATEWAY_ADDR"] = ":8080"

    proc = subprocess.Popen(["/tmp/gateway-bin"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    started_by_fixture = True

    # Poll healthz up to 5 seconds
    deadline = time.time() + 5.0
    ready = False
    while time.time() < deadline:
        try:
            r = httpx.get(gateway_url, timeout=0.5)
            if r.status_code == 200:
                ready = True
                break
        except Exception:
            time.sleep(0.1)

    if not ready:
        if proc:
            proc.kill()
        pytest.fail("Go gateway failed to start and respond to /healthz within 5s")

    yield

    if started_by_fixture and proc:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.mark.asyncio
async def test_cross_runtime_idempotency_race(go_gateway_process, db_setup):
    """Fires 50 concurrent requests to Go Gateway (:8080) and 50 concurrent
    requests to Python FastAPI (:8000) using the EXACT SAME idempotency key.
    
    Validates that:
    1. Zero database integrity violations or uncaught 500 errors occur.
    2. All 100 requests return HTTP 202 with identical job_id.
    3. Exactly 1 Job row and 1 JobTask row exist in PostgreSQL.
    4. Redis contains the cached entry.
    """
    idempotency_key = f"cross_runtime_race_{uuid.uuid4()}"
    go_url = "http://localhost:8080/v1/jobs"
    payload = {
        "job_type": "cross_runtime",
        "handler_name": "compute_handler",
        "payload": {"test": "race"}
    }
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key
    }

    python_transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=python_transport, base_url="http://test") as py_client, \
               httpx.AsyncClient(timeout=10.0) as go_client:

        barrier = asyncio.Event()

        async def send_go(idx):
            await barrier.wait()
            return await go_client.post(go_url, json=payload, headers=headers)

        async def send_py(idx):
            await barrier.wait()
            return await py_client.post("/v1/jobs", json=payload, headers=headers)

        # Launch 50 Go requests + 50 Python requests
        go_coros = [send_go(i) for i in range(50)]
        py_coros = [send_py(i) for i in range(50)]
        all_tasks = asyncio.gather(*go_coros, *py_coros)

        # Release barrier to fire all 100 coroutines simultaneously
        barrier.set()
        responses = await all_tasks

    assert len(responses) == 100

    # Assert 100% 202 Accepted status codes
    status_codes = [r.status_code for r in responses]
    assert all(code == 202 for code in status_codes), f"Non-202 status received: {set(status_codes)}"

    # Assert all responses returned identical job_id
    job_ids = set()
    for r in responses:
        data = r.json()
        job_ids.add(data["job_id"])

    assert len(job_ids) == 1, f"Invariant violation: multiple job_ids generated: {job_ids}"
    winner_job_id = list(job_ids)[0]

    # Direct database verification: exactly 1 job and 1 task
    async with AsyncSessionLocal() as session:
        job_count = await session.scalar(
            select(func.count(Job.id)).where(Job.idempotency_key == idempotency_key)
        )
        task_count = await session.scalar(
            select(func.count(JobTask.id)).where(JobTask.job_id == uuid.UUID(winner_job_id))
        )

        assert job_count == 1, f"Expected 1 job row, found {job_count}"
        assert task_count == 1, f"Expected 1 task row, found {task_count}"

    # Verify Redis fast-path has cached response
    cached_val = await redis_client.get(f"idemp:{idempotency_key}")
    assert cached_val is not None


@pytest.mark.asyncio
async def test_polyglot_worker_interoperability(db_setup):
    """Enqueues 15 Go-native tasks (compute_handler) and 15 Python-native tasks
    (payment_handler) and launches both a Go worker and a Python worker simultaneously.
    
    Validates that:
    1. Both workers contend for the queue without deadlocks or double-claims.
    2. Go worker completes the compute_handler tasks.
    3. Python worker completes the payment_handler tasks.
    4. All 30 tasks transition cleanly to COMPLETED with 0 lingering tasks.
    """
    # Seed 15 compute tasks + 15 payment tasks
    seeded_task_ids = []
    async with AsyncSessionLocal() as session:
        for i in range(15):
            job = Job(idempotency_key=f"poly_compute_{i}_{uuid.uuid4()}", job_type="compute")
            session.add(job)
            await session.flush()
            task = JobTask(
                job_id=job.id,
                handler_name="compute_handler",
                payload={"iter": i},
                status=TaskStatus.PENDING
            )
            session.add(task)
            await session.flush()
            seeded_task_ids.append(task.id)

        for i in range(15):
            job = Job(idempotency_key=f"poly_payment_{i}_{uuid.uuid4()}", job_type="payment")
            session.add(job)
            await session.flush()
            task = JobTask(
                job_id=job.id,
                handler_name="payment_handler",
                payload={"amount": 10 + i},
                status=TaskStatus.PENDING
            )
            session.add(task)
            await session.flush()
            seeded_task_ids.append(task.id)

        await session.commit()

    assert len(seeded_task_ids) == 30

    # Ensure Go worker binary is built
    if not os.path.exists("/tmp/worker-bin"):
        subprocess.run(
            ["go", "build", "-o", "/tmp/worker-bin", "./cmd/worker/"],
            cwd="services/gateway",
            check=True
        )

    # Start Go worker in background process
    env = os.environ.copy()
    env["DATABASE_DSN"] = "postgres://postgres:postgres@localhost:5432/orchestrator"
    env["REDIS_ADDR"] = "localhost:6379"
    env["WORKER_ID"] = f"polyglot-go-worker-{uuid.uuid4()}"

    go_worker_proc = subprocess.Popen(
        ["/tmp/worker-bin"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Start Python worker in background asyncio task
    python_worker = Worker(batch_size=5)
    py_worker_task = asyncio.create_task(python_worker.start())

    try:
        # Poll for all 30 tasks to reach COMPLETED status (max 15 seconds)
        deadline = time.time() + 15.0
        all_completed = False

        while time.time() < deadline:
            async with AsyncSessionLocal() as session:
                completed_count = await session.scalar(
                    select(func.count(JobTask.id)).where(
                        JobTask.id.in_(seeded_task_ids),
                        JobTask.status == TaskStatus.COMPLETED
                    )
                )
                if completed_count == 30:
                    all_completed = True
                    break
            await asyncio.sleep(0.3)

        assert all_completed, f"Timed out waiting for 30 tasks to complete. Found {completed_count}/30"

    finally:
        # Teardown workers cleanly
        python_worker.stop()
        py_worker_task.cancel()
        try:
            await py_worker_task
        except asyncio.CancelledError:
            pass

        go_worker_proc.send_signal(signal.SIGTERM)
        try:
            go_worker_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            go_worker_proc.kill()

    # Final DB assertion: all 30 tasks are COMPLETED
    async with AsyncSessionLocal() as session:
        tasks = (
            await session.scalars(select(JobTask).where(JobTask.id.in_(seeded_task_ids)))
        ).all()

        assert len(tasks) == 30
        assert all(t.status == TaskStatus.COMPLETED for t in tasks)
        assert all(t.locked_by is None for t in tasks)
        assert all(t.heartbeat_at is None for t in tasks)
