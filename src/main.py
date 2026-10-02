from fastapi import FastAPI
from prometheus_client import make_asgi_app
from src.api.routes import router as jobs_router

app = FastAPI(title="Orchestrator API")

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.include_router(jobs_router)
