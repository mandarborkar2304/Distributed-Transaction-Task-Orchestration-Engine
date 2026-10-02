from fastapi import FastAPI
from src.api.routes import router as jobs_router

app = FastAPI(title="Orchestrator API")

app.include_router(jobs_router)
