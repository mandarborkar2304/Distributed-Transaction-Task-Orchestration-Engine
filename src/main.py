import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from prometheus_client import make_asgi_app
from src.api.routes import router as jobs_router

app = FastAPI(title="Orchestrator API")

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.include_router(jobs_router)


@app.get("/demo", response_class=HTMLResponse)
async def demo_ui():
    candidates = [
        "web/index.html",
        "../web/index.html",
        "/workspaces/Distributed-Transaction-Task-Orchestration-Engine/web/index.html",
    ]
    for p in candidates:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return f.read()
    return HTMLResponse("<h1>Demo visualizer not found</h1>", status_code=404)

