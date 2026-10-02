from prometheus_client import Counter, Histogram

tasks_total = Counter(
    "orchestrator_tasks_total", 
    "Total tasks processed", 
    ["status", "handler"]
)

execution_duration = Histogram(
    "orchestrator_execution_duration_seconds", 
    "Task execution duration", 
    ["handler"]
)

reclaimed_orphans = Counter(
    "orchestrator_reclaimed_orphans_total", 
    "Total orphaned tasks reclaimed by watchdog"
)

idempotency_hits = Counter(
    "orchestrator_idempotency_hits_total", 
    "Total idempotent hits bypassing DB"
)
