from locust import HttpUser, task, between
import uuid

class OrchestratorUser(HttpUser):
    wait_time = between(1, 2)
    
    @task
    def create_job(self):
        idempotency_key = str(uuid.uuid4())
        
        self.client.post("/v1/jobs", json={
            "job_type": "payment",
            "handler_name": "payment_handler",
            "payload": {"amount": 500}
        }, headers={"Idempotency-Key": idempotency_key})

    @task(3)
    def create_job_idempotent(self):
        # Fire twice to hit the fast-path
        idempotency_key = str(uuid.uuid4())
        
        # First request
        self.client.post("/v1/jobs", json={
            "job_type": "payment",
            "handler_name": "payment_handler",
            "payload": {"amount": 500}
        }, headers={"Idempotency-Key": idempotency_key}, name="/v1/jobs (first)")

        # Second request (Idempotent hit)
        self.client.post("/v1/jobs", json={
            "job_type": "payment",
            "handler_name": "payment_handler",
            "payload": {"amount": 500}
        }, headers={"Idempotency-Key": idempotency_key}, name="/v1/jobs (cached)")
