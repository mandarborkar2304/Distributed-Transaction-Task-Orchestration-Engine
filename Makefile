.PHONY: up down lint test chaos-test benchmark install

VENV_DIR = venv

up:
	docker-compose up -d

down:
	docker-compose down -v

install:
	python -m venv $(VENV_DIR)
	$(VENV_DIR)\Scripts\pip install -r requirements.txt

lint:
	$(VENV_DIR)\Scripts\black src tests
	$(VENV_DIR)\Scripts\ruff check src tests

test:
	$(VENV_DIR)\Scripts\pytest tests/unit tests/integration

chaos-test:
	$(VENV_DIR)\Scripts\pytest tests/chaos

benchmark:
	$(VENV_DIR)\Scripts\locust -f tests/benchmark/locustfile.py --headless -u 500 -r 100 -t 30s
