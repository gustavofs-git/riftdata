.PHONY: smoke dev docker-up docker-down test

smoke:
	.venv/bin/python scripts/smoke.py

dev:
	.venv/bin/dagster dev -m datarift.definitions

docker-up:
	docker compose up --build

docker-down:
	docker compose down

test:
	.venv/bin/pytest -x
