.PHONY: bootstrap full incremental backfill up down logs test

bootstrap:
	docker compose up -d --build airflow-db warehouse
	docker compose run --rm airflow-init

full: bootstrap
	docker compose run --rm airflow python /opt/project/src/download_data.py
	docker compose run --rm airflow python /opt/project/src/pipeline.py --full-refresh

incremental:
	docker compose run --rm airflow python /opt/project/src/download_data.py
	docker compose run --rm airflow python /opt/project/src/pipeline.py

backfill:
	docker compose run --rm airflow python /opt/project/src/pipeline.py --start-at "$(START)" --end-at "$(END)"

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

test:
	python -m pytest -q
