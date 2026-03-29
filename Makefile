# Steam Video Games project automation
# Run from repository root: make <target>

SHELL := /bin/sh

COMPOSE_FILE := steam-project/docker-compose.yml
DAG_ID := steamspy_top1000_etl

# Optional overrides:
# make set-bucket BUCKET=my-gcs-bucket
# make backfill DATE=2026-03-28
# make dag-state RUN_ID=scheduled__2026-03-28T12:00:00+00:00
BUCKET ?=
DATE ?=
RUN_ID ?=

.PHONY: help check-files build up down restart ps logs scheduler-logs webshell \
	airflow-db-migrate airflow-create-admin airflow-init set-bucket \
	unpause-dag pause-dag trigger-dag backfill dag-list-runs dag-state \
	dbt-build snapshot-latest clean-logs

help:
	@echo "Available targets:"
	@echo "  make build             - Build Docker images"
	@echo "  make up                - Start Airflow + Postgres containers"
	@echo "  make down              - Stop and remove containers"
	@echo "  make restart           - Restart containers"
	@echo "  make ps                - Show container status"
	@echo "  make logs              - Tail all compose logs"
	@echo "  make scheduler-logs    - Tail scheduler logs only"
	@echo "  make webshell          - Open shell in airflow-webserver"
	@echo "  make airflow-init      - Migrate DB + create admin user"
	@echo "  make set-bucket BUCKET=<name>"
	@echo "  make unpause-dag       - Unpause $(DAG_ID)"
	@echo "  make pause-dag         - Pause $(DAG_ID)"
	@echo "  make trigger-dag       - Trigger $(DAG_ID) now"
	@echo "  make backfill DATE=YYYY-MM-DD"
	@echo "  make dag-list-runs     - List DAG runs"
	@echo "  make dag-state RUN_ID=<run_id>"
	@echo "  make dbt-build         - Run external-table prep + dbt build"
	@echo "  make snapshot-latest   - Show latest steam_store_details snapshot"
	@echo "  make clean-logs        - Remove local Airflow logs"

check-files:
	@test -f $(COMPOSE_FILE) || (echo "Missing $(COMPOSE_FILE)"; exit 1)
	@test -f steam-project/.env || echo "Warning: steam-project/.env not found"
	@test -f steam-project/gcp/credentials.json || echo "Warning: steam-project/gcp/credentials.json not found"

build: check-files
	docker compose -f $(COMPOSE_FILE) build

up: check-files
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

restart:
	docker compose -f $(COMPOSE_FILE) restart

ps:
	docker compose -f $(COMPOSE_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) logs -f --tail=150

scheduler-logs:
	docker compose -f $(COMPOSE_FILE) logs -f --tail=200 airflow-scheduler

webshell:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver bash

airflow-db-migrate:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow db migrate

airflow-create-admin:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow users create \
		--username admin \
		--firstname Zoomcamp \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin || true

airflow-init: airflow-db-migrate airflow-create-admin

set-bucket:
	@test -n "$(BUCKET)" || (echo "BUCKET is required. Example: make set-bucket BUCKET=my-gcs-bucket"; exit 1)
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow variables set gcp_bucket_name "$(BUCKET)"

unpause-dag:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow dags unpause $(DAG_ID)

pause-dag:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow dags pause $(DAG_ID)

trigger-dag:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow dags trigger $(DAG_ID)

backfill:
	@test -n "$(DATE)" || (echo "DATE is required. Example: make backfill DATE=2026-03-28"; exit 1)
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow dags backfill $(DAG_ID) -s "$(DATE)" -e "$(DATE)"

dag-list-runs:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow dags list-runs -d $(DAG_ID)

dag-state:
	@test -n "$(RUN_ID)" || (echo "RUN_ID is required. Example: make dag-state RUN_ID=scheduled__2026-03-28T12:00:00+00:00"; exit 1)
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow dags state $(DAG_ID) "$(RUN_ID)"

dbt-build:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver bash -lc "python /opt/airflow/dbt/scripts/prepare_bigquery_external_tables.py"
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver bash -lc "dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select +path:models/marts"

snapshot-latest:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver bash -lc "ls -1 /opt/airflow/data/steam_store_details_*.csv 2>/dev/null | sort | tail -n 1"

clean-logs:
	rm -rf steam-project/logs/*
