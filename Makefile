PROJECT=W8-D1-airflow-basics

.PHONY: init up down clean logs ui trigger test backfill

init:
	@echo "Setting up directories..."
	@mkdir -p artifacts logs

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

ui:
	@echo "Open http://54.147.138.39:8080  (user: admin, pass: admin)"

trigger:
	docker compose exec airflow-webserver airflow dags trigger mlops_w8_pipeline || true

test:
	@echo "Run a single task locally for debugging (evaluate task)"
	docker compose exec airflow-webserver airflow tasks test mlops_w8_pipeline evaluate $(shell date -u +"%Y-%m-%d")

backfill:
	@echo "Backfill yesterday only (example)"
	docker compose exec airflow-webserver airflow dags backfill -s $(shell date -u -d "1 day ago" +"%Y-%m-%d") -e $(shell date -u -d "1 day ago" +"%Y-%m-%d") mlops_w8_pipeline

clean:
	rm -rf artifacts/* logs/* pg_data/*

