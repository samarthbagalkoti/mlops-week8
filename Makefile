PROJECT=W8-D1-airflow-basics
COMPOSE        ?= docker compose
AIRFLOW_WS     ?= airflow-webserver
AIRFLOW_SCH    ?= airflow-scheduler
DAG_ID         ?= dq_and_schedule_dag
ARTIFACTS_DIR  ?= /home/airflow/artifacts
.PHONY: init up down clean logs ui trigger test backfill
.PHONY: airflow.bootstrap dq.trigger dq.backfill.3d dq.catchup.on dq.catchup.off init.reference


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
	@echo "Open http://54.234.80.17:8080  (user: admin, pass: admin)"

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



params.good:
	docker compose exec $(AIRFLOW_SVC) \
	airflow dags trigger mlops_w8_param_pipeline \
	--conf '{"threshold":0.90,"C":1.0,"max_iter":200}'

params.bad:
	docker compose exec $(AIRFLOW_SVC) \
	airflow dags trigger mlops_w8_param_pipeline \
	--conf '{"threshold":0.99,"C":0.5,"max_iter":100}'


# trigger a DAG (works from webserver or scheduler)
dataset.produce:
	docker compose exec airflow-webserver airflow dags trigger dataset_producer_dag

dataset.consume:
	docker compose exec airflow-webserver airflow dags trigger dataset_consumer_dag


.PHONY: lint format test airflow.init airflow.list airflow.test ci.local

lint:
	ruff check dags src tests

format:
	black dags src tests

test:
	pytest -q

airflow.init:
	AIRFLOW_HOME=.airflow AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__EXECUTOR=SequentialExecutor \\
	airflow db init

airflow.list:
	AIRFLOW_HOME=.airflow airflow dags list

airflow.test:
	AIRFLOW_HOME=.airflow airflow dags test dataset_producer_dag make_csv 2025-09-01; \\
	AIRFLOW_HOME=.airflow airflow dags test mlops_w8_param_pipeline evaluate 2025-09-01

ci.local: 
	lint format test airflow.init airflow.list airflow.test
	@echo "✅ Local CI parity run passed."


# --- W8:D5 helpers ---#
airflow.bootstrap:
	$(COMPOSE) exec -T $(AIRFLOW_WS)  bash -lc 'mkdir -p $(ARTIFACTS_DIR)/{datasets,reports}'
	$(COMPOSE) exec -T $(AIRFLOW_SCH) bash -lc 'mkdir -p $(ARTIFACTS_DIR)/{datasets,reports}'

dq.trigger:
	$(COMPOSE) exec -T $(AIRFLOW_WS) airflow dags trigger $(DAG_ID)

# Backfill last 3 days up to yesterday (UTC), compute dates inside container for portability
dq.backfill.3d:
	$(COMPOSE) exec -T $(AIRFLOW_WS) bash -lc '\
		START=$$(python -c "import datetime as dt; print((dt.datetime.utcnow()-dt.timedelta(days=3)).strftime(\"%Y-%m-%d\"))"); \
		END=$$(python -c "import datetime as dt; print((dt.datetime.utcnow()-dt.timedelta(days=1)).strftime(\"%Y-%m-%d\"))"); \
		echo \"Backfilling $${START}..$${END} for $(DAG_ID)\"; \
		airflow dags backfill -s $${START} -e $${END} $(DAG_ID) \
	'

dq.catchup.on:
	$(COMPOSE) exec -T $(AIRFLOW_WS) bash -lc '\
		airflow dags unpause $(DAG_ID); \
		airflow variables set _dummy 1 >/dev/null 2>&1 || true \
	'

dq.catchup.off:
	$(COMPOSE) exec -T $(AIRFLOW_WS) airflow dags pause $(DAG_ID)

# Initialize reference as a copy of current dataset (only if you want to force baseline now)
init.reference:
	$(COMPOSE) exec -T $(AIRFLOW_WS) bash -lc '\
		cp -f $(ARTIFACTS_DIR)/datasets/dataset.csv $(ARTIFACTS_DIR)/datasets/reference.csv && echo "reference set" \
	'
