# dags/ml_pipeline_dag.py
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from importlib import import_module

# Ensure the container-mounted src/ is importable (compose also sets PYTHONPATH)
if "/opt/airflow/src" not in sys.path:
    sys.path.extend(["/opt/airflow", "/opt/airflow/src"])

from airflow import DAG
from airflow.operators.python import PythonOperator


def _call_task(func_name: str):
    """
    Lazy-imports src.train_eval and calls the given function name.
    This avoids DAG parse-time ImportErrors if src isn't mounted yet.
    """
    mod = import_module("src.train_eval")
    if not hasattr(mod, func_name):
        raise AttributeError(f"'src.train_eval' has no function '{func_name}'")
    func = getattr(mod, func_name)
    return func()


def task_ingest():
    return _call_task("ingest")


def task_train():
    return _call_task("train")


def task_evaluate():
    return _call_task("evaluate")


def task_deploy():
    return _call_task("deploy")


default_args = {
    "owner": "samarth",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    dag_id="mlops_w8_pipeline",
    default_args=default_args,
    schedule=None,  # Airflow 2.9+ uses 'schedule' (not schedule_interval)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["w8", "mlops"],
) as dag:
    t_ingest = PythonOperator(task_id="ingest", python_callable=task_ingest)
    t_train = PythonOperator(task_id="train", python_callable=task_train)
    t_eval = PythonOperator(task_id="evaluate", python_callable=task_evaluate)
    t_deploy = PythonOperator(task_id="deploy", python_callable=task_deploy)

    t_ingest >> t_train >> t_eval >> t_deploy
