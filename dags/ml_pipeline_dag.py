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


def _call_task(func_name: str) -> None:
    """Lazy-import src.train_eval and call the given function name."""
    mod = import_module("src.train_eval")
    fn = getattr(mod, func_name)
    fn()


default_args = {"retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="ml_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["w8", "pipeline"],
) as dag:
    train = PythonOperator(
        task_id="train",
        python_callable=_call_task,
        op_kwargs={"func_name": "train_model"},
    )
    evaluate = PythonOperator(
        task_id="evaluate",
        python_callable=_call_task,
        op_kwargs={"func_name": "evaluate"},
    )

    train >> evaluate
