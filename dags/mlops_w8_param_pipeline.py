# dags/mlops_w8_param_pipeline.py
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _echo_conf(**context):
    """Read conf passed with --conf / --conf-file and log it."""
    conf = context["dag_run"].conf or {}
    print(f"Received conf: {conf!r}")
    return conf


def _train_with_params(**context):
    """Example training that reads parameters from conf and runs training."""
    from src.train_eval import train_model

    conf = context["ti"].xcom_pull(task_ids="echo_conf") or {}
    C = float(conf.get("C", 1.0))
    max_iter = int(conf.get("max_iter", 100))
    print(f"Training with C={C}, max_iter={max_iter}")
    train_model(C=C, max_iter=max_iter)


with DAG(
    dag_id="mlops_w8_param_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["w8", "params"],
) as dag:
    echo = PythonOperator(
        task_id="echo_conf",
        python_callable=_echo_conf,
        op_kwargs={},
    )
    train = PythonOperator(
        task_id="train_with_params",
        python_callable=_train_with_params,
        op_kwargs={},
    )
    echo >> train
