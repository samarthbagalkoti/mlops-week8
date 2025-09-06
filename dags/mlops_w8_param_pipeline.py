# dags/mlops_w8_param_pipeline.py
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def _echo_conf(**context):
    """Read conf passed with --conf / --conf-file and log it."""
    conf = context["dag_run"].conf or {}
    print("[echo_conf] received conf:", conf)
    return conf

def _train_with_params(**context):
    """
    Pretend to train using params from conf; fail if threshold too high
    so we can test good/bad runs.
    """
    conf = context["dag_run"].conf or {}
    threshold = float(conf.get("threshold", 0.82))
    C = float(conf.get("C", 1.0))
    max_iter = int(conf.get("max_iter", 200))

    print(f"[train] threshold={threshold}, C={C}, max_iter={max_iter}")
    # simulate a quality gate
    if threshold > 0.95:
        raise RuntimeError(f"threshold {threshold} is too high for this demo (gate)")
    print("[train] ok")
    return {"ok": True, "threshold": threshold, "C": C, "max_iter": max_iter}

with DAG(
    dag_id="mlops_w8_param_pipeline",   # <-- EXACT id you trigger
    start_date=datetime(2024, 1, 1),
    schedule=None,                      # manual trigger
    catchup=False,
    tags=["w8", "params"],
) as dag:
    echo = PythonOperator(task_id="echo_conf", python_callable=_echo_conf, op_kwargs={})
    train = PythonOperator(task_id="train_with_params", python_callable=_train_with_params, op_kwargs={})
    echo >> train

