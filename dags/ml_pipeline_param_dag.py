from __future__ import annotations
import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset
from airflow.utils.context import get_current_context
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException

# Same dataset URI as the producer
DATASET = Dataset("file:///opt/airflow/artifacts/datasets/dataset.csv")

@dag(
    dag_id="mlops_w8_param_pipeline",
    description="W8:D3 - Parametrized TaskFlow DAG scheduled by a Dataset",
    start_date=datetime(2025, 9, 1),
    schedule=[DATASET],       # <-- triggers when producer updates DATASET
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "samarth",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={                   # default knobs (overridable per trigger)
        "threshold": 0.90,
        "C": 1.0,
        "max_iter": 200
    },
    tags=["w8", "datasets", "params", "taskflow"],
)
def pipeline():
    @task
    def prepare_run_dir() -> str:
        """Create a unique run folder using the execution date; return its path."""
        ctx = get_current_context()
        run_dir = Path(f"/opt/airflow/artifacts/run_{ctx['ds_nodash']}")
        run_dir.mkdir(parents=True, exist_ok=True)
        return str(run_dir)

    @task
    def train(C: float, max_iter: int) -> dict:
        """Train model using params taken from dag_run.conf or defaults."""
        from src.train_eval import train_model
        model_path, acc = train_model(C=C, max_iter=max_iter)
        return {"model_path": model_path, "train_acc": float(acc)}

    @task
    def evaluate(threshold: float) -> dict:
        """Evaluate with a param threshold; keep return payload small (scalars)."""
        from src.train_eval import evaluate as eval_fn
        acc = float(eval_fn(threshold=threshold))
        return {"accuracy": acc, "threshold": float(threshold)}

    @task
    def gate(metrics: dict) -> dict:
        """Stop pipeline if accuracy is below threshold."""
        acc = float(metrics["accuracy"])
        thr = float(metrics["threshold"])
        if acc < thr:
            raise AirflowFailException(
                f"Gate failed: accuracy {acc:.3f} < threshold {thr:.2f}"
            )
        return {"gate": "passed", "accuracy": acc}

    # --- Read runtime params safely (prefers dag_run.conf over params) ---
    ctx = get_current_context()
    conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
    p = ctx["params"]
    threshold = float(conf.get("threshold", p["threshold"]))
    C = float(conf.get("C", p["C"]))
    max_iter = int(conf.get("max_iter", p["max_iter"]))

    run_dir = prepare_run_dir()
    t = train(C=C, max_iter=max_iter)
    e = evaluate(threshold=threshold)
    g = gate(e)

    # --- Tiny Jinja templating demo (BashOperator) ---
    # Write a meta file capturing the run facts using Jinja templates.
    stamp = BashOperator(
        task_id="stamp_run_meta",
        bash_command=(
            "echo 'ds={{ ds }} run_id={{ run_id }} "
            "threshold={{ dag_run.conf.get(\"threshold\", params.threshold) }} "
            "C={{ dag_run.conf.get(\"C\", params.C) }} "
            "max_iter={{ dag_run.conf.get(\"max_iter\", params.max_iter) }}' "
            "> {{ ti.xcom_pull(task_ids='prepare_run_dir') }}/meta.txt"
        ),
    )

    chain(run_dir, t, e, g, stamp)

pipeline()

