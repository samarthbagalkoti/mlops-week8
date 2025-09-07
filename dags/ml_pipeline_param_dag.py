from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator

# Same dataset URI as the producer
DATASET = Dataset("file:///opt/airflow/artifacts/datasets/dataset.csv")


@dag(
    dag_id="ml_pipeline_param_dag",
    start_date=datetime(2024, 1, 1),
    schedule=DATASET,  # run when dataset is updated
    catchup=False,
    tags=["w8", "params"],
)
def ml_pipeline_param_dag():
    @task
    def validate_dataset(
        path: str = "/opt/airflow/artifacts/datasets/dataset.csv",
    ) -> str:
        p = Path(path)
        if not p.exists():
            raise AirflowFailException(f"Dataset missing at: {p}")
        return str(p)

    @task
    def train_with_params(C: float = 1.0, max_iter: int = 100) -> None:
        # Local import keeps Airflow worker import fast
        from src.train_eval import train_model

        train_model(C=C, max_iter=max_iter)

    tick = BashOperator(task_id="tick", bash_command="echo 'start params run'")
    v = validate_dataset()
    t = train_with_params()

    chain(tick, v, t)


dag = ml_pipeline_param_dag()
