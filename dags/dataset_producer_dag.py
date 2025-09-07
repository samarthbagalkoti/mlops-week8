from __future__ import annotations

import csv
import random
from datetime import datetime
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task

# A logical "asset" URI. Updating this URI notifies any DAG scheduled on it.
DATASET_URI = "file:///opt/airflow/artifacts/datasets/dataset.csv"
DATASET = Dataset(DATASET_URI)


@dag(
    dag_id="dataset_producer_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["w8", "dataset"],
)
def dataset_producer_dag():
    @task(outlets=[DATASET])
    def write_dataset(n: int = 100) -> str:
        path = Path("/opt/airflow/artifacts/datasets/dataset.csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(n):
                writer.writerow([i, random.random()])
        return str(path)

    write_dataset()


dag = dataset_producer_dag()
