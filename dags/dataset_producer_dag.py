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
    start_date=datetime(2025, 9, 1),
    schedule="@hourly",  # simple cadence; you can trigger manually too
    catchup=False,
    tags=["w8", "datasets", "producer"],
)
def producer():
    @task(outlets=[DATASET])  # <-- tells Airflow this task updates DATASET
    def make_csv():
        base = Path("/opt/airflow/artifacts/datasets")
        base.mkdir(parents=True, exist_ok=True)
        p = base / "dataset.csv"
        # Write a tiny CSV with random numbers
        with p.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["value"])
            for _ in range(50):
                w.writerow([round(random.uniform(0, 1), 4)])
        return str(p)

    make_csv()


producer()
