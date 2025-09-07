# ./dags/dq_and_schedule_dag.py
from __future__ import annotations

import os
import csv
import json
import math
import random
from pathlib import Path
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context  # stable path across 2.x

# Writable location inside Airflow containers (persists while container lives)
ARTIFACTS_DIR = Path(os.environ.get("ARTIFACTS_DIR", "/home/airflow/artifacts"))
DATASETS_DIR = ARTIFACTS_DIR / "datasets"
REPORTS_DIR = ARTIFACTS_DIR / "reports"

DATASET_FILE = DATASETS_DIR / "dataset.csv"
REFERENCE_FILE = DATASETS_DIR / "reference.csv"

DAG_ID = "dq_and_schedule_dag"

# If you want the DAG to actually fail when drift is high, set env FAIL_ON_DRIFT=1
FAIL_ON_DRIFT = os.environ.get("FAIL_ON_DRIFT", "0") == "1"
DRIFT_THRESHOLD = float(os.environ.get("DRIFT_THRESHOLD", "1000000"))  # huge default → won't fail

def _ensure_dirs():
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def _seeded_numbers(seed: int, n: int = 500) -> list[float]:
    # Deterministic but similar distribution across days
    rnd = random.Random(seed)
    return [rnd.gauss(100.0, 5.0) for _ in range(n)]

def _write_csv(path: Path, values: list[float]) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["value"])
        for v in values:
            w.writerow([f"{v:.6f}"])

def _read_csv(path: Path) -> list[float]:
    vals = []
    with path.open("r") as f:
        r = csv.reader(f)
        header = next(r, None)
        for row in r:
            try:
                vals.append(float(row[0]))
            except Exception:
                pass
    return vals

def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else float("nan")

def _std(xs: list[float]) -> float:
    if not xs:
        return float("nan")
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / max(1, len(xs) - 1))

@dag(
    dag_id=DAG_ID,
    schedule="@daily",                       # <- was None
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,                           # backfill CLI ignores this, but fine to keep
    tags=["dq","drift","stdlib"],
)
def dq_and_schedule_dag():
    @task
    def prepare_run_dir():
        _ensure_dirs()
        return str(ARTIFACTS_DIR)

    @task
    def require_dataset(_artifacts_dir: str):
        """
        Ensure dataset.csv exists. If missing, generate a deterministic synthetic dataset
        based on logical_date so backfills produce stable data.
        """
        ctx = get_current_context()
        logical_date = ctx["logical_date"]  # pendulum DateTime
        if not DATASET_FILE.exists():
            # Seed using YYYYMMDD to get stable but day-dependent data
            seed = int(logical_date.strftime("%Y%m%d"))
            vals = _seeded_numbers(seed, n=500)
            _write_csv(DATASET_FILE, vals)
        return str(DATASET_FILE)

    @task
    def dq_checks(_dataset_path: str):
        """
        Simple sanity checks: file exists, non-empty, basic stats.
        Writes a small metrics json.
        """
        assert DATASET_FILE.exists(), f"dataset missing at {DATASET_FILE}"
        vals = _read_csv(DATASET_FILE)
        assert len(vals) > 0, "dataset has no rows"
        metrics = {
            "count": len(vals),
            "mean": _mean(vals),
            "std": _std(vals),
            "path": str(DATASET_FILE),
        }
        report_path = REPORTS_DIR / f"{DAG_ID}_metrics.json"
        report_path.write_text(json.dumps(metrics, indent=2))
        return metrics

    @task
    def drift(_metrics: dict):
        """
        Compare current dataset to reference. If reference missing, create it (baseline).
        Produces a JSON report. Only fails if FAIL_ON_DRIFT=1 and metric > DRIFT_THRESHOLD.
        """
        if not REFERENCE_FILE.exists():
            # First run: set baseline and exit
            vals = _read_csv(DATASET_FILE)
            _write_csv(REFERENCE_FILE, vals)

        current = _read_csv(DATASET_FILE)
        reference = _read_csv(REFERENCE_FILE)

        cur_mean, ref_mean = _mean(current), _mean(reference)
        cur_std, ref_std = _std(current), _std(reference)
        mean_abs_diff = abs(cur_mean - ref_mean)

        ctx = get_current_context()
        run_id = ctx["run_id"].replace(":", "_")
        exec_dt = ctx["logical_date"].to_datetime_string()

        out = {
            "run_id": run_id,
            "execution_time": exec_dt,
            "dataset_path": str(DATASET_FILE),
            "reference_path": str(REFERENCE_FILE),
            "current": {"count": len(current), "mean": cur_mean, "std": cur_std},
            "reference": {"count": len(reference), "mean": ref_mean, "std": ref_std},
            "mean_abs_diff": mean_abs_diff,
            "threshold": DRIFT_THRESHOLD,
            "fail_on_drift": FAIL_ON_DRIFT,
        }

        out_path = REPORTS_DIR / f"{DAG_ID}_drift_{run_id}.json"
        out_path.write_text(json.dumps(out, indent=2))

        # Gate only when explicitly enabled
        if FAIL_ON_DRIFT and mean_abs_diff > DRIFT_THRESHOLD:
            raise ValueError(
                f"Drift detected: |mean(current)-mean(reference)|={mean_abs_diff:.6f} > {DRIFT_THRESHOLD}"
            )

        return {"report": str(out_path), "mean_abs_diff": mean_abs_diff}

    a = prepare_run_dir()
    b = require_dataset(a)
    c = dq_checks(b)
    drift(c)

dq_and_schedule_dag()

