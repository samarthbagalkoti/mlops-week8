# src/train_eval.py
from __future__ import annotations

import json
import os
import random
import time

ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/opt/airflow/artifacts")
os.makedirs(ARTIFACT_DIR, exist_ok=True)


def ingest():
    """Simulate data ingestion and return a small summary."""
    print("[ingest] starting")
    time.sleep(0.5)
    summary = {"rows": 1000, "source": "synthetic"}
    with open(os.path.join(ARTIFACT_DIR, "ingest_summary.json"), "w") as f:
        json.dump(summary, f)
    print("[ingest] wrote ingest_summary.json:", summary)
    return summary


def train():
    """Simulate training and write metrics.json."""
    print("[train] starting")
    time.sleep(0.5)
    metrics = {"auc": round(0.80 + random.random() * 0.05, 4)}
    with open(os.path.join(ARTIFACT_DIR, "metrics.json"), "w") as f:
        json.dump(metrics, f)
    print("[train] wrote metrics.json:", metrics)
    return metrics


def evaluate():
    """Evaluate using metrics.json and gate on AUC."""
    print("[evaluate] starting")
    path = os.path.join(ARTIFACT_DIR, "metrics.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"metrics.json not found at {path}")
    with open(path) as f:
        metrics = json.load(f)
    auc = float(metrics.get("auc", 0.0))
    passed = auc >= 0.82
    result = {"passed": passed, "auc": auc}
    print("[evaluate] result:", result)
    if not passed:
        raise RuntimeError(f"Quality gate failed (AUC={auc})")
    return result


def deploy():
    """Mock deploy step—would push model or flip a flag."""
    print("[deploy] starting")
    time.sleep(0.5)
    result = {"status": "deployed"}
    with open(os.path.join(ARTIFACT_DIR, "deploy_status.json"), "w") as f:
        json.dump(result, f)
    print("[deploy] wrote deploy_status.json:", result)
    return result
