# src/train_eval.py
from __future__ import annotations

import json
import os
import random
from pathlib import Path
from typing import Tuple

ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/opt/airflow/artifacts")
Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)


def train_model(C: float = 1.0, max_iter: int = 100) -> Tuple[str, float]:
    """Dummy trainer that writes a model file and metrics.json, returns (path, acc)."""
    models_dir = Path(ARTIFACT_DIR) / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    model_path = models_dir / f"model_C_{C}_iters_{max_iter}.bin"
    model_path.write_bytes(b"dummy-model-bytes")

    acc = round(0.7 + 0.3 * random.random(), 4)
    metrics = {"accuracy": acc, "C": C, "max_iter": max_iter}
    (Path(ARTIFACT_DIR) / "metrics.json").write_text(json.dumps(metrics))

    return str(model_path), acc


def evaluate(model_path: str | None = None) -> float:
    """Dummy evaluator that reads metrics.json if present; else returns a random score."""
    metrics_file = Path(ARTIFACT_DIR) / "metrics.json"
    if metrics_file.exists():
        data = json.loads(metrics_file.read_text())
        return float(data.get("accuracy", 0.0))
    return round(0.7 + 0.3 * random.random(), 4)
