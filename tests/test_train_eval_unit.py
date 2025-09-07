from pathlib import Path

from src.train_eval import train_model


def test_train_model_fast():
    model_path, acc = train_model(C=1.0, max_iter=50)  # faster for CI
    assert Path(model_path).exists()
