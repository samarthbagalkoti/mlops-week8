from src.train_eval import train_model, evaluate
from pathlib import Path

def test_train_model_fast():
    model_path, acc = train_model(C=1.0, max_iter=50)  # faster for CI
    assert Path(model_path).exists()
    assert 0.0 <= acc <= 1.0

def test_evaluate_returns_metric():
    acc = evaluate(threshold=0.5)
    assert 0.0 <= acc <= 1.0

