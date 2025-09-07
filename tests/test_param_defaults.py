from airflow.models import DagBag

def _get_dag(dag_id: str):
    dagbag = DagBag(include_examples=False)
    return dagbag.get_dag(dag_id)

def test_param_defaults_present():
    dag = _get_dag("mlops_w8_param_pipeline")
    assert dag is not None
    # Defaults set in @dag(params=...)
    assert "threshold" in dag.params
    assert "C" in dag.params
    assert "max_iter" in dag.params

def test_param_default_values():
    dag = _get_dag("mlops_w8_param_pipeline")
    assert float(dag.params["threshold"]) == 0.90
    assert float(dag.params["C"]) == 1.0
    assert int(dag.params["max_iter"]) == 200

