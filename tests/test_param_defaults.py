import pytest

airflow = pytest.importorskip("airflow", reason="Airflow not installed in this test env")


def _get_dag(dag_id: str):
    DagBag = airflow.models.DagBag
    dagbag = DagBag(include_examples=False)
    return dagbag.get_dag(dag_id)

