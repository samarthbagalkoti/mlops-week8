import pytest
pytest.importorskip("airflow", reason="Airflow not installed in this test env")

from airflow.models import DagBag


def _get_dag(dag_id: str):
    dagbag = DagBag(include_examples=False)
    return dagbag.get_dag(dag_id)

