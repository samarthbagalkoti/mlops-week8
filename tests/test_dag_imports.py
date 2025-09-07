import pytest
pytest.importorskip("airflow", reason="Airflow not installed in this test env")

from airflow.models import DagBag


def test_dag_imports_clean():
    dagbag = DagBag(include_examples=False)
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"

