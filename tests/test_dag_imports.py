import pytest

airflow = pytest.importorskip("airflow", reason="Airflow not installed in this test env")


def test_dag_imports_clean():
    DagBag = airflow.models.DagBag
    dagbag = DagBag(include_examples=False)
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"
