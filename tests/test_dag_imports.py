from airflow.models import DagBag


def test_dag_imports_clean():
    dagbag = DagBag(include_examples=False)
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"


def test_expected_dags_exist():
    dagbag = DagBag(include_examples=False)
    for dag_id in ["dataset_producer_dag", "mlops_w8_param_pipeline"]:
        assert dag_id in dagbag.dags, f"Missing DAG: {dag_id}"
