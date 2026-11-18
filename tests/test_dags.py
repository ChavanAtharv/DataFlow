import importlib.util
from pathlib import Path


def test_dag_file_imports_cleanly():
    dag_path = Path("dags/dataflow_dag.py")
    assert dag_path.exists()

    spec = importlib.util.spec_from_file_location("dataflow_dag", dag_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Verify task functions exist
    assert hasattr(module, "task_bronze_ingest")
    assert hasattr(module, "task_schema_validation")
    assert hasattr(module, "task_quality_and_quarantine")
    assert hasattr(module, "task_spark_silver_transform")
    assert hasattr(module, "task_lakehouse_gold_parquet")
    assert hasattr(module, "task_postgres_load_and_merge")
    assert hasattr(module, "task_postgres_gold_marts")
