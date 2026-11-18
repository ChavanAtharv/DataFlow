"""
DataFlow Medallion Architecture - Apache Airflow Production DAG
Orchestrates Bronze Ingestion, Data Quality Gating, PySpark Silver Processing,
Lakehouse Gold Parquet Materialization, and PostgreSQL Warehouse Upsert.
"""

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    # Allow importing and linting in environments without Apache Airflow installed
    AIRFLOW_AVAILABLE = False
    DAG = object
    PythonOperator = object


DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["dataflow-alerts@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def task_bronze_ingest(**context):
    from src.config import RAW_DATA_PATH
    from src.ingestion.data_ingestor import ingest_data
    df = ingest_data(RAW_DATA_PATH)
    return {"raw_record_count": len(df)}


def task_schema_validation(**context):
    from src.config import RAW_DATA_PATH
    from src.ingestion.data_ingestor import ingest_data
    from src.validation.schema_validator import validate_schema
    df = ingest_data(RAW_DATA_PATH)
    validate_schema(df)
    return "Schema validation passed."


def task_quality_and_quarantine(**context):
    from src.config import QUARANTINE_DATA_PATH, RAW_DATA_PATH
    from src.ingestion.data_ingestor import ingest_data
    from src.quality.data_quality import quarantine_bad_records
    df = ingest_data(RAW_DATA_PATH)
    clean_df, quarantined_df, metrics = quarantine_bad_records(
        df, quarantine_output_path=QUARANTINE_DATA_PATH
    )
    return {
        "clean_count": len(clean_df),
        "quarantined_count": len(quarantined_df),
        "metrics": metrics,
    }


def task_spark_silver_transform(**context):
    from src.config import RAW_DATA_PATH
    from src.transformation.spark_transform import transform_data
    spark, transformed_df = transform_data(RAW_DATA_PATH, partition_by="city")
    count = transformed_df.count()
    spark.stop()
    return {"transformed_records": count}


def task_lakehouse_gold_parquet(**context):
    from src.config import RAW_DATA_PATH
    from src.transformation.spark_transform import (
        build_gold_parquet_layers,
        transform_data,
    )
    spark, transformed_df = transform_data(RAW_DATA_PATH)
    build_gold_parquet_layers(transformed_df)
    spark.stop()
    return "Lakehouse Gold Parquet materialized."


def task_postgres_load_and_merge(**context):
    from src.config import RAW_DATA_PATH
    from src.load.postgres_jdbc_loader import (
        ensure_schema_exists,
        merge_staging_to_customers,
        write_to_staging,
    )
    from src.transformation.spark_transform import transform_data
    spark, transformed_df = transform_data(RAW_DATA_PATH)
    ensure_schema_exists()
    write_to_staging(transformed_df)
    merge_staging_to_customers()
    spark.stop()
    return "PostgreSQL staging and merge completed."


def task_postgres_gold_marts(**context):
    from src.analytics.gold_layer import build_gold_layer
    build_gold_layer()
    return "PostgreSQL Gold analytical summary marts refreshed."


if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="dataflow_medallion_etl",
        default_args=DEFAULT_ARGS,
        description="End-to-End Medallion Lakehouse Pipeline with Quality Gating",
        schedule_interval="0 3 * * *",  # 03:00 AM daily
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["dataflow", "lakehouse", "spark", "postgres", "production"],
    ) as dag:

        ingest = PythonOperator(
            task_id="ingest_bronze",
            python_callable=task_bronze_ingest,
        )

        validate = PythonOperator(
            task_id="validate_schema",
            python_callable=task_schema_validation,
        )

        quality = PythonOperator(
            task_id="audit_and_quarantine",
            python_callable=task_quality_and_quarantine,
        )

        transform_silver = PythonOperator(
            task_id="pyspark_silver_transform",
            python_callable=task_spark_silver_transform,
        )

        gold_parquet = PythonOperator(
            task_id="lakehouse_gold_parquet",
            python_callable=task_lakehouse_gold_parquet,
        )

        postgres_staging = PythonOperator(
            task_id="postgres_staging_and_merge",
            python_callable=task_postgres_load_and_merge,
        )

        gold_sql = PythonOperator(
            task_id="postgres_gold_marts",
            python_callable=task_postgres_gold_marts,
        )

        # Pipeline DAG dependencies
        ingest >> validate >> quality >> transform_silver
        transform_silver >> [gold_parquet, postgres_staging]
        postgres_staging >> gold_sql
