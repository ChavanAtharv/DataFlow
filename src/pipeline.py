from src.ingestion.csv_ingestion import ingest_csv
from src.validation.schema_validator import validate_schema
from src.quality.data_quality import run_quality_checks
from src.transformation.spark_transform import transform_data
from src.load.postgres_jdbc_loader import (
    write_to_staging,
    merge_staging_to_customers,
)
from src.analytics.gold_layer import build_gold_layer


def run_pipeline(input_file: str):
    print("Starting DataFlow pipeline...")

    # 1. Ingestion - Bronze
    df = ingest_csv(input_file)

    # 2. Schema validation
    validate_schema(df)

    # 3. Data quality checks
    quality_results = run_quality_checks(df)

    # 4. PySpark transformation - Silver
    spark, transformed_df = transform_data(input_file)

    print("Transformed data:")
    transformed_df.show()

    # 5. Load Spark DataFrame into PostgreSQL staging table
    write_to_staging(transformed_df)

    # 6. Merge staging data into final PostgreSQL table
    merge_staging_to_customers()

    # 7. Build Gold analytics layer
    build_gold_layer()

    spark.stop()

    print("Pipeline completed successfully.")

    return quality_results


if __name__ == "__main__":
    run_pipeline("data/raw/customers.csv")
