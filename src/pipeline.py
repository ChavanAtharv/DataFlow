from src.ingestion.csv_ingestion import ingest_csv
from src.validation.schema_validator import validate_schema
from src.quality.data_quality import run_quality_checks
from src.transformation.spark_transform import transform_data


def run_pipeline(input_file: str):
    print("Starting DataFlow pipeline...")

    # 1. Ingestion
    df = ingest_csv(input_file)

    # 2. Schema validation
    validate_schema(df)

    # 3. Data quality
    quality_results = run_quality_checks(df)

    # 4. PySpark transformation
    spark, transformed_df = transform_data(input_file)

    print("Transformed data:")
    transformed_df.show()

    spark.stop()

    print("Pipeline completed successfully.")

    return quality_results


if __name__ == "__main__":
    run_pipeline("data/raw/customers.csv")
