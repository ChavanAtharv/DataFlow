from src.ingestion.csv_ingestion import ingest_csv
from src.validation.schema_validator import validate_schema
from src.quality.data_quality import run_quality_checks
from src.transformation.spark_transform import transform_data
from src.load.postgres_loader import load_customers


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

    # 5. Convert transformed Spark data to rows
    rows = [
        (
            int(row["customer_id"]),
            row["name"],
            row["email"],
            int(row["age"]) if row["age"] is not None else None,
            row["city"],
            row["age_group"],
        )
        for row in transformed_df.collect()
    ]

    # 6. Load into PostgreSQL
    load_customers(rows)

    spark.stop()

    print("Pipeline completed successfully.")

    return quality_results


if __name__ == "__main__":
    run_pipeline("data/raw/customers.csv")

