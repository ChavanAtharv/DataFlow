from src.ingestion.csv_ingestion import ingest_csv
from src.validation.schema_validator import validate_schema
from src.quality.data_quality import run_quality_checks


def run_pipeline(input_file: str):
    print("Starting DataFlow pipeline...")

    df = ingest_csv(input_file)
    validate_schema(df)

    quality_results = run_quality_checks(df)

    print("Pipeline completed successfully.")
    return df, quality_results


if __name__ == "__main__":
    run_pipeline("data/raw/customers.csv")
