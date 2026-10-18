import logging

from src.ingestion.csv_ingestion import ingest_csv
from src.validation.schema_validator import validate_schema
from src.quality.data_quality import run_quality_checks
from src.transformation.spark_transform import transform_data
from src.load.postgres_jdbc_loader import (
    write_to_staging,
    merge_staging_to_customers,
)
from src.analytics.gold_layer import build_gold_layer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/dataflow.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def run_pipeline(input_file: str):
    logger.info("Starting DataFlow pipeline")

    try:
        # 1. Ingestion - Bronze
        logger.info("Step 1: Ingesting source data")
        df = ingest_csv(input_file)

        # 2. Schema validation
        logger.info("Step 2: Validating schema")
        validate_schema(df)

        # 3. Data quality checks
        logger.info("Step 3: Running data quality checks")
        quality_results = run_quality_checks(df)

        # 4. PySpark transformation - Silver
        logger.info("Step 4: Transforming data with PySpark")
        spark, transformed_df = transform_data(input_file)

        logger.info("Transformed data:")
        transformed_df.show()

        # 5. Load into PostgreSQL staging
        logger.info("Step 5: Loading data into PostgreSQL staging")
        write_to_staging(transformed_df)

        # 6. Merge into final PostgreSQL table
        logger.info("Step 6: Merging staging data into customers")
        merge_staging_to_customers()

        # 7. Build Gold analytics layer
        logger.info("Step 7: Building Gold analytics layer")
        build_gold_layer()

        spark.stop()

        logger.info("DataFlow pipeline completed successfully")

        return quality_results

    except Exception:
        logger.exception("DataFlow pipeline failed")
        raise


if __name__ == "__main__":
    run_pipeline("data/raw/customers.csv")
