import argparse
import datetime
import json
import logging
from pathlib import Path
import tempfile
import time

from src.config import (
    GOLD_PARQUET_BASE_PATH,
    LOG_FILE_PATH,
    QUARANTINE_DATA_PATH,
    RAW_DATA_PATH,
    SILVER_OUTPUT_PATH,
)
from src.ingestion.data_ingestor import ingest_data
from src.validation.schema_validator import validate_schema
from src.quality.data_quality import DataQualityError, quarantine_bad_records
from src.transformation.spark_transform import (
    build_gold_parquet_layers,
    transform_data,
)
from src.analytics.duckdb_layer import DuckDBAnalytics
from src.load.postgres_jdbc_loader import (
    ensure_schema_exists,
    merge_staging_to_customers,
    write_to_staging,
)
from src.analytics.gold_layer import build_gold_layer

# Ensure log directory exists
Path(LOG_FILE_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def run_pipeline(
    input_file: str = RAW_DATA_PATH,
    skip_db: bool = False,
    quarantine_path: str = QUARANTINE_DATA_PATH,
    partition_by: str | None = None,
    silver_output_path: str = SILVER_OUTPUT_PATH,
) -> dict:
    start_time = time.time()
    logger.info("Starting DataFlow pipeline")
    logger.info(
        "Configuration: input_file=%s, skip_db=%s, partition_by=%s",
        input_file,
        skip_db,
        partition_by,
    )

    try:
        # 1. Multi-format Ingestion - Bronze
        logger.info("Step 1: Ingesting source data")
        raw_df = ingest_data(input_file)

        # 2. Schema validation
        logger.info("Step 2: Validating schema")
        validate_schema(raw_df)

        # 3. Data quality checks & Quarantine dead-letter routing
        logger.info("Step 3: Auditing data quality and segregating corrupt records")
        clean_df, quarantined_df, quality_results = quarantine_bad_records(
            raw_df,
            quarantine_output_path=quarantine_path,
        )

        if len(quarantined_df) > 0:
            logger.warning(
                "Quarantined %d corrupt record(s) to: %s",
                len(quarantined_df),
                quarantine_path,
            )

        if clean_df.empty:
            raise DataQualityError("All ingested records failed quality checks; aborting pipeline.")

        # Determine path to feed into PySpark
        if len(quarantined_df) > 0:
            temp_clean_dir = Path(tempfile.mkdtemp(prefix="dataflow_clean_"))
            clean_csv_path = str(temp_clean_dir / "clean_customers.csv")
            clean_df.to_csv(clean_csv_path, index=False)
            spark_input = clean_csv_path
        else:
            spark_input = input_file

        # 4. PySpark transformation - Silver
        logger.info("Step 4: Transforming clean data with PySpark")
        spark, transformed_df = transform_data(
            spark_input,
            silver_output_path=silver_output_path,
            partition_by=partition_by,
        )

        logger.info("Transformed Silver data preview:")
        transformed_df.show()

        # 5. Lakehouse Gold Parquet Layer
        logger.info("Step 5: Generating Lakehouse Gold Parquet layers")
        build_gold_parquet_layers(transformed_df, gold_output_base=GOLD_PARQUET_BASE_PATH)

        # 6. In-Process DuckDB SQL Analytics on Silver Parquet
        logger.info("Step 6: Running serverless DuckDB OLAP queries on Parquet")
        duck_analytics = DuckDBAnalytics(silver_output_path)
        city_dist = duck_analytics.city_distribution()
        logger.info("DuckDB City distribution summary:\n%s", city_dist.to_string(index=False))

        if skip_db:
            logger.info("Skipping PostgreSQL steps (--skip-db flag set)")
        else:
            # 7. Database schema verification
            logger.info("Step 7: Verifying / initializing PostgreSQL target schema")
            ensure_schema_exists()

            # 8. Load into PostgreSQL staging
            logger.info("Step 8: Loading data into PostgreSQL staging via JDBC")
            write_to_staging(transformed_df)

            # 9. Merge into final PostgreSQL table
            logger.info("Step 9: Merging staging data into customers")
            merge_staging_to_customers()

            # 10. Build Gold analytics SQL marts
            logger.info("Step 10: Building Gold analytics SQL marts")
            build_gold_layer()

        spark.stop()

        duration = round(time.time() - start_time, 2)
        logger.info("DataFlow pipeline completed successfully in %s seconds", duration)

        # Generate run summary report
        report_dir = Path("logs/reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_file = report_dir / f"run_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_payload = {
            "status": "SUCCESS",
            "duration_seconds": duration,
            "input_file": str(input_file),
            "partition_by": partition_by,
            "metrics": quality_results,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        report_file.write_text(json.dumps(report_payload, indent=2))
        logger.info("Execution report saved to: %s", report_file)

        return quality_results

    except Exception:
        logger.exception("DataFlow pipeline failed")
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description="DataFlow Medallion ETL Pipeline"
    )
    parser.add_argument(
        "--input",
        "-i",
        default=RAW_DATA_PATH,
        help=f"Path to input CSV/JSON (default: {RAW_DATA_PATH})",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip PostgreSQL loading and Gold SQL steps (e.g. for offline/local testing)",
    )
    parser.add_argument(
        "--quarantine-path",
        default=QUARANTINE_DATA_PATH,
        help=f"Path to write quarantined corrupt records (default: {QUARANTINE_DATA_PATH})",
    )
    parser.add_argument(
        "--partition-by",
        default=None,
        help="Column to partition Silver Parquet by (e.g. city, age_group)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(
        input_file=args.input,
        skip_db=args.skip_db,
        quarantine_path=args.quarantine_path,
        partition_by=args.partition_by,
    )
