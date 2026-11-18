from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lower, trim, when

from src.config import (
    GOLD_PARQUET_BASE_PATH,
    SILVER_OUTPUT_PATH,
    SPARK_APP_NAME,
    SPARK_JDBC_JAR,
    SPARK_MASTER,
)


def get_spark_session() -> SparkSession:
    """Initialize or retrieve local SparkSession."""
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars", SPARK_JDBC_JAR)
        .getOrCreate()
    )


def transform_data(
    input_path: str,
    silver_output_path: str = SILVER_OUTPUT_PATH,
    spark: SparkSession | None = None,
) -> tuple[SparkSession, DataFrame]:
    """
    Transform Bronze CSV data into cleaned, standardized, and enriched Silver Parquet data.
    """
    if spark is None:
        spark = get_spark_session()

    # Read Bronze data
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # Clean and transform data into the Silver layer with audit timestamp
    transformed_df = (
        df
        .withColumn("name", trim(col("name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("city", trim(col("city")))
        .withColumn(
            "age_group",
            when(col("age") < 25, "Young")
            .when(col("age") <= 30, "Adult")
            .otherwise("Senior"),
        )
        .withColumn("processed_at", current_timestamp())
    )

    # Persist Silver data in Parquet format
    (
        transformed_df.write
        .mode("overwrite")
        .parquet(silver_output_path)
    )

    print(f"Silver layer written to: {silver_output_path}")

    return spark, transformed_df


def build_gold_parquet_layers(
    silver_df: DataFrame,
    gold_output_base: str = GOLD_PARQUET_BASE_PATH,
) -> dict[str, DataFrame]:
    """
    Materialize Gold aggregated analytics datasets as Parquet in the lakehouse.
    """
    city_summary_path = f"{gold_output_base}/city_summary"
    age_summary_path = f"{gold_output_base}/age_summary"

    city_summary_df = (
        silver_df.groupBy("city")
        .count()
        .withColumnRenamed("count", "customer_count")
        .orderBy(col("customer_count").desc())
    )

    age_summary_df = (
        silver_df.groupBy("age_group")
        .count()
        .withColumnRenamed("count", "customer_count")
        .orderBy(col("customer_count").desc())
    )

    (
        city_summary_df.write
        .mode("overwrite")
        .parquet(city_summary_path)
    )

    (
        age_summary_df.write
        .mode("overwrite")
        .parquet(age_summary_path)
    )

    print(f"Gold Parquet city summary written to: {city_summary_path}")
    print(f"Gold Parquet age summary written to: {age_summary_path}")

    return {
        "city_summary": city_summary_df,
        "age_summary": age_summary_df,
    }
