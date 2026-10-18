from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, col, when


def transform_data(
    input_path: str,
    silver_output_path: str = "data/processed/silver/customers",
):
    spark = (
        SparkSession.builder
        .appName("DataFlowTransformation")
        .master("local[*]")
        .config("spark.jars", "/usr/share/java/postgresql-jdbc.jar")
        .getOrCreate()
    )

    # Read Bronze data
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # Clean and transform data into the Silver layer
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
    )

    # Persist Silver data in Parquet format
    (
        transformed_df.write
        .mode("overwrite")
        .parquet(silver_output_path)
    )

    print(f"Silver layer written to: {silver_output_path}")

    return spark, transformed_df
