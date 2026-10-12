from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, col, when


def transform_data(input_path: str):
    spark = (
        SparkSession.builder
        .appName("DataFlowTransformation")
        .master("local[*]")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    transformed_df = (
        df
        .withColumn("name", trim("name"))
        .withColumn("email", lower(trim("email")))
        .withColumn("city", trim("city"))
        .withColumn(
            "age_group",
            when(col("age") < 25, "Young")
            .when(col("age") <= 30, "Adult")
            .otherwise("Senior")
        )
    )

    print(f"Spark transformation completed. Records: {transformed_df.count()}")

    return spark, transformed_df
