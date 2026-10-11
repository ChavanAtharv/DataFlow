from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, col, when


def transform_data(input_path: str, output_path: str):
    spark = (
        SparkSession.builder
        .appName("DataFlowTransformation")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

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

    transformed_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    print(f"Spark transformation completed. Records: {transformed_df.count()}")

    transformed_df.show()

    spark.stop()


if __name__ == "__main__":
    transform_data(
        "data/raw/customers.csv",
        "data/processed/customers"
    )
