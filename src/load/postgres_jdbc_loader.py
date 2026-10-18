import os

import psycopg


DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "dataflow"
DB_USER = "dataflow"


def get_db_url():
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise RuntimeError("DB_PASSWORD environment variable is not set")

    return (
        f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def write_to_staging(spark_df):
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise RuntimeError("DB_PASSWORD environment variable is not set")

    (
        spark_df.write
        .format("jdbc")
        .option("url", get_db_url())
        .option("dbtable", "customers_staging")
        .option("user", DB_USER)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print("Spark JDBC load completed: customers_staging")


def merge_staging_to_customers():
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise RuntimeError("DB_PASSWORD environment variable is not set")

    connection = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=password,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO customers (
                    customer_id,
                    name,
                    email,
                    age,
                    city,
                    age_group
                )
                SELECT
                    customer_id,
                    name,
                    email,
                    age,
                    city,
                    age_group
                FROM customers_staging
                ON CONFLICT (customer_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    age = EXCLUDED.age,
                    city = EXCLUDED.city,
                    age_group = EXCLUDED.age_group;
            """)

            cursor.execute("DROP TABLE IF EXISTS customers_staging;")

        connection.commit()

        print("Staging data merged into customers successfully.")

    finally:
        connection.close()
