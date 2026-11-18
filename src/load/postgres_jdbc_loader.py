from pathlib import Path
import psycopg

from src.config import (
    DB_USER,
    SCHEMA_SQL_PATH,
    get_db_connection_params,
    get_db_password,
    get_jdbc_url,
)


def get_db_url():
    # Retain for backward compatibility
    get_db_password()
    return get_jdbc_url()


def ensure_schema_exists(schema_path: str | Path = SCHEMA_SQL_PATH):
    """Ensure the target tables exist before loading staging data."""
    sql_file = Path(schema_path)
    if not sql_file.exists():
        raise FileNotFoundError(f"Schema file not found: {sql_file}")

    schema_sql = sql_file.read_text(encoding="utf-8")

    connection = psycopg.connect(**get_db_connection_params())
    try:
        with connection.cursor() as cursor:
            cursor.execute(schema_sql)
        connection.commit()
        print("Database schema verified/initialized successfully.")
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def write_to_staging(spark_df):
    """Write DataFrame into PostgreSQL staging table via JDBC."""
    password = get_db_password()

    # Exclude non-database temporary columns like processed_at if not in table
    staging_cols = ["customer_id", "name", "email", "age", "city", "age_group"]
    df_to_load = spark_df.select(*[c for c in staging_cols if c in spark_df.columns])

    (
        df_to_load.write
        .format("jdbc")
        .option("url", get_jdbc_url())
        .option("dbtable", "customers_staging")
        .option("user", DB_USER)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print("Spark JDBC load completed: customers_staging")


def merge_staging_to_customers():
    """Atomically merge staging records into the primary customers table."""
    connection = psycopg.connect(**get_db_connection_params())

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
                    age_group = EXCLUDED.age_group,
                    updated_at = CURRENT_TIMESTAMP;
            """)

            cursor.execute("DROP TABLE IF EXISTS customers_staging;")

        connection.commit()
        print("Staging data merged into customers successfully.")

    except Exception:
        # Guarantee rollback before cleanup to avoid InFailedSqlTransaction
        connection.rollback()
        try:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS customers_staging;")
            connection.commit()
        except Exception:
            pass
        raise

    finally:
        connection.close()
