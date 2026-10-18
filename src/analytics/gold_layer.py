import os
from pathlib import Path
import psycopg


def build_gold_layer():
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise RuntimeError("DB_PASSWORD environment variable is not set")

    sql_file = Path("sql/gold.sql")

    if not sql_file.exists():
        raise FileNotFoundError(f"Gold SQL file not found: {sql_file}")

    gold_sql = sql_file.read_text()

    connection = psycopg.connect(
        host="localhost",
        port="5432",
        dbname="dataflow",
        user="dataflow",
        password=password,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(gold_sql)

        connection.commit()

        print("Gold layer built successfully.")

    finally:
        connection.close()
