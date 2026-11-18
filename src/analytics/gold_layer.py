from pathlib import Path
import psycopg

from src.config import GOLD_SQL_PATH, get_db_connection_params


def build_gold_layer(sql_path: str | Path = GOLD_SQL_PATH):
    sql_file = Path(sql_path)

    if not sql_file.exists():
        raise FileNotFoundError(f"Gold SQL file not found: {sql_file}")

    gold_sql = sql_file.read_text()

    connection = psycopg.connect(**get_db_connection_params())

    try:
        with connection.cursor() as cursor:
            cursor.execute(gold_sql)

        connection.commit()

        print("Gold layer built successfully.")

    finally:
        connection.close()
