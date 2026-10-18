import os
import psycopg


def get_connection():
    return psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "dataflow"),
        user=os.getenv("DB_USER", "dataflow"),
        password=os.getenv("DB_PASSWORD"),
    )


def load_customers(rows):
    query = """
        INSERT INTO customers
        (customer_id, name, email, age, city, age_group)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            age = EXCLUDED.age,
            city = EXCLUDED.city,
            age_group = EXCLUDED.age_group;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)

        conn.commit()

    print(f"Loaded {len(rows)} records into PostgreSQL.")
