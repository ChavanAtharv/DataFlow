import psycopg

from src.config import get_db_connection_params


def get_connection():
    return psycopg.connect(**get_db_connection_params())


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
            age_group = EXCLUDED.age_group,
            updated_at = CURRENT_TIMESTAMP;
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)

        conn.commit()

    print(f"Loaded {len(rows)} records into PostgreSQL.")
