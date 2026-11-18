import os
from pathlib import Path


def load_env_file(env_path: Path | str = ".env") -> None:
    """Load key-value pairs from a .env file into os.environ if not already set."""
    path = Path(env_path)
    if not path.is_file():
        return

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = val


# Automatically load .env on import if present
load_env_file()

# Base project paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", str(BASE_DIR / "data/raw/customers.csv"))
QUARANTINE_DATA_PATH = os.getenv(
    "QUARANTINE_DATA_PATH", str(BASE_DIR / "data/quarantine/corrupt_customers.csv")
)
SILVER_OUTPUT_PATH = os.getenv(
    "SILVER_OUTPUT_PATH", str(BASE_DIR / "data/processed/silver/customers")
)
GOLD_PARQUET_BASE_PATH = os.getenv(
    "GOLD_PARQUET_BASE_PATH", str(BASE_DIR / "data/processed/gold")
)
SCHEMA_SQL_PATH = os.getenv("SCHEMA_SQL_PATH", str(BASE_DIR / "sql/schema.sql"))
GOLD_SQL_PATH = os.getenv("GOLD_SQL_PATH", str(BASE_DIR / "sql/gold.sql"))
ANALYTICS_SQL_PATH = os.getenv(
    "ANALYTICS_SQL_PATH", str(BASE_DIR / "sql/analytics.sql")
)
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", str(BASE_DIR / "logs/dataflow.log"))

# Database settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "dataflow")
DB_USER = os.getenv("DB_USER", "dataflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Spark settings
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "DataFlowTransformation")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_JDBC_JAR = os.getenv("SPARK_JDBC_JAR", "/usr/share/java/postgresql-jdbc.jar")


def get_db_password() -> str:
    """Return DB_PASSWORD or raise RuntimeError if unset."""
    password = os.getenv("DB_PASSWORD", DB_PASSWORD)
    if not password:
        raise RuntimeError("DB_PASSWORD environment variable is not set")
    return password


def get_jdbc_url() -> str:
    """Construct JDBC PostgreSQL connection URL."""
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_db_connection_params() -> dict:
    """Return dictionary of parameters for psycopg connection."""
    return {
        "host": DB_HOST,
        "port": int(DB_PORT),
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": get_db_password(),
    }
