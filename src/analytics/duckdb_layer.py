import argparse
from pathlib import Path
import duckdb
import pandas as pd

from src.config import SILVER_OUTPUT_PATH


def resolve_parquet_pattern(parquet_path: str | Path = SILVER_OUTPUT_PATH) -> str:
    """
    Resolve Parquet path into a DuckDB-compatible read_parquet pattern.
    Handles single files, directories, partitioned directories, and wildcards.
    """
    p = Path(parquet_path)
    if not p.exists():
        raise FileNotFoundError(f"Parquet path does not exist: {p}")

    if p.is_file():
        return str(p)
    elif p.is_dir():
        # Check if directory has parquets directly or in partition subdirectories
        has_subdirs = any(item.is_dir() for item in p.iterdir())
        if has_subdirs:
            return f"{str(p)}/**/*.parquet"
        return f"{str(p)}/*.parquet"
    return str(parquet_path)


class DuckDBAnalytics:
    """
    In-process columnar OLAP analytics runner querying Lakehouse Parquet directly.
    Exposes a registered 'customers' view over Parquet files.
    """

    def __init__(self, parquet_path: str | Path = SILVER_OUTPUT_PATH):
        self.parquet_path = parquet_path
        self.source_pattern = resolve_parquet_pattern(parquet_path)
        self.conn = duckdb.connect(database=":memory:")
        # Register virtual view over Parquet dataset
        self.conn.execute(
            f"CREATE OR REPLACE VIEW customers AS SELECT * FROM read_parquet('{self.source_pattern}')"
        )

    def execute(self, sql_query: str) -> pd.DataFrame:
        """
        Execute arbitrary SQL against the Parquet dataset view 'customers'.
        """
        clean_query = sql_query.replace("{{customers}}", "customers")
        return self.conn.query(clean_query).df()

    def city_distribution(self) -> pd.DataFrame:
        """Aggregate total customers and average age by city."""
        sql = """
            SELECT 
                city, 
                COUNT(*) AS total_customers,
                ROUND(AVG(age), 1) AS average_age
            FROM customers
            GROUP BY city
            ORDER BY total_customers DESC, city ASC
        """
        return self.conn.query(sql).df()

    def age_group_summary(self) -> pd.DataFrame:
        """Aggregate customer count, minimum, maximum, and average age by age_group."""
        sql = """
            SELECT 
                age_group,
                COUNT(*) AS customer_count,
                MIN(age) AS min_age,
                MAX(age) AS max_age,
                ROUND(AVG(age), 1) AS avg_age
            FROM customers
            GROUP BY age_group
            ORDER BY customer_count DESC
        """
        return self.conn.query(sql).df()

    def customers_above_average_age(self) -> pd.DataFrame:
        """CTE returning customers whose age is above the overall cohort average."""
        sql = """
            WITH cohort_avg AS (
                SELECT AVG(age) AS overall_avg
                FROM customers
            )
            SELECT 
                c.customer_id, 
                c.name, 
                c.age, 
                c.city, 
                c.age_group,
                ROUND(c.age - ca.overall_avg, 1) AS years_above_avg
            FROM customers c
            CROSS JOIN cohort_avg ca
            WHERE c.age > ca.overall_avg
            ORDER BY c.age DESC
        """
        return self.conn.query(sql).df()

    def rank_customers_by_age(self) -> pd.DataFrame:
        """Window function ranking customers within their city by age."""
        sql = """
            SELECT 
                customer_id,
                name,
                city,
                age,
                age_group,
                RANK() OVER (PARTITION BY city ORDER BY age DESC) AS city_age_rank,
                DENSE_RANK() OVER (ORDER BY age DESC) AS overall_age_rank
            FROM customers
            ORDER BY city ASC, city_age_rank ASC
        """
        return self.conn.query(sql).df()

    def generate_all_reports(self) -> dict[str, pd.DataFrame]:
        """Generate full suite of Lakehouse analytical reports."""
        return {
            "city_distribution": self.city_distribution(),
            "age_group_summary": self.age_group_summary(),
            "above_average_age": self.customers_above_average_age(),
            "ranked_customers": self.rank_customers_by_age(),
        }


def parse_args():
    parser = argparse.ArgumentParser(
        description="DataFlow In-Process DuckDB Parquet Analytics"
    )
    parser.add_argument(
        "--parquet-path",
        "-p",
        default=SILVER_OUTPUT_PATH,
        help=f"Path to Silver Parquet files (default: {SILVER_OUTPUT_PATH})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    analytics = DuckDBAnalytics(args.parquet_path)
    reports = analytics.generate_all_reports()

    print("\n" + "=" * 60)
    print(" 🦆 DataFlow DuckDB Analytics Summary")
    print("=" * 60)

    for name, df in reports.items():
        print(f"\n📊 Report: {name.upper()}")
        print("-" * 50)
        print(df.to_string(index=False))
