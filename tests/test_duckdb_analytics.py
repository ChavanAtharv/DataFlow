import duckdb
import pandas as pd
import pytest

from src.analytics.duckdb_layer import DuckDBAnalytics, resolve_parquet_pattern


@pytest.fixture
def sample_parquet_dir(tmp_path):
    parquet_dir = tmp_path / "silver_customers"
    parquet_dir.mkdir()

    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4],
        "name": ["Aarav", "Neha", "Rohan", "Priya"],
        "email": ["a@ex.com", "n@ex.com", "r@ex.com", "p@ex.com"],
        "age": [20, 28, 32, 45],
        "city": ["Pune", "Mumbai", "Pune", "Delhi"],
        "age_group": ["Young", "Adult", "Senior", "Senior"],
    })

    file_path = parquet_dir / "data.parquet"
    duckdb.query("SELECT * FROM df").write_parquet(str(file_path))
    return parquet_dir


def test_resolve_parquet_pattern_missing():
    with pytest.raises(FileNotFoundError):
        resolve_parquet_pattern("non_existent_dir_dataflow")


def test_resolve_parquet_pattern_dir(sample_parquet_dir):
    pattern = resolve_parquet_pattern(sample_parquet_dir)
    assert pattern.endswith("*.parquet")


def test_city_distribution(sample_parquet_dir):
    analytics = DuckDBAnalytics(sample_parquet_dir)
    df = analytics.city_distribution()

    assert len(df) == 3
    assert set(df["city"]) == {"Pune", "Mumbai", "Delhi"}
    pune_row = df[df["city"] == "Pune"].iloc[0]
    assert pune_row["total_customers"] == 2
    assert pune_row["average_age"] == 26.0


def test_age_group_summary(sample_parquet_dir):
    analytics = DuckDBAnalytics(sample_parquet_dir)
    df = analytics.age_group_summary()

    assert len(df) == 3
    assert set(df["age_group"]) == {"Young", "Adult", "Senior"}
    senior_row = df[df["age_group"] == "Senior"].iloc[0]
    assert senior_row["customer_count"] == 2
    assert senior_row["min_age"] == 32
    assert senior_row["max_age"] == 45


def test_customers_above_average_age(sample_parquet_dir):
    analytics = DuckDBAnalytics(sample_parquet_dir)
    # Average age = (20 + 28 + 32 + 45) / 4 = 31.25
    # Above average: Rohan (32), Priya (45) -> 2 customers
    df = analytics.customers_above_average_age()

    assert len(df) == 2
    assert set(df["customer_id"]) == {3, 4}
    assert list(df["customer_id"]) == [4, 3]  # Ordered DESC by age


def test_rank_customers_by_age(sample_parquet_dir):
    analytics = DuckDBAnalytics(sample_parquet_dir)
    df = analytics.rank_customers_by_age()

    assert len(df) == 4
    assert "city_age_rank" in df.columns
    assert "overall_age_rank" in df.columns
    # In Pune: Rohan (32) is rank 1, Aarav (20) is rank 2
    pune_rohan = df[(df["city"] == "Pune") & (df["customer_id"] == 3)].iloc[0]
    assert pune_rohan["city_age_rank"] == 1


def test_custom_execute(sample_parquet_dir):
    analytics = DuckDBAnalytics(sample_parquet_dir)
    df = analytics.execute("SELECT COUNT(*) AS cnt FROM {{customers}} WHERE age > 25")
    assert df.iloc[0]["cnt"] == 3
