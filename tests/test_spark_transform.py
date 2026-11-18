import pytest
from src.transformation.spark_transform import (
    build_gold_parquet_layers,
    transform_data,
)


def test_spark_transform_and_gold_parquet(tmp_path):
    # Create sample input CSV with messy strings and varying ages
    input_csv = tmp_path / "raw_customers.csv"
    input_csv.write_text(
        "customer_id,name,email,age,city\n"
        "1,  Aarav Sharma  , AARAV@EXAMPLE.COM ,22, Pune \n"
        "2,Neha Patil,NEHA@EXAMPLE.COM,27, Mumbai \n"
        "3,Priya Joshi,priya@example.com,45,Delhi\n"
    )

    silver_out = tmp_path / "silver_output"

    spark, transformed_df = transform_data(
        input_path=str(input_csv),
        silver_output_path=str(silver_out),
    )

    try:
        rows = {row["customer_id"]: row.asDict() for row in transformed_df.collect()}

        # Row 1: trimmed name, lower email, Young (<25)
        assert rows[1]["name"] == "Aarav Sharma"
        assert rows[1]["email"] == "aarav@example.com"
        assert rows[1]["city"] == "Pune"
        assert rows[1]["age_group"] == "Young"
        assert "processed_at" in rows[1]

        # Row 2: Adult (25-30)
        assert rows[2]["name"] == "Neha Patil"
        assert rows[2]["email"] == "neha@example.com"
        assert rows[2]["city"] == "Mumbai"
        assert rows[2]["age_group"] == "Adult"

        # Row 3: Senior (>30)
        assert rows[3]["name"] == "Priya Joshi"
        assert rows[3]["email"] == "priya@example.com"
        assert rows[3]["city"] == "Delhi"
        assert rows[3]["age_group"] == "Senior"

        # Check output parquet was persisted
        assert silver_out.exists()
        parquet_files = list(silver_out.glob("*.parquet"))
        assert len(parquet_files) > 0

        # Test Gold Parquet Layer Generation
        gold_base = str(tmp_path / "gold_output")
        gold_dfs = build_gold_parquet_layers(transformed_df, gold_output_base=gold_base)

        assert "city_summary" in gold_dfs
        assert "age_summary" in gold_dfs

        city_parquets = list((tmp_path / "gold_output" / "city_summary").glob("*.parquet"))
        assert len(city_parquets) > 0

        age_parquets = list((tmp_path / "gold_output" / "age_summary").glob("*.parquet"))
        assert len(age_parquets) > 0

    finally:
        spark.stop()
