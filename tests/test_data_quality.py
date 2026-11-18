import pandas as pd
import pytest
from src.quality.data_quality import (
    DataQualityError,
    quarantine_bad_records,
    run_quality_checks,
)


def test_clean_data_quality():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Aarav", "Neha"],
        "email": ["aarav@example.com", "neha@example.com"],
        "age": [24, 27],
        "city": ["Pune", "Mumbai"],
    })

    results = run_quality_checks(df)

    assert results["total_records"] == 2
    assert results["null_values"] == 0
    assert results["duplicate_records"] == 0
    assert results["invalid_age_records"] == 0


def test_data_quality_with_nulls():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Aarav", None],
        "email": ["aarav@example.com", "neha@example.com"],
        "age": [24, 27],
        "city": [None, "Mumbai"],
    })

    results = run_quality_checks(df)
    assert results["null_values"] == 2


def test_data_quality_with_duplicates():
    df = pd.DataFrame({
        "customer_id": [1, 1],
        "name": ["Aarav", "Aarav"],
        "email": ["aarav@example.com", "aarav@example.com"],
        "age": [24, 24],
        "city": ["Pune", "Pune"],
    })

    results = run_quality_checks(df)
    assert results["duplicate_records"] == 1


def test_data_quality_with_invalid_ages():
    df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "name": ["Aarav", "Neha", "Rohan"],
        "email": ["a@ex.com", "b@ex.com", "c@ex.com"],
        "age": [-5, 25, 140],
        "city": ["Pune", "Mumbai", "Nashik"],
    })

    results = run_quality_checks(df)
    assert results["invalid_age_records"] == 2


def test_threshold_enforcement_max_nulls():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": [None, None],
        "email": ["a@ex.com", "b@ex.com"],
        "age": [20, 30],
        "city": ["Pune", "Mumbai"],
    })

    with pytest.raises(DataQualityError, match="Null threshold exceeded"):
        run_quality_checks(df, max_null_threshold=1)


def test_threshold_enforcement_fail_on_duplicates():
    df = pd.DataFrame({
        "customer_id": [1, 1],
        "name": ["Aarav", "Aarav"],
        "email": ["a@ex.com", "a@ex.com"],
        "age": [20, 20],
        "city": ["Pune", "Pune"],
    })

    with pytest.raises(DataQualityError, match="Duplicate records found"):
        run_quality_checks(df, fail_on_duplicates=True)


def test_quarantine_bad_records_routing(tmp_path):
    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 3, 4],
        "name": ["Aarav", None, "Rohan", "Rohan Dup", "Aditya"],
        "email": ["a@ex.com", "b@ex.com", "c@ex.com", "c2@ex.com", "d@ex.com"],
        "age": [24, 25, -10, 30, 28],
        "city": ["Pune", "Mumbai", "Nashik", "Pune", "Nagpur"],
    })

    quarantine_file = tmp_path / "quarantined.csv"
    clean_df, quarantined_df, metrics = quarantine_bad_records(
        df, quarantine_output_path=str(quarantine_file)
    )

    # Valid records: ID 1 and ID 4 (2 records)
    assert len(clean_df) == 2
    assert set(clean_df["customer_id"]) == {1, 4}

    # Quarantined: ID 2 (null name), ID 3 (negative age), ID 3 duplicate
    assert len(quarantined_df) == 3
    assert quarantine_file.exists()

    quarantined_saved = pd.read_csv(quarantine_file)
    assert len(quarantined_saved) == 3
    assert "quarantine_reason" in quarantined_saved.columns
