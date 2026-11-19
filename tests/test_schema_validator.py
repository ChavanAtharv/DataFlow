import numpy as np
import pandas as pd
import pytest

from src.validation.schema_validator import validate_schema


def test_valid_schema():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Aarav", "Neha"],
        "email": ["aarav@example.com", "neha@example.com"],
        "age": [24, 27],
        "city": ["Pune", "Mumbai"],
    })

    assert validate_schema(df) is True


def test_missing_required_column():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Aarav", "Neha"],
        # 'email' is missing
        "age": [24, 27],
        "city": ["Pune", "Mumbai"],
    })

    with pytest.raises(ValueError, match="Schema validation failed. Missing columns"):
        validate_schema(df)


def test_invalid_customer_id_type():
    df = pd.DataFrame({
        "customer_id": ["one", "two"],
        "name": ["Aarav", "Neha"],
        "email": ["aarav@example.com", "neha@example.com"],
        "age": [24, 27],
        "city": ["Pune", "Mumbai"],
    })

    with pytest.raises(TypeError, match="customer_id must contain integers"):
        validate_schema(df)


def test_invalid_age_type():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Aarav", "Neha"],
        "email": ["aarav@example.com", "neha@example.com"],
        "age": ["twenty-four", "twenty-seven"],
        "city": ["Pune", "Mumbai"],
    })

    with pytest.raises(TypeError, match="age must contain integers"):
        validate_schema(df)


def test_schema_passes_with_null_in_integer_column():
    # When pandas reads integer columns with nulls, it casts to float64 with NaN.
    # Schema validation should pass so Step 3 quarantine can handle the null row.
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Aarav", "Neha"],
        "email": ["aarav@example.com", "neha@example.com"],
        "age": [24.0, np.nan],
        "city": ["Pune", "Mumbai"],
    })

    assert validate_schema(df) is True


def test_schema_passes_with_empty_dataframe_having_correct_columns():
    df = pd.DataFrame(columns=["customer_id", "name", "email", "age", "city"])
    assert validate_schema(df) is True

