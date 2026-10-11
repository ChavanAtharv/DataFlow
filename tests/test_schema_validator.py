import pandas as pd

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
