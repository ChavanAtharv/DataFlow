import pandas as pd


REQUIRED_COLUMNS = {
    "customer_id",
    "name",
    "email",
    "age",
    "city",
}


def validate_schema(df: pd.DataFrame) -> bool:
    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Schema validation failed. Missing columns: {sorted(missing_columns)}"
        )

    if not pd.api.types.is_integer_dtype(df["customer_id"]):
        raise TypeError("customer_id must contain integers.")

    if not pd.api.types.is_integer_dtype(df["age"]):
        raise TypeError("age must contain integers.")

    print("Schema validation passed.")
    return True
