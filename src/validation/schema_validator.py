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

    # Validate customer_id: allow pure integers or float with nulls (converted by pandas when nulls exist)
    col_cid = df["customer_id"]
    if not (
        pd.api.types.is_integer_dtype(col_cid)
        or (pd.api.types.is_float_dtype(col_cid) and col_cid.isnull().any())
    ):
        raise TypeError("customer_id must contain integers.")

    # Validate age: allow pure integers or float with nulls (converted by pandas when nulls exist)
    col_age = df["age"]
    if not (
        pd.api.types.is_integer_dtype(col_age)
        or (pd.api.types.is_float_dtype(col_age) and col_age.isnull().any())
    ):
        raise TypeError("age must contain integers.")

    print("Schema validation passed.")
    return True
