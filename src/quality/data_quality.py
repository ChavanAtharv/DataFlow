import pandas as pd


def run_quality_checks(df: pd.DataFrame) -> dict:
    null_counts = df.isnull().sum()
    total_nulls = int(null_counts.sum())

    duplicate_count = int(df.duplicated().sum())

    invalid_age_count = int(
        ((df["age"] < 0) | (df["age"] > 120)).sum()
    )

    results = {
        "total_records": len(df),
        "null_values": total_nulls,
        "duplicate_records": duplicate_count,
        "invalid_age_records": invalid_age_count,
    }

    print("Data quality results:")
    for check, value in results.items():
        print(f"  {check}: {value}")

    return results
