from pathlib import Path
import pandas as pd


class DataQualityError(Exception):
    """Raised when data quality checks exceed failure thresholds."""
    pass


def run_quality_checks(
    df: pd.DataFrame,
    max_null_threshold: int | None = None,
    fail_on_duplicates: bool = False,
    verbose: bool = False,
) -> dict:
    """Run data quality metrics calculation and optional threshold enforcement."""
    null_counts = df.isnull().sum()
    total_nulls = int(null_counts.sum())

    duplicate_count = int(
        df.duplicated(subset=["customer_id"] if "customer_id" in df.columns else None).sum()
    )

    invalid_age_count = 0
    if "age" in df.columns and pd.api.types.is_numeric_dtype(df["age"]):
        invalid_age_count = int(((df["age"] < 0) | (df["age"] > 120)).sum())

    results = {
        "total_records": len(df),
        "null_values": total_nulls,
        "duplicate_records": duplicate_count,
        "invalid_age_records": invalid_age_count,
    }

    if verbose:
        print("Data quality results:")
        for check, value in results.items():
            print(f"  {check}: {value}")

    # Threshold enforcement
    if max_null_threshold is not None and total_nulls > max_null_threshold:
        raise DataQualityError(
            f"Null threshold exceeded: found {total_nulls} nulls, max allowed is {max_null_threshold}"
        )

    if fail_on_duplicates and duplicate_count > 0:
        raise DataQualityError(
            f"Duplicate records found: {duplicate_count} duplicates detected"
        )

    return results


def quarantine_bad_records(
    df: pd.DataFrame,
    quarantine_output_path: str | Path | None = None,
    verbose: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Audit records, split clean rows from corrupt ones, and optionally persist
    quarantined rows with violation reasons.
    """
    if df.empty:
        return df, df, run_quality_checks(df, verbose=verbose)

    quarantine_reasons = []

    # 1. Null check
    null_mask = df.isnull().any(axis=1)

    # 2. Duplicate customer_id check
    if "customer_id" in df.columns:
        dup_mask = df.duplicated(subset=["customer_id"], keep="first")
    else:
        dup_mask = pd.Series(False, index=df.index)

    # 3. Invalid age check
    if "age" in df.columns and pd.api.types.is_numeric_dtype(df["age"]):
        age_mask = (df["age"] < 0) | (df["age"] > 120)
    else:
        age_mask = pd.Series(False, index=df.index)

    # Combine quarantine mask
    bad_mask = null_mask | dup_mask | age_mask

    quarantined_df = df[bad_mask].copy()
    clean_df = df[~bad_mask].copy()

    # Assign reason tags
    if not quarantined_df.empty:
        reasons = []
        for idx in quarantined_df.index:
            r = []
            if null_mask.loc[idx]:
                r.append("NULL_VALUES")
            if dup_mask.loc[idx]:
                r.append("DUPLICATE_ID")
            if age_mask.loc[idx]:
                r.append("INVALID_AGE")
            reasons.append(";".join(r))
        quarantined_df["quarantine_reason"] = reasons

        if quarantine_output_path:
            out_file = Path(quarantine_output_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            quarantined_df.to_csv(out_file, index=False)
            if verbose:
                print(f"Quarantined {len(quarantined_df)} bad records to {out_file}")

    metrics = run_quality_checks(df, verbose=verbose)
    metrics["clean_records"] = len(clean_df)
    metrics["quarantined_records"] = len(quarantined_df)

    return clean_df, quarantined_df, metrics
