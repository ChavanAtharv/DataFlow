from pathlib import Path
import pandas as pd


def ingest_csv(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() != ".csv":
        raise ValueError("Only CSV files are supported.")

    if path.stat().st_size == 0:
        raise ValueError("Input file is empty (0 bytes).")

    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        raise ValueError("Input file contains no readable columns or data.")

    print(f"Successfully ingested: {path}")
    print(f"Records: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    return df


if __name__ == "__main__":
    ingest_csv("data/raw/customers.csv")
