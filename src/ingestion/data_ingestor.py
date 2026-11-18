from pathlib import Path
import pandas as pd

from src.ingestion.csv_ingestion import ingest_csv


SUPPORTED_EXTENSIONS = {".csv", ".json", ".parquet"}


def ingest_data(file_path: str | Path) -> pd.DataFrame:
    """
    Multi-format ingestion supporting CSV, JSON, and Parquet.
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    ext = path.suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file format '{ext}'. Supported formats: {sorted(SUPPORTED_EXTENSIONS)}"
        )

    if path.stat().st_size == 0:
        raise ValueError(f"Input file '{path}' is empty (0 bytes).")

    if ext == ".csv":
        return ingest_csv(str(path))
    elif ext == ".json":
        try:
            # First try standard json array
            df = pd.read_json(path)
        except (ValueError, pd.errors.EmptyDataError):
            try:
                # Fallback to json lines
                df = pd.read_json(path, lines=True)
            except Exception:
                raise ValueError("Failed to parse JSON file: file is malformed or empty.")
    elif ext == ".parquet":
        df = pd.read_parquet(path)

    print(f"Successfully ingested ({ext}): {path}")
    print(f"Records: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    return df
