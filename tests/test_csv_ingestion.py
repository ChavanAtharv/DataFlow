import pytest
import pandas as pd
from src.ingestion.csv_ingestion import ingest_csv


def test_ingest_valid_csv(tmp_path):
    csv_file = tmp_path / "test_customers.csv"
    csv_file.write_text(
        "customer_id,name,email,age,city\n"
        "1,Aarav,aarav@test.com,25,Pune\n"
        "2,Neha,neha@test.com,30,Mumbai\n"
    )

    df = ingest_csv(str(csv_file))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["customer_id", "name", "email", "age", "city"]


def test_ingest_missing_file():
    with pytest.raises(FileNotFoundError, match="Input file not found"):
        ingest_csv("data/non_existent_file.csv")


def test_ingest_invalid_file_extension(tmp_path):
    txt_file = tmp_path / "customers.txt"
    txt_file.write_text("dummy,content")

    with pytest.raises(ValueError, match="Only CSV files are supported"):
        ingest_csv(str(txt_file))

