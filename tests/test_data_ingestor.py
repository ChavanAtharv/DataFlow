import pandas as pd
import pytest
from src.ingestion.data_ingestor import ingest_data


def test_ingest_data_csv(tmp_path):
    csv_file = tmp_path / "customers.csv"
    csv_file.write_text(
        "customer_id,name,email,age,city\n"
        "1,Aarav,aarav@ex.com,24,Pune\n"
    )
    df = ingest_data(csv_file)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Aarav"


def test_ingest_data_json_array(tmp_path):
    json_file = tmp_path / "customers.json"
    json_file.write_text(
        '[{"customer_id": 1, "name": "Neha", "email": "neha@ex.com", "age": 27, "city": "Mumbai"}]'
    )
    df = ingest_data(json_file)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Neha"


def test_ingest_data_json_lines(tmp_path):
    json_file = tmp_path / "customers.json"
    json_file.write_text(
        '{"customer_id": 1, "name": "Rohan", "email": "rohan@ex.com", "age": 22, "city": "Nashik"}\n'
    )
    df = ingest_data(json_file)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Rohan"


def test_ingest_data_unsupported_format(tmp_path):
    xml_file = tmp_path / "customers.xml"
    xml_file.write_text("<data></data>")
    with pytest.raises(ValueError, match="Unsupported file format"):
        ingest_data(xml_file)


def test_ingest_data_missing_file():
    with pytest.raises(FileNotFoundError):
        ingest_data("non_existent_file.json")


def test_ingest_data_empty_zero_byte_file(tmp_path):
    empty_file = tmp_path / "empty.json"
    empty_file.touch()
    with pytest.raises(ValueError, match="empty"):
        ingest_data(empty_file)


def test_ingest_data_parquet(tmp_path):
    import duckdb
    parquet_file = tmp_path / "customers.parquet"
    duckdb.query("SELECT 1 AS customer_id, 'Kavita' AS name, 'k@ex.com' AS email, 29 AS age, 'Pune' AS city").write_parquet(str(parquet_file))
    df = ingest_data(parquet_file)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Kavita"
    assert df.iloc[0]["customer_id"] == 1
