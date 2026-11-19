import pandas as pd
import pytest

from src.pipeline import run_pipeline
from src.quality.data_quality import DataQualityError


def test_pipeline_skip_db_execution(tmp_path):
    csv_file = tmp_path / "customers.csv"
    csv_file.write_text(
        "customer_id,name,email,age,city\n"
        "1,Aarav Sharma,aarav@example.com,24,Pune\n"
        "2,Neha Patil,neha@example.com,28,Mumbai\n"
    )

    quarantine_file = tmp_path / "quarantine.csv"
    silver_out = tmp_path / "silver_test"

    results = run_pipeline(
        input_file=str(csv_file),
        skip_db=True,
        quarantine_path=str(quarantine_file),
        silver_output_path=str(silver_out),
    )

    assert isinstance(results, dict)
    assert results["total_records"] == 2
    assert results["clean_records"] == 2
    assert results["quarantined_records"] == 0


def test_pipeline_with_quarantine_records(tmp_path):
    csv_file = tmp_path / "mixed_customers.csv"
    csv_file.write_text(
        "customer_id,name,email,age,city\n"
        "1,Aarav Sharma,aarav@example.com,24,Pune\n"
        "2,Corrupt Customer,corrupt@example.com,-99,Mumbai\n"
        "3,Neha Patil,neha@example.com,28,Mumbai\n"
    )

    quarantine_file = tmp_path / "quarantine.csv"
    silver_out = tmp_path / "silver_test"

    results = run_pipeline(
        input_file=str(csv_file),
        skip_db=True,
        quarantine_path=str(quarantine_file),
        silver_output_path=str(silver_out),
    )

    assert results["total_records"] == 3
    assert results["clean_records"] == 2
    assert results["quarantined_records"] == 1

    # Check quarantine CSV was created and contains the corrupt row
    assert quarantine_file.exists()
    quarantined = pd.read_csv(quarantine_file)
    assert len(quarantined) == 1
    assert quarantined.iloc[0]["customer_id"] == 2


def test_pipeline_all_records_corrupt_fails(tmp_path):
    csv_file = tmp_path / "all_bad.csv"
    csv_file.write_text(
        "customer_id,name,email,age,city\n"
        "1,Bad Age,bad@example.com,-5,Pune\n"
        "2,Bad Age 2,bad2@example.com,200,Mumbai\n"
    )

    quarantine_file = tmp_path / "quarantine.csv"
    with pytest.raises(DataQualityError, match="All ingested records failed quality checks"):
        run_pipeline(
            input_file=str(csv_file),
            skip_db=True,
            quarantine_path=str(quarantine_file),
        )


def test_pipeline_json_array_execution(tmp_path):
    import json
    json_file = tmp_path / "customers.json"
    json_file.write_text(json.dumps([
        {"customer_id": 1, "name": "Aarav", "email": "aarav@example.com", "age": 24, "city": "Pune"},
        {"customer_id": 2, "name": "Neha", "email": "neha@example.com", "age": 27, "city": "Mumbai"},
    ]))

    quarantine_file = tmp_path / "quarantine.csv"
    silver_out = tmp_path / "silver_json_test"

    results = run_pipeline(
        input_file=str(json_file),
        skip_db=True,
        quarantine_path=str(quarantine_file),
        silver_output_path=str(silver_out),
    )

    assert results["total_records"] == 2
    assert results["clean_records"] == 2
    assert results["quarantined_records"] == 0
    assert (silver_out).exists()

