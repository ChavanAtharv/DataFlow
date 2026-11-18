import pandas as pd
from scripts.generate_data import generate_customers


def test_generate_customers(tmp_path):
    output_file = tmp_path / "test_synth.csv"
    df = generate_customers(count=50, dirty_ratio=0.10, output_path=str(output_file))

    assert output_file.exists()
    assert len(df) == 50
    assert set(df.columns) == {"customer_id", "name", "email", "age", "city"}

