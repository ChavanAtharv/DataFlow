from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from src.load.postgres_jdbc_loader import (
    ensure_schema_exists,
    get_db_url,
    merge_staging_to_customers,
    write_to_staging,
)
from src.analytics.gold_layer import build_gold_layer


def test_get_db_url(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "secret123")
    url = get_db_url()
    assert "jdbc:postgresql://" in url


def test_ensure_schema_exists_missing_file():
    with pytest.raises(FileNotFoundError, match="Schema file not found"):
        ensure_schema_exists("/nonexistent/path/to/schema.sql")


@patch("src.load.postgres_jdbc_loader.psycopg.connect")
def test_ensure_schema_exists_success(mock_connect, tmp_path):
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text("CREATE TABLE test (id INT);", encoding="utf-8")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    ensure_schema_exists(schema_file)

    mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INT);")
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("src.load.postgres_jdbc_loader.psycopg.connect")
def test_ensure_schema_exists_rollback_on_failure(mock_connect, tmp_path):
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text("INVALID SQL;", encoding="utf-8")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = RuntimeError("Syntax error")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    with pytest.raises(RuntimeError, match="Syntax error"):
        ensure_schema_exists(schema_file)

    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()


def test_write_to_staging(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "secret123")

    mock_spark_df = MagicMock()
    mock_spark_df.columns = ["customer_id", "name", "email", "age", "city", "age_group", "extra_col"]

    mock_filtered_df = MagicMock()
    mock_spark_df.select.return_value = mock_filtered_df

    mock_writer = MagicMock()
    mock_filtered_df.write.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    write_to_staging(mock_spark_df)

    mock_spark_df.select.assert_called_once_with("customer_id", "name", "email", "age", "city", "age_group")
    mock_writer.mode.assert_called_once_with("overwrite")
    mock_writer.save.assert_called_once()


@patch("src.load.postgres_jdbc_loader.psycopg.connect")
def test_merge_staging_to_customers_success(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    merge_staging_to_customers()

    assert mock_cursor.execute.call_count == 2
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("src.load.postgres_jdbc_loader.psycopg.connect")
def test_merge_staging_to_customers_rollback_on_error(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = [RuntimeError("Merge failed"), MagicMock()]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    with pytest.raises(RuntimeError, match="Merge failed"):
        merge_staging_to_customers()

    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()


def test_build_gold_layer_missing_file():
    with pytest.raises(FileNotFoundError, match="Gold SQL file not found"):
        build_gold_layer("/nonexistent/gold.sql")


@patch("src.analytics.gold_layer.psycopg.connect")
def test_build_gold_layer_success(mock_connect, tmp_path):
    gold_sql = tmp_path / "gold.sql"
    gold_sql.write_text("CREATE TABLE gold_summary AS SELECT 1;", encoding="utf-8")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    build_gold_layer(gold_sql)

    mock_cursor.execute.assert_called_once_with("CREATE TABLE gold_summary AS SELECT 1;")
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("src.analytics.gold_layer.psycopg.connect")
def test_build_gold_layer_rollback_on_error(mock_connect, tmp_path):
    gold_sql = tmp_path / "gold.sql"
    gold_sql.write_text("CREATE TABLE gold_summary AS SELECT 1;", encoding="utf-8")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = RuntimeError("Table creation failed")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    with pytest.raises(RuntimeError, match="Table creation failed"):
        build_gold_layer(gold_sql)

    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()
