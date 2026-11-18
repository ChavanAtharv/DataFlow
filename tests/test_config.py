import os
import pytest
from src.config import (
    DB_HOST,
    DB_NAME,
    DB_PORT,
    DB_USER,
    get_db_password,
    get_jdbc_url,
    load_env_file,
)


def test_default_config_values():
    assert DB_HOST is not None
    assert DB_PORT is not None
    assert DB_NAME == "dataflow"
    assert DB_USER == "dataflow"


def test_get_jdbc_url():
    url = get_jdbc_url()
    assert url.startswith("jdbc:postgresql://")
    assert DB_NAME in url


def test_get_db_password_raises_when_unset(monkeypatch):
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.setattr("src.config.DB_PASSWORD", "")
    with pytest.raises(RuntimeError, match="DB_PASSWORD environment variable is not set"):
        get_db_password()


def test_get_db_password_returns_value(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "secret123")
    assert get_db_password() == "secret123"


def test_load_env_file_missing_file_noop(tmp_path):
    missing_file = tmp_path / "does_not_exist.env"
    # Should not raise
    load_env_file(missing_file)


def test_load_env_file_populates_os_environ(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("TEST_KEY_DATAFLOW=super_secret\n# Comment\nINVALID_LINE\n")
    monkeypatch.delenv("TEST_KEY_DATAFLOW", raising=False)
    load_env_file(env_file)
    assert os.getenv("TEST_KEY_DATAFLOW") == "super_secret"

