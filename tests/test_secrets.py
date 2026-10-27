import logging

import pytest

from wb_etl.database import DatabaseConfig, DatabaseConfigError
from wb_etl.logging_config import configure_logging, redact


def test_credentials_are_required(monkeypatch):
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    with pytest.raises(DatabaseConfigError, match="DB_USER, DB_PASSWORD"):
        DatabaseConfig.from_env()


def test_password_never_rendered(monkeypatch):
    for var in ("DB_HOST", "DB_PORT", "DB_NAME"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DB_USER", "etl")
    monkeypatch.setenv("DB_PASSWORD", "p@ss:w/rd!")
    cfg = DatabaseConfig.from_env()
    assert "p@ss" not in repr(cfg)
    assert cfg.safe_url() == "postgresql+psycopg2://etl:***@localhost:5432/portfolio"
    assert cfg.sqlalchemy_url.password == "p@ss:w/rd!"  # special characters survive, escaped in the URL


@pytest.mark.parametrize("message", [
    "connect postgresql://etl:hunter22@db:5432/x failed",
    "password=hunter22 rejected",
    "DB_PASSWORD value hunter22 leaked",
])
def test_logs_mask_credentials(message, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("DB_PASSWORD", "hunter22")
    assert "hunter22" not in redact(message)
    configure_logging("INFO", tmp_path)
    logging.getLogger("wb_etl.test").error(message)
    for h in logging.getLogger().handlers:
        h.flush()
    assert "hunter22" not in capsys.readouterr().out
    assert "hunter22" not in (tmp_path / "pipeline.jsonl").read_text()
