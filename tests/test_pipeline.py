import json

import pytest

from wb_etl import pipeline
from wb_etl.pipeline import PipelineConfig, QualityGateError, run_pipeline


@pytest.fixture()
def cfg(tmp_path):
    return PipelineConfig(countries=["USA", "CAN"], start_year=2018, end_year=2021, data_dir=tmp_path)


def test_end_to_end_csv(cfg, raw_df, monkeypatch):
    monkeypatch.setattr(pipeline, "fetch_indicator_data", lambda *a, **k: raw_df)
    r = run_pipeline(cfg)
    assert (r.raw_rows, r.processed_rows, r.quality.passed) == (8, 8, True)
    assert cfg.raw_path.exists() and cfg.processed_path.exists()
    assert json.loads(cfg.quality_path.read_text())["passed"] is True


def test_rerun_is_idempotent(cfg, raw_df, monkeypatch):
    monkeypatch.setattr(pipeline, "fetch_indicator_data", lambda *a, **k: raw_df)
    run_pipeline(cfg)
    first = cfg.processed_path.read_bytes()
    run_pipeline(cfg)
    assert cfg.processed_path.read_bytes() == first


def test_from_raw_runs_offline(cfg, raw_df, monkeypatch, tmp_path):
    def no_network(*a, **k):
        raise AssertionError("should not call the API")

    monkeypatch.setattr(pipeline, "fetch_indicator_data", no_network)
    raw = tmp_path / "raw.csv"
    raw_df.to_csv(raw, index=False)
    assert run_pipeline(cfg, from_raw=raw).processed_rows == 8


def test_failed_quality_gate_persists_nothing(cfg, raw_df, monkeypatch):
    monkeypatch.setattr(pipeline, "fetch_indicator_data", lambda *a, **k: raw_df[raw_df.country_code == "USA"])
    with pytest.raises(QualityGateError, match="nothing was persisted"):
        run_pipeline(cfg)
    assert not cfg.processed_path.exists()
    assert "Missing expected countries" in json.loads(cfg.quality_path.read_text())["errors"][0]


def test_cli_exit_codes(tmp_path, raw_df):
    raw = tmp_path / "raw.csv"
    raw_df.to_csv(raw, index=False)
    common = ["--from-raw", str(raw), "--start-year", "2018", "--end-year", "2021",
              "--data-dir", str(tmp_path / "data"), "--log-dir", str(tmp_path / "logs")]
    assert pipeline.main(["--countries", "USA,CAN", *common]) == 0
    assert pipeline.main(["--countries", "USA,CAN,MEX", *common]) == 1
