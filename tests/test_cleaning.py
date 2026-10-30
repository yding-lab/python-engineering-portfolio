"""Every dropped row is counted by reason; non-finite values never pass the gate."""

import json
import math

import numpy as np
import pandas as pd
import pytest

from wb_etl.pipeline import PipelineConfig, QualityGateError, run_pipeline
from wb_etl.quality import DataQualityError, run_quality_checks
from wb_etl.transform import clean_indicator_data


def with_values(raw_df, values: dict):
    """Copy of raw_df with (country, year) -> value overrides."""
    df = raw_df.copy().astype({"value": object})
    for (country, year), v in values.items():
        df.loc[(df.country_code == country) & (df.year == str(year)), "value"] = v
    return df


def test_each_drop_reason_is_counted_separately(raw_df):
    df = with_values(raw_df, {("USA", 2018): None, ("USA", 2019): "", ("USA", 2020): "n/a",
                              ("CAN", 2018): float("inf"), ("CAN", 2019): "-inf"})
    df = pd.concat([df, df.iloc[[7]]], ignore_index=True)  # duplicate CAN 2021
    df.loc[len(df)] = {**df.iloc[0].to_dict(), "country_code": None}
    df.loc[len(df)] = {**df.iloc[0].to_dict(), "year": "20x9"}
    out, s = clean_indicator_data(df)
    assert s.input_rows == 11 and s.output_rows == len(out) == 3
    assert (s.dropped_missing_value, s.dropped_unparseable_value, s.dropped_non_finite_value) == (2, 1, 2)
    assert (s.dropped_duplicates, s.dropped_missing_key, s.dropped_invalid_year) == (1, 1, 1)
    d = s.to_dict()
    assert d["dropped_rows"] == 8 and d["ratios"]["non_finite_value"] == pytest.approx(2 / 11, abs=1e-6)
    assert np.isfinite(out["value"]).all()


def test_yoy_with_zero_previous_year_is_blank_not_infinite(raw_df):
    df = with_values(raw_df, {("USA", 2019): 0.0})
    out, s = clean_indicator_data(df)
    usa = out[out.country_code == "USA"].set_index("year")["year_over_year_pct_change"]
    assert usa.loc[2019] == pytest.approx(-100.0)  # 110 -> 0 is a defined change
    assert math.isnan(usa.loc[2020])  # 0 -> 121 has no finite % change
    assert s.yoy_undefined_zero_base == 1
    assert not np.isinf(out["year_over_year_pct_change"]).any()

    q = run_quality_checks(out, cleaning=s.to_dict(), fail_on_error=False)
    assert q.passed and any("previous year's value was 0" in w for w in q.warnings)


def test_quality_rejects_non_finite_values_even_without_cleaning_stats(raw_df):
    out, _ = clean_indicator_data(raw_df)
    bad = out.copy()
    bad.loc[0, "value"] = np.inf
    bad.loc[1, "year_over_year_pct_change"] = -np.inf
    q = run_quality_checks(bad, fail_on_error=False)
    assert not q.passed
    assert q.metrics["non_finite_value_count"] == 1 and q.metrics["non_finite_yoy_count"] == 1
    assert any("non-finite (inf/-inf) indicator values" in e for e in q.errors)
    with pytest.raises(DataQualityError, match="non-finite"):
        run_quality_checks(bad)


def test_invalid_source_values_fail_the_gate(raw_df):
    out, s = clean_indicator_data(with_values(raw_df, {("USA", 2020): "inf"}))
    q = run_quality_checks(out, cleaning=s.to_dict(), fail_on_error=False)
    assert not q.passed and "1 non-finite (inf/-inf)" in q.errors[0]
    ok = run_quality_checks(out, cleaning=s.to_dict(), allow_invalid_values=True, fail_on_error=False)
    assert ok.passed


def test_missing_values_are_reported_and_heavy_drops_fail(raw_df):
    out, s = clean_indicator_data(with_values(raw_df, {("USA", 2021): None}))
    q = run_quality_checks(out, cleaning=s.to_dict(), fail_on_error=False)
    assert q.passed and q.metrics["dropped_rows"] == 1 and q.metrics["dropped_rate"] == 0.125
    assert any("missing_value=1" in w for w in q.warnings)  # passed, but the drop is visible

    heavy = with_values(raw_df, {(c, y): None for c in ("USA", "CAN") for y in (2019, 2020, 2021)})
    out, s = clean_indicator_data(heavy)
    q = run_quality_checks(out, cleaning=s.to_dict(), fail_on_error=False)
    assert not q.passed and any("dropped 6 of 8 input rows (75.0%)" in e for e in q.errors)


def test_pipeline_report_records_cleaning_and_stops_on_inf(raw_df, tmp_path):
    raw = tmp_path / "raw.csv"
    with_values(raw_df, {("USA", 2021): None}).to_csv(raw, index=False)
    cfg = PipelineConfig(countries=["USA", "CAN"], start_year=2018, end_year=2021, data_dir=tmp_path / "d")
    res = run_pipeline(cfg, from_raw=raw)
    report = json.loads(cfg.quality_path.read_text())
    assert report["passed"] and report["cleaning"]["input_rows"] == 8
    assert report["cleaning"]["output_rows"] == res.processed_rows == 7
    assert report["cleaning"]["dropped_missing_value"] == 1

    with_values(raw_df, {("CAN", 2019): "inf"}).to_csv(raw, index=False)
    cfg2 = PipelineConfig(countries=["USA", "CAN"], start_year=2018, end_year=2021, data_dir=tmp_path / "e")
    with pytest.raises(QualityGateError):
        run_pipeline(cfg2, from_raw=raw)
    assert not cfg2.processed_path.exists()  # nothing persisted
    assert json.loads(cfg2.quality_path.read_text())["cleaning"]["dropped_non_finite_value"] == 1
