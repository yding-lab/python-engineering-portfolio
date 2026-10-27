import math

import pandas as pd
import pytest

from wb_etl.quality import DataQualityError, run_quality_checks
from wb_etl.transform import DataValidationError, transform_indicator_data


def test_transform_builds_keys_and_yoy(raw_df):
    out = transform_indicator_data(raw_df)
    assert list(out["record_key"][:2]) == ["CAN_NY.GDP.PCAP.CD_2018", "CAN_NY.GDP.PCAP.CD_2019"]
    usa = out[out.country_code == "USA"].set_index("year")
    assert math.isnan(usa.loc[2018, "year_over_year_pct_change"])
    assert usa.loc[2019, "year_over_year_pct_change"] == pytest.approx(10.0)


def test_yoy_is_blank_across_a_gap(raw_df):
    gap = raw_df[~((raw_df.country_code == "USA") & (raw_df.year == "2019"))]
    usa = transform_indicator_data(gap).query("country_code == 'USA'").set_index("year")
    assert math.isnan(usa.loc[2020, "year_over_year_pct_change"])  # 2018 -> 2020 is not year-over-year
    assert usa.loc[2021, "year_over_year_pct_change"] == pytest.approx(10.0)


def test_transform_drops_nulls_and_duplicates(raw_df):
    dirty = pd.concat([raw_df, raw_df.iloc[[0]]]).reset_index(drop=True)
    dirty.loc[1, "value"] = None
    out = transform_indicator_data(dirty)
    assert out["record_key"].is_unique and len(out) == len(raw_df) - 1


def test_transform_rejects_missing_columns(raw_df):
    with pytest.raises(DataValidationError, match="value"):
        transform_indicator_data(raw_df.drop(columns=["value"]))


def test_quality_passes_clean_data_and_flags_problems(raw_df):
    clean = transform_indicator_data(raw_df)
    ok = run_quality_checks(clean, expected_countries={"USA", "CAN"}, minimum_year=2018, maximum_year=2021)
    assert ok.passed and ok.metrics["row_count"] == 8
    with pytest.raises(DataQualityError, match="Missing expected countries"):
        run_quality_checks(clean, expected_countries={"USA", "CAN", "MEX"})
    bad = run_quality_checks(clean, maximum_year=2020, fail_on_error=False)
    assert not bad.passed and "later than 2020" in bad.errors[0]
