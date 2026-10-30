"""Validation and transformation logic for the pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {
    "country_code",
    "country_name",
    "year",
    "indicator_code",
    "indicator_name",
    "value",
}


class DataValidationError(ValueError):
    """Raised when incoming data does not meet pipeline requirements."""


def validate_columns(df: pd.DataFrame) -> None:
    """Validate that all required source columns are present."""
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise DataValidationError(
            f"Missing required columns: {', '.join(sorted(missing))}"
        )


@dataclass
class CleaningStats:
    """What the transform removed and why, so a report can't show "passed" after silently
    dropping most of the input. Each input row is counted under exactly one reason, checked
    in this order: missing key, invalid year, missing value, unparseable value, non-finite
    value, duplicate."""

    input_rows: int = 0
    output_rows: int = 0
    dropped_missing_key: int = 0  # country_code, indicator_code or year absent
    dropped_invalid_year: int = 0  # year present but not an integer
    dropped_missing_value: int = 0  # value absent in the source (World Bank: not published yet)
    dropped_unparseable_value: int = 0  # value present but not a number, e.g. "n/a"
    dropped_non_finite_value: int = 0  # value parsed to inf or -inf
    dropped_duplicates: int = 0  # same country, indicator and year; the last occurrence is kept
    yoy_undefined_zero_base: int = 0  # previous year's value was 0, so YoY is left blank

    @property
    def dropped_rows(self) -> int:
        return self.input_rows - self.output_rows

    def to_dict(self) -> dict:
        d = asdict(self)
        d["dropped_rows"] = self.dropped_rows
        n = self.input_rows or 1
        d["ratios"] = {k.removeprefix("dropped_"): round(v / n, 6) for k, v in d.items()
                       if k.startswith("dropped_")}
        return d


def _blank(series: pd.Series) -> pd.Series:
    """True where the source has no value: null/NaN or an empty / whitespace-only string."""
    return series.isna() | series.astype(str).str.strip().eq("")


def clean_indicator_data(df: pd.DataFrame) -> tuple[pd.DataFrame, CleaningStats]:
    """Clean and enrich World Bank indicator data, and account for every dropped row.

    - schema validation
    - year and value conversion; rows are dropped, and counted by reason, when a key is
      missing, the year isn't an integer, or the value is missing, unparseable or non-finite
    - duplicate removal (last occurrence wins)
    - year-over-year % change by country and indicator, defined only when the previous
      observation is year - 1 and its value is non-zero. A zero base would give +/-inf (or
      NaN for 0 -> 0); that change is undefined, so it's left blank and counted
    - composite natural key
    """
    validate_columns(df)
    stats = CleaningStats(input_rows=len(df))
    clean = df.copy()

    missing_key = _blank(clean["country_code"]) | _blank(clean["indicator_code"]) | _blank(clean["year"])
    year = pd.to_numeric(clean["year"], errors="coerce")
    invalid_year = ~missing_key & (year.isna() | ~np.isfinite(year.fillna(0)) | (year.fillna(0) % 1 != 0))
    value_missing = _blank(clean["value"])
    value = pd.to_numeric(clean["value"], errors="coerce")
    rest = ~missing_key & ~invalid_year
    missing_value = rest & value_missing
    unparseable = rest & ~value_missing & value.isna()
    non_finite = rest & ~value_missing & value.notna() & ~np.isfinite(value.fillna(0))

    stats.dropped_missing_key = int(missing_key.sum())
    stats.dropped_invalid_year = int(invalid_year.sum())
    stats.dropped_missing_value = int(missing_value.sum())
    stats.dropped_unparseable_value = int(unparseable.sum())
    stats.dropped_non_finite_value = int(non_finite.sum())

    keep = rest & ~missing_value & ~unparseable & ~non_finite
    clean = clean[keep].copy()
    clean["year"] = year[keep].astype(int)
    clean["value"] = value[keep].astype(float)

    before = len(clean)
    clean = clean.drop_duplicates(
        subset=["country_code", "indicator_code", "year"],
        keep="last",
    )
    stats.dropped_duplicates = before - len(clean)

    clean = clean.sort_values(["country_code", "indicator_code", "year"]).reset_index(drop=True)

    grouped = clean.groupby(["country_code", "indicator_code"])
    previous_value = grouped["value"].shift(1)
    previous_year = grouped["year"].shift(1)
    consecutive = previous_year == clean["year"] - 1
    zero_base = consecutive & (previous_value == 0)
    stats.yoy_undefined_zero_base = int(zero_base.sum())
    yoy = (clean["value"] / previous_value - 1).mul(100)
    clean["year_over_year_pct_change"] = yoy.where(consecutive & ~zero_base)

    clean["record_key"] = (
        clean["country_code"].astype(str)
        + "_"
        + clean["indicator_code"].astype(str)
        + "_"
        + clean["year"].astype(str)
    )

    columns = [
        "record_key",
        "country_code",
        "country_name",
        "year",
        "indicator_code",
        "indicator_name",
        "value",
        "year_over_year_pct_change",
    ]
    if "source_updated_at" in clean.columns:
        columns.append("source_updated_at")
    clean = clean[columns]
    stats.output_rows = len(clean)
    return clean, stats


def transform_indicator_data(df: pd.DataFrame) -> pd.DataFrame:
    """clean_indicator_data without the statistics."""
    return clean_indicator_data(df)[0]
