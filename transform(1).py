"""Validation and transformation logic for the pipeline."""

from __future__ import annotations

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


def transform_indicator_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich World Bank indicator data.

    Transformations include:
    - schema validation
    - year conversion
    - numeric value conversion
    - removal of missing observations
    - duplicate removal
    - sorting
    - year-over-year percentage change by country
    - creation of a composite natural key
    """
    validate_columns(df)

    clean = df.copy()

    clean["year"] = pd.to_numeric(clean["year"], errors="coerce").astype("Int64")
    clean["value"] = pd.to_numeric(clean["value"], errors="coerce")

    clean = clean.dropna(
        subset=["country_code", "year", "indicator_code", "value"]
    ).copy()

    clean["year"] = clean["year"].astype(int)

    clean = clean.drop_duplicates(
        subset=["country_code", "indicator_code", "year"],
        keep="last",
    )

    clean = clean.sort_values(["country_code", "year"]).reset_index(drop=True)

    clean["year_over_year_pct_change"] = (
        clean.groupby(["country_code", "indicator_code"])["value"]
        .pct_change(fill_method=None)
        .mul(100)
    )

    clean["record_key"] = (
        clean["country_code"].astype(str)
        + "_"
        + clean["indicator_code"].astype(str)
        + "_"
        + clean["year"].astype(str)
    )

    return clean[
        [
            "record_key",
            "country_code",
            "country_name",
            "year",
            "indicator_code",
            "indicator_name",
            "value",
            "year_over_year_pct_change",
        ]
    ]
