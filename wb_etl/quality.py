"""Reusable data quality checks for transformed pipeline datasets."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class QualityCheckResult:
    """Result of a complete data quality validation run."""

    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, float | int] = field(default_factory=dict)


class DataQualityError(ValueError):
    """Raised when strict data quality requirements are violated."""


def run_quality_checks(
    df: pd.DataFrame,
    *,
    expected_countries: set[str] | None = None,
    minimum_year: int | None = None,
    maximum_year: int | None = None,
    max_null_rate: float = 0.05,
    fail_on_error: bool = True,
) -> QualityCheckResult:
    """Run multiple business and technical quality checks.

    Checks include:
    - empty dataset detection
    - uniqueness of record_key
    - null rates
    - valid year range
    - country coverage
    - finite numeric values
    - suspicious negative GDP-per-capita values
    """
    errors: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, float | int] = {}

    if df.empty:
        errors.append("Dataset is empty.")
        result = QualityCheckResult(
            passed=False,
            errors=errors,
            warnings=warnings,
            metrics=metrics,
        )
        if fail_on_error:
            raise DataQualityError("; ".join(errors))
        return result

    metrics["row_count"] = len(df)
    metrics["country_count"] = int(df["country_code"].nunique())
    metrics["year_count"] = int(df["year"].nunique())

    duplicate_count = int(df["record_key"].duplicated().sum())
    metrics["duplicate_record_keys"] = duplicate_count
    if duplicate_count:
        errors.append(
            f"Found {duplicate_count} duplicate record_key values."
        )

    core_columns = [
        "record_key",
        "country_code",
        "year",
        "indicator_code",
        "value",
    ]

    for column in core_columns:
        null_rate = float(df[column].isna().mean())
        metrics[f"{column}_null_rate"] = round(null_rate, 6)
        if null_rate > max_null_rate:
            errors.append(
                f"{column} null rate {null_rate:.2%} exceeds "
                f"allowed threshold {max_null_rate:.2%}."
            )

    if minimum_year is not None:
        too_old = int((df["year"] < minimum_year).sum())
        if too_old:
            errors.append(
                f"{too_old} rows have year earlier than {minimum_year}."
            )

    if maximum_year is not None:
        too_new = int((df["year"] > maximum_year).sum())
        if too_new:
            errors.append(
                f"{too_new} rows have year later than {maximum_year}."
            )

    if expected_countries:
        observed = set(df["country_code"].dropna().astype(str))
        missing_countries = expected_countries - observed
        unexpected_countries = observed - expected_countries

        if missing_countries:
            errors.append(
                f"Missing expected countries: {sorted(missing_countries)}"
            )

        if unexpected_countries:
            warnings.append(
                f"Unexpected countries observed: {sorted(unexpected_countries)}"
            )

    numeric_values = pd.to_numeric(df["value"], errors="coerce")
    non_numeric_count = int(numeric_values.isna().sum())
    if non_numeric_count:
        errors.append(
            f"{non_numeric_count} rows contain non-numeric indicator values."
        )

    negative_values = int((numeric_values < 0).sum())
    metrics["negative_value_count"] = negative_values
    if negative_values:
        warnings.append(
            f"{negative_values} negative values detected. "
            "Review whether they are valid for this indicator."
        )

    # Large jumps are not automatically incorrect, but they are useful
    # anomaly signals for downstream review.
    if "year_over_year_pct_change" in df.columns:
        extreme_changes = int(
            (df["year_over_year_pct_change"].abs() > 50).sum()
        )
        metrics["extreme_yoy_change_count"] = extreme_changes
        if extreme_changes:
            warnings.append(
                f"{extreme_changes} observations have absolute YoY "
                "change greater than 50%."
            )

    passed = not errors
    result = QualityCheckResult(
        passed=passed,
        errors=errors,
        warnings=warnings,
        metrics=metrics,
    )

    if errors and fail_on_error:
        raise DataQualityError("; ".join(errors))

    return result
