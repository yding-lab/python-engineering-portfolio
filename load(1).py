"""Incremental loading logic for PostgreSQL.

The loader uses a staging table and an UPSERT strategy so repeated pipeline
runs are idempotent: existing business keys are updated and new records are
inserted without creating duplicates.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from database import ensure_indicator_table, transactional_connection


TARGET_TABLE = "economic_indicators"
STAGING_TABLE = "economic_indicators_staging"


@dataclass(frozen=True)
class LoadResult:
    """Summary statistics produced by a database load."""

    staged_rows: int
    affected_rows: int
    deleted_staging_rows: int


class LoadError(RuntimeError):
    """Raised when the warehouse load fails."""


def _validate_load_frame(df: pd.DataFrame) -> None:
    required = {
        "record_key",
        "country_code",
        "country_name",
        "year",
        "indicator_code",
        "indicator_name",
        "value",
        "year_over_year_pct_change",
    }

    missing = required.difference(df.columns)
    if missing:
        raise LoadError(
            f"DataFrame cannot be loaded. Missing columns: {sorted(missing)}"
        )

    if df["record_key"].duplicated().any():
        duplicate_keys = (
            df.loc[df["record_key"].duplicated(keep=False), "record_key"]
            .drop_duplicates()
            .head(10)
            .tolist()
        )
        raise LoadError(
            f"Duplicate record_key values detected before load: {duplicate_keys}"
        )


def _create_staging_table(engine: Engine) -> None:
    statement = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE}
    (LIKE {TARGET_TABLE} INCLUDING DEFAULTS);
    """

    with engine.begin() as connection:
        connection.execute(text(statement))


def _truncate_staging(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {STAGING_TABLE}"))


def _write_to_staging(df: pd.DataFrame, engine: Engine) -> int:
    load_df = df.copy()
    load_df["source_updated_at"] = pd.Timestamp.utcnow().tz_localize(None)

    try:
        load_df.to_sql(
            STAGING_TABLE,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
    except SQLAlchemyError as exc:
        raise LoadError(f"Unable to write staging rows: {exc}") from exc

    return len(load_df)


def _merge_staging_into_target(engine: Engine) -> int:
    merge_sql = f"""
    INSERT INTO {TARGET_TABLE} (
        record_key,
        country_code,
        country_name,
        year,
        indicator_code,
        indicator_name,
        value,
        year_over_year_pct_change,
        source_updated_at
    )
    SELECT
        record_key,
        country_code,
        country_name,
        year,
        indicator_code,
        indicator_name,
        value,
        year_over_year_pct_change,
        source_updated_at
    FROM {STAGING_TABLE}
    ON CONFLICT (record_key)
    DO UPDATE SET
        country_name = EXCLUDED.country_name,
        value = EXCLUDED.value,
        year_over_year_pct_change =
            EXCLUDED.year_over_year_pct_change,
        source_updated_at = EXCLUDED.source_updated_at,
        pipeline_loaded_at = CURRENT_TIMESTAMP
    WHERE
        {TARGET_TABLE}.value IS DISTINCT FROM EXCLUDED.value
        OR {TARGET_TABLE}.country_name
            IS DISTINCT FROM EXCLUDED.country_name
        OR {TARGET_TABLE}.year_over_year_pct_change
            IS DISTINCT FROM EXCLUDED.year_over_year_pct_change;
    """

    try:
        with transactional_connection(engine) as connection:
            result = connection.execute(text(merge_sql))
            return result.rowcount if result.rowcount is not None else 0
    except SQLAlchemyError as exc:
        raise LoadError(f"Incremental merge failed: {exc}") from exc


def incremental_load(df: pd.DataFrame, engine: Engine) -> LoadResult:
    """Load transformed records into PostgreSQL using staging + UPSERT."""
    _validate_load_frame(df)
    ensure_indicator_table(engine)
    _create_staging_table(engine)
    _truncate_staging(engine)

    staged_rows = _write_to_staging(df, engine)
    affected_rows = _merge_staging_into_target(engine)

    with engine.begin() as connection:
        delete_result = connection.execute(
            text(f"DELETE FROM {STAGING_TABLE}")
        )
        deleted_staging_rows = (
            delete_result.rowcount if delete_result.rowcount is not None else 0
        )

    return LoadResult(
        staged_rows=staged_rows,
        affected_rows=affected_rows,
        deleted_staging_rows=deleted_staging_rows,
    )
