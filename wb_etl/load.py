"""Incremental loading logic for PostgreSQL.

The loader uses a staging table and an UPSERT strategy so repeated pipeline
runs are idempotent: existing business keys are updated and new records are
inserted without creating duplicates.

Idempotency in detail:
- The business key is record_key = <country>_<indicator>_<year> (PRIMARY KEY).
- A key that is new is INSERTed. A key that exists is UPDATEd only if its value,
  country name or YoY change differs (IS DISTINCT FROM, so NULLs compare correctly);
  identical rows are left untouched, keeping their pipeline_loaded_at.
- Re-running with the same source data therefore changes nothing (0 inserted, 0 updated).
  When the World Bank revises a figure, the next run updates exactly that row.
- Staging, merge and staging cleanup run in ONE transaction: a failure rolls back the
  whole load, so the target table is never half-updated.
- Rows that disappear from the source are not deleted (append/update only).
- The staging table is shared, so two loads must not run concurrently.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from .database import ensure_indicator_table


TARGET_TABLE = "economic_indicators"
STAGING_TABLE = "economic_indicators_staging"


@dataclass(frozen=True)
class LoadResult:
    """Summary statistics produced by a database load."""

    staged_rows: int
    inserted_rows: int
    updated_rows: int
    unchanged_rows: int

    @property
    def affected_rows(self) -> int:
        return self.inserted_rows + self.updated_rows


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


def _create_staging_table(connection) -> None:
    statement = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE}
    (LIKE {TARGET_TABLE} INCLUDING DEFAULTS);
    """
    connection.execute(text(statement))


def _write_to_staging(df: pd.DataFrame, connection) -> int:
    load_df = df.copy()
    if "source_updated_at" in load_df.columns:
        # The World Bank's "last updated" date for the indicator, when the extract provided it.
        load_df["source_updated_at"] = pd.to_datetime(load_df["source_updated_at"], errors="coerce")
    else:
        load_df["source_updated_at"] = pd.NaT

    load_df.to_sql(
        STAGING_TABLE,
        connection,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    return len(load_df)


def _merge_staging_into_target(connection) -> tuple[int, int]:
    """Upsert staging into the target. Returns (inserted, updated)."""
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
            IS DISTINCT FROM EXCLUDED.year_over_year_pct_change
    RETURNING (xmax = 0) AS inserted;
    """
    # In PostgreSQL, xmax = 0 on a RETURNING row means it was inserted, not updated.
    flags = [row.inserted for row in connection.execute(text(merge_sql))]
    inserted = sum(flags)
    return inserted, len(flags) - inserted


def _db_message(exc: BaseException) -> str:
    """The database's own error, first line only: no SQL statement, no row values in logs."""
    while exc.__cause__ is not None and not hasattr(exc, "orig"):
        exc = exc.__cause__
    return str(getattr(exc, "orig", exc)).strip().splitlines()[0]


def incremental_load(df: pd.DataFrame, engine: Engine) -> LoadResult:
    """Load transformed records into PostgreSQL using staging + UPSERT, atomically."""
    _validate_load_frame(df)
    ensure_indicator_table(engine)

    try:
        with engine.begin() as connection:  # one transaction: all or nothing
            _create_staging_table(connection)
            connection.execute(text(f"TRUNCATE TABLE {STAGING_TABLE}"))
            staged_rows = _write_to_staging(df, connection)
            inserted, updated = _merge_staging_into_target(connection)
            connection.execute(text(f"TRUNCATE TABLE {STAGING_TABLE}"))
    except (SQLAlchemyError, pd.errors.DatabaseError) as exc:
        raise LoadError(f"Load failed and was rolled back: {_db_message(exc)}") from exc

    return LoadResult(
        staged_rows=staged_rows,
        inserted_rows=inserted,
        updated_rows=updated,
        unchanged_rows=staged_rows - inserted - updated,
    )
