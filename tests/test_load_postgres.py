"""Integration test against a real PostgreSQL. Runs when DB_USER/DB_PASSWORD point at a
reachable server (CI starts one); skipped otherwise."""

import os

import pandas as pd
import pytest
from sqlalchemy import text

from wb_etl.transform import transform_indicator_data

pytestmark = pytest.mark.skipif(not os.getenv("DB_PASSWORD"), reason="no PostgreSQL configured (DB_* env)")


@pytest.fixture()
def engine():
    from wb_etl.database import DatabaseConfig, create_database_engine

    eng = create_database_engine(DatabaseConfig.from_env())
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS economic_indicators_staging"))
        c.execute(text("DROP TABLE IF EXISTS economic_indicators"))
    yield eng
    eng.dispose()


def count(engine):
    with engine.connect() as c:
        return c.execute(text("SELECT count(*) FROM economic_indicators")).scalar_one()


def test_upsert_is_idempotent_and_applies_revisions(engine, raw_df):
    from wb_etl.load import incremental_load

    df = transform_indicator_data(raw_df)
    first = incremental_load(df, engine)
    assert (first.inserted_rows, first.updated_rows) == (8, 0) and count(engine) == 8

    again = incremental_load(df, engine)
    assert (again.inserted_rows, again.updated_rows, again.unchanged_rows) == (0, 0, 8)

    revised = df.copy()
    revised.loc[revised.record_key == "USA_NY.GDP.PCAP.CD_2020", "value"] = 999.0
    r = incremental_load(revised, engine)
    assert (r.inserted_rows, r.updated_rows, r.unchanged_rows) == (0, 1, 7) and count(engine) == 8
    with engine.connect() as c:
        assert c.execute(text("SELECT value FROM economic_indicators WHERE record_key = 'USA_NY.GDP.PCAP.CD_2020'")).scalar_one() == 999.0


def test_failed_load_rolls_back_entirely(engine, raw_df):
    from wb_etl.load import LoadError, incremental_load

    df = transform_indicator_data(raw_df)
    incremental_load(df, engine)
    bad = df.copy()
    bad.loc[0, "value"] = 1.0  # a valid change ...
    bad.loc[1, "value"] = None  # ... and a NOT NULL violation in the same batch
    with pytest.raises(LoadError, match="rolled back: null value in column") as err:
        incremental_load(bad, engine)
    assert "INSERT" not in str(err.value)  # no SQL or row values in the message
    with engine.connect() as c:
        v = c.execute(text("SELECT value FROM economic_indicators WHERE record_key = :k"),
                      {"k": df.loc[0, "record_key"]}).scalar_one()
    assert v == df.loc[0, "value"]  # the valid change was rolled back too
    assert pd.isna(bad.loc[1, "value"])
