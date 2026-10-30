# Python engineering portfolio: World Bank indicator ETL

[![tests](https://github.com/yding-lab/python-engineering-portfolio/actions/workflows/tests.yml/badge.svg)](https://github.com/yding-lab/python-engineering-portfolio/actions/workflows/tests.yml)

A small, production-style ETL pipeline. It extracts an indicator from the World Bank API, cleans it and computes year-over-year change, gates it on data-quality checks, and writes it to CSV (the default) or upserts it idempotently into PostgreSQL. It includes structured logs, no hard-coded credentials, and tests against a real database.

```
World Bank API ──extract──> raw CSV ──transform──> quality gate ──> processed CSV + quality report
                (retries,           (types, nulls,  (fails the run      │
                 pagination)         dedupe, YoY,    before anything     └─(--destination postgres)─>
                                     record_key)     is persisted)            staging + UPSERT, one transaction
```

## Quick start

Requires Python 3.11–3.13 and internet access to `api.worldbank.org`. The CSV run needs no database and no credentials.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

python -m wb_etl                       # GDP per capita, 7 countries, 2015-2025 -> data/
python -m pytest -q                    # unit tests; PostgreSQL tests skip unless DB_* is set
```

Options: `--countries USA,CAN,MEX`, `--indicator SP.POP.TOTL`, `--start-year`, `--end-year`, `--destination {csv,postgres}`, `--data-dir`, `--log-dir`, `--log-level`, and `--from-raw <csv>` to rerun from a saved extract with no network access. The process exits non-zero on any failure, so it can be scheduled.

## Example output

This is a real run from CI (`.github/workflows/tests.yml`, 2026-10-30), on a fresh PostgreSQL database, with the World Bank data as last updated 2026-10-08. Timestamps and start lines are trimmed:

```text
INFO | wb_etl.pipeline | pipeline run ded5f4dae33b: indicator NY.GDP.PCAP.CD, 7 countries, 2015-2025, destination postgres
INFO | wb_etl.pipeline | extract finished: 77 rows in 0.58s
INFO | wb_etl.pipeline | cleaning: 77 in, 77 out, dropped 0 (missing value 0, unparseable 0, non-finite 0, missing key 0, invalid year 0, duplicate 0)
INFO | wb_etl.pipeline | transform finished: 77 rows in 0.01s
INFO | wb_etl.pipeline | quality finished: 77 rows in 0.00s
INFO | wb_etl.pipeline | persist_csv finished: 77 rows in 0.00s
INFO | wb_etl.pipeline | upsert: 77 inserted, 0 updated, 0 unchanged
```

Running the same command again:

```text
INFO | wb_etl.pipeline | upsert: 0 inserted, 0 updated, 77 unchanged
```

`data/processed/NY.GDP.PCAP.CD.csv` (first rows):

```csv
record_key,country_code,country_name,year,indicator_code,indicator_name,value,year_over_year_pct_change,source_updated_at
AUS_NY.GDP.PCAP.CD_2015,AUS,Australia,2015,NY.GDP.PCAP.CD,GDP per capita (current US$),56970.3613028094,,2026-10-08
AUS_NY.GDP.PCAP.CD_2016,AUS,Australia,2016,NY.GDP.PCAP.CD,GDP per capita (current US$),50084.4440606385,-12.086841446503737,2026-10-08
AUS_NY.GDP.PCAP.CD_2017,AUS,Australia,2017,NY.GDP.PCAP.CD,GDP per capita (current US$),54117.547718609,8.052607418557978,2026-10-08
```

`data/processed/NY.GDP.PCAP.CD_quality.json`: `passed: true`. Cleaning took in 77 rows and kept all 77 (0 missing, unparseable, non-finite or duplicate). The output has 7 countries, 11 years, 0 duplicate keys, 0 nulls in core columns, 0 non-finite values, 0 negative values, and 0 year-over-year moves above 50%. All 7 countries had published 2025 values on that date. When a year isn't published yet, those rows show up as `dropped_missing_value` with a warning, rather than disappearing silently.

The figures are World Bank data. Indicator values change when the World Bank revises them.

## Configuration

**CSV is the default destination** and needs no configuration. PostgreSQL is optional.

```bash
cp .env.example .env                  # set DB_USER and DB_PASSWORD (no defaults exist)
set -a && source .env && set +a       # export them into this shell
docker compose --env-file .env up -d  # local PostgreSQL 16, using the same credentials
python -m wb_etl --destination postgres
```

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `DB_USER`, `DB_PASSWORD` | for postgres | none | The pipeline refuses to start without them |
| `DB_HOST`, `DB_PORT`, `DB_NAME` | no | `localhost`, `5432`, `portfolio` | Connection |
| `DB_CONNECT_TIMEOUT` | no | `10` | Seconds |
| `ETL_DESTINATION`, `LOG_LEVEL`, `LOG_DIR` | no | `csv`, `INFO`, `logs/` | Same as the CLI flags |

`.env` is git-ignored. The password is never printed: it's excluded from the config object's `repr`, masked in the connection string that's logged (`postgresql+psycopg2://etl:***@localhost:5432/portfolio`), and removed from log messages and tracebacks by a redaction filter on every log handler. CI checks that the database password doesn't appear anywhere in the logs.

## Idempotent loading

- **Business key:** `record_key = <country>_<indicator>_<year>`, the table's primary key.
- **CSV:** each run writes to a temp file and atomically replaces the output, so a crash never leaves a half-written file and identical inputs produce identical files.
- **PostgreSQL:** rows go into a staging table, then `INSERT ... ON CONFLICT (record_key) DO UPDATE ... WHERE value IS DISTINCT FROM EXCLUDED.value OR ...`:
  - new keys are inserted;
  - existing keys are updated only if the value, country name or YoY change differs, so a World Bank revision updates exactly the revised rows;
  - identical rows aren't touched and keep their `pipeline_loaded_at`;
  - `RETURNING (xmax = 0)` separates inserts from updates for the log line above.
- **Atomicity:** staging, merge and cleanup run in one transaction. If anything fails, such as a constraint violation, the whole load rolls back and the error message names the problem without echoing SQL or row values.
- **Not handled:** rows that disappear from the source aren't deleted, and two loads must not run at the same time, because they share the staging table.

## Cleaning and the data quality gate

**Cleaning is accounted for, row by row** (`clean_indicator_data` in `wb_etl/transform.py`). Every input row that doesn't reach the output is counted under exactly one reason, and the counts and ratios go into the quality report and the log:

| Reason | Meaning |
|---|---|
| `missing_key` | country, indicator or year absent |
| `invalid_year` | year present but not an integer |
| `missing_value` | value absent in the source (null or blank). The World Bank returns null for years it hasn't published yet |
| `unparseable_value` | value present but not a number, e.g. `"n/a"` |
| `non_finite_value` | value parses to `inf` or `-inf` |
| `duplicates` | same country, indicator and year (the last occurrence is kept) |

**Year-over-year change** is `100 × (value / previous − 1)`. It's defined only when the previous observation is exactly the year before and its value isn't zero. A zero base would give ±inf (or NaN for 0 → 0), so that change is left blank and counted as `yoy_undefined_zero_base`, with a warning. A drop *to* zero is a defined −100%.

**The gate** runs after the transform and before anything is persisted (`wb_etl/quality.py`):

- **Errors stop the run:**
  - an empty dataset, duplicate keys, a null rate above 5% in core columns, years outside the requested range, or expected countries missing;
  - non-numeric or non-finite (`inf`/`-inf`) values in the output's `value` or YoY columns;
  - any unparseable or non-finite *source* value. The API returns a number or null, so anything else means a source or parsing problem, not a gap. `allow_invalid_values=True` relaxes this;
  - cleaning that dropped more than 25% of the input rows (`max_dropped_rate`), for any reason.

  The quality report is still written, so you can see why.
- **Warnings are logged** and the run continues: any dropped rows (with counts by reason), blank YoY from a zero base, negative values, and YoY changes above 50%.

So a run that loses most of its input can't report `passed: true` with only the surviving rows in view.

## Logs

The console gets human-readable lines. `logs/pipeline.jsonl` gets one JSON object per event, with `pipeline_run_id`, `stage`, `record_count` and `duration_seconds`, so every stage of every run can be traced and timed. The file rotates at 5 MB.

## Tests

```bash
python -m pytest -q                                          # 26 unit tests, offline
DB_USER=... DB_PASSWORD=... python -m pytest -q              # + 2 PostgreSQL integration tests
```

The tests cover:
- transform: keys, YoY only across consecutive years, null and duplicate handling;
- cleaning: each drop reason counted separately (missing, unparseable, non-finite, duplicate, bad key or year), YoY left blank on a zero base and never infinite;
- quality: the gate passes clean data, rejects `inf`/`-inf` and invalid source values, fails on heavy drops, and reports drops even when it passes;
- extract: pagination, API error payloads, invalid arguments (no network needed);
- the end-to-end pipeline, idempotent reruns, the cleaning statistics in the written report, and that a failed quality gate (for example, an `inf` in the source) persists nothing;
- credentials: required, masked, and never logged;
- against real PostgreSQL: insert, then no-op, then revision, and full rollback on failure.

CI runs everything against a PostgreSQL 16 service, then performs the live World Bank run twice.

## Layout

```
wb_etl/
  extract.py         World Bank API client: retries, pagination, source "last updated" date
  transform.py       validation, typing, dedupe, YoY change, record_key, CleaningStats
  quality.py         quality checks -> QualityCheckResult (errors vs. warnings, metrics)
  load.py            PostgreSQL staging + upsert in one transaction
  database.py        config from env (no default credentials), engine, schema
  logging_config.py  console + JSON-lines logs, credential redaction
  pipeline.py        orchestration and CLI (python -m wb_etl)
tests/               unit + PostgreSQL integration tests
docker-compose.yml   local PostgreSQL for --destination postgres
```
