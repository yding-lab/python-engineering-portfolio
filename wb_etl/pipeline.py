"""Main orchestration entry point for the data pipeline.

    python -m wb_etl                                   # extract -> transform -> quality -> CSV
    python -m wb_etl --destination postgres            # ... -> CSV and PostgreSQL (needs DB_* env vars)
    python -m wb_etl --from-raw data/raw/NY.GDP.PCAP.CD_raw.csv   # re-run from a saved extract, offline

Stages: extract (World Bank API) -> save raw CSV -> transform -> quality checks (a failing
check stops the run before anything is persisted) -> processed CSV + quality report ->
optional PostgreSQL upsert. Every stage logs its row count and duration with a run id.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

from .extract import fetch_indicator_data
from .logging_config import configure_logging
from .quality import QualityCheckResult, run_quality_checks
from .transform import clean_indicator_data

PROJECT_ROOT = Path(__file__).resolve().parents[1]

log = logging.getLogger("wb_etl.pipeline")


@dataclass
class PipelineConfig:
    countries: list[str] = field(default_factory=lambda: ["USA", "CAN", "GBR", "DEU", "FRA", "JPN", "AUS"])
    indicator: str = "NY.GDP.PCAP.CD"  # GDP per capita, current US$
    start_year: int = 2015
    end_year: int = 2025
    destination: str = "csv"  # csv | postgres (postgres also writes the CSVs)
    data_dir: Path = PROJECT_ROOT / "data"
    max_null_rate: float = 0.05
    max_dropped_rate: float = 0.25  # share of input rows the transform may drop before the gate fails

    @property
    def raw_path(self) -> Path:
        return self.data_dir / "raw" / f"{self.indicator}_raw.csv"

    @property
    def processed_path(self) -> Path:
        return self.data_dir / "processed" / f"{self.indicator}.csv"

    @property
    def quality_path(self) -> Path:
        return self.data_dir / "processed" / f"{self.indicator}_quality.json"


@dataclass
class PipelineResult:
    run_id: str
    raw_rows: int
    processed_rows: int
    quality: QualityCheckResult
    outputs: dict[str, str]
    load: dict | None = None


@contextmanager
def stage(name: str, run_id: str):
    """Log the start and end of a stage, with its duration and row count."""
    info: dict = {"record_count": None}
    start = time.perf_counter()
    log.info("%s started", name, extra={"pipeline_run_id": run_id, "stage": name})
    try:
        yield info
    except Exception:
        log.exception("%s failed", name, extra={"pipeline_run_id": run_id, "stage": name})
        raise
    log.info("%s finished: %s rows in %.2fs", name, info["record_count"], time.perf_counter() - start,
             extra={"pipeline_run_id": run_id, "stage": name, "record_count": info["record_count"],
                    "duration_seconds": round(time.perf_counter() - start, 3)})


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write atomically: a crash mid-write never leaves a half-written file behind.
    Re-running overwrites the file, so CSV output is idempotent for the same inputs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def run_pipeline(config: PipelineConfig | None = None, from_raw: Path | None = None) -> PipelineResult:
    """Extract, transform, quality-check, and persist one indicator."""
    config = config or PipelineConfig()
    run_id = uuid.uuid4().hex[:12]
    log.info("pipeline run %s: indicator %s, %d countries, %d-%d, destination %s", run_id, config.indicator,
             len(config.countries), config.start_year, config.end_year, config.destination,
             extra={"pipeline_run_id": run_id, "destination": config.destination})
    outputs: dict[str, str] = {}

    with stage("extract", run_id) as s:
        if from_raw:
            raw_df = pd.read_csv(from_raw)
        else:
            raw_df = fetch_indicator_data(config.countries, config.indicator, config.start_year, config.end_year)
            write_csv(raw_df, config.raw_path)
            outputs["raw_csv"] = str(config.raw_path)
        s["record_count"] = len(raw_df)

    with stage("transform", run_id) as s:
        processed_df, cleaning = clean_indicator_data(raw_df)
        s["record_count"] = len(processed_df)
        log.info("cleaning: %d in, %d out, dropped %d (missing value %d, unparseable %d, non-finite %d, "
                 "missing key %d, invalid year %d, duplicate %d)", cleaning.input_rows, cleaning.output_rows,
                 cleaning.dropped_rows, cleaning.dropped_missing_value, cleaning.dropped_unparseable_value,
                 cleaning.dropped_non_finite_value, cleaning.dropped_missing_key, cleaning.dropped_invalid_year,
                 cleaning.dropped_duplicates, extra={"pipeline_run_id": run_id, "stage": "transform"})

    with stage("quality", run_id) as s:
        quality = run_quality_checks(
            processed_df,
            expected_countries=set(config.countries),
            minimum_year=config.start_year,
            maximum_year=config.end_year,
            max_null_rate=config.max_null_rate,
            cleaning=cleaning.to_dict(),
            max_dropped_rate=config.max_dropped_rate,
            fail_on_error=False,
        )
        s["record_count"] = len(processed_df)
        for w in quality.warnings:
            log.warning("quality warning: %s", w, extra={"pipeline_run_id": run_id, "stage": "quality"})
        config.quality_path.parent.mkdir(parents=True, exist_ok=True)
        config.quality_path.write_text(json.dumps(asdict(quality), indent=2))
        outputs["quality_report"] = str(config.quality_path)
        if not quality.passed:
            for e in quality.errors:
                log.error("quality error: %s", e, extra={"pipeline_run_id": run_id, "stage": "quality"})
            raise QualityGateError(f"{len(quality.errors)} quality check(s) failed; nothing was persisted. "
                                   f"See {config.quality_path}")

    with stage("persist_csv", run_id) as s:
        write_csv(processed_df, config.processed_path)
        outputs["processed_csv"] = str(config.processed_path)
        s["record_count"] = len(processed_df)

    load_summary = None
    if config.destination == "postgres":
        from .database import DatabaseConfig, check_database_connection, create_database_engine
        from .load import incremental_load

        with stage("load_postgres", run_id) as s:
            db = DatabaseConfig.from_env()
            log.info("connecting to %s", db.safe_url(), extra={"pipeline_run_id": run_id, "stage": "load_postgres"})
            engine = create_database_engine(db)
            check_database_connection(engine)
            result = incremental_load(processed_df, engine)
            load_summary = {"staged": result.staged_rows, "inserted": result.inserted_rows,
                            "updated": result.updated_rows, "unchanged": result.unchanged_rows}
            log.info("upsert: %(inserted)d inserted, %(updated)d updated, %(unchanged)d unchanged", load_summary,
                     extra={"pipeline_run_id": run_id, "stage": "load_postgres"})
            s["record_count"] = result.staged_rows

    log.info("pipeline run %s complete", run_id, extra={"pipeline_run_id": run_id})
    return PipelineResult(run_id, len(raw_df), len(processed_df), quality, outputs, load_summary)


class QualityGateError(RuntimeError):
    """Raised when quality checks fail; the run stops before persisting anything."""


def main(argv: list[str] | None = None) -> int:
    d = PipelineConfig()
    ap = argparse.ArgumentParser(prog="python -m wb_etl", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--countries", default=",".join(d.countries), help="comma-separated ISO3 codes")
    ap.add_argument("--indicator", default=d.indicator)
    ap.add_argument("--start-year", type=int, default=d.start_year)
    ap.add_argument("--end-year", type=int, default=d.end_year)
    ap.add_argument("--destination", choices=["csv", "postgres"],
                    default=os.getenv("ETL_DESTINATION", d.destination))
    ap.add_argument("--data-dir", type=Path, default=d.data_dir)
    ap.add_argument("--from-raw", type=Path, help="skip the API and transform a saved raw CSV")
    ap.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    ap.add_argument("--log-dir", type=Path, default=Path(os.getenv("LOG_DIR", PROJECT_ROOT / "logs")))
    args = ap.parse_args(argv)

    configure_logging(args.log_level, args.log_dir)
    if args.end_year > date.today().year:
        ap.error("--end-year is in the future")
    cfg = PipelineConfig(countries=[c.strip().upper() for c in args.countries.split(",") if c.strip()],
                         indicator=args.indicator, start_year=args.start_year, end_year=args.end_year,
                         destination=args.destination, data_dir=args.data_dir)
    try:
        result = run_pipeline(cfg, from_raw=args.from_raw)
    except Exception as exc:  # already logged with context by stage(); exit non-zero for schedulers
        log.error("pipeline failed: %s", exc)
        return 1
    print(json.dumps({"run_id": result.run_id, "raw_rows": result.raw_rows,
                      "processed_rows": result.processed_rows, "quality_passed": result.quality.passed,
                      "cleaning": result.quality.cleaning,
                      "warnings": result.quality.warnings, "outputs": result.outputs, "load": result.load},
                     indent=2))
    return 0
