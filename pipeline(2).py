"""Main orchestration entry point for the data pipeline."""

from __future__ import annotations

from pathlib import Path

from extract import fetch_indicator_data
from transform import transform_indicator_data


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

COUNTRIES = ["USA", "CAN", "GBR", "DEU", "FRA", "JPN", "AUS"]
INDICATOR = "NY.GDP.PCAP.CD"
START_YEAR = 2015
END_YEAR = 2025


def ensure_directories() -> None:
    """Create pipeline output directories when they do not already exist."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def run_pipeline() -> None:
    """Extract, transform, and persist the first pipeline dataset."""
    ensure_directories()

    print("Starting extraction...")
    raw_df = fetch_indicator_data(
        countries=COUNTRIES,
        indicator=INDICATOR,
        start_year=START_YEAR,
        end_year=END_YEAR,
    )

    raw_path = RAW_DIR / "gdp_per_capita_raw.csv"
    raw_df.to_csv(raw_path, index=False)
    print(f"Raw data saved to: {raw_path}")

    print("Starting transformation...")
    processed_df = transform_indicator_data(raw_df)

    processed_path = PROCESSED_DIR / "gdp_per_capita.csv"
    processed_df.to_csv(processed_path, index=False)
    print(f"Processed data saved to: {processed_path}")

    print(f"Pipeline complete. {len(processed_df):,} records processed.")


if __name__ == "__main__":
    run_pipeline()
