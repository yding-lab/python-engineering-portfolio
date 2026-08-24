"""API extraction utilities for the World Bank data pipeline."""

from __future__ import annotations

from typing import Iterable

import pandas as pd
import requests


WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2"


class ExtractionError(RuntimeError):
    """Raised when data cannot be extracted from the source API."""


def fetch_indicator_data(
    countries: Iterable[str],
    indicator: str,
    start_year: int,
    end_year: int,
    timeout: int = 30,
) -> pd.DataFrame:
    """Fetch annual indicator observations from the World Bank API.

    Parameters
    ----------
    countries:
        Iterable of ISO country codes, such as ``["USA", "CAN"]``.
    indicator:
        World Bank indicator code.
    start_year:
        First year to request.
    end_year:
        Last year to request.
    timeout:
        HTTP request timeout in seconds.

    Returns
    -------
    pandas.DataFrame
        Tidy table containing country, year, indicator, and value.

    Raises
    ------
    ValueError
        If the requested year range is invalid.
    ExtractionError
        If the API request fails or returns an unexpected payload.
    """
    if start_year > end_year:
        raise ValueError("start_year must be less than or equal to end_year")

    country_codes = [code.strip().upper() for code in countries if code.strip()]
    if not country_codes:
        raise ValueError("At least one country code is required")

    country_path = ";".join(country_codes)
    url = f"{WORLD_BANK_BASE_URL}/country/{country_path}/indicator/{indicator}"

    params = {
        "format": "json",
        "date": f"{start_year}:{end_year}",
        "per_page": 20000,
    }

    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise ExtractionError(f"World Bank API request failed: {exc}") from exc

    if not isinstance(payload, list) or len(payload) < 2:
        raise ExtractionError("Unexpected response format from World Bank API")

    records = payload[1]
    if records is None:
        return pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "year",
                "indicator_code",
                "indicator_name",
                "value",
            ]
        )

    rows = []
    for item in records:
        rows.append(
            {
                "country_code": item.get("countryiso3code"),
                "country_name": (item.get("country") or {}).get("value"),
                "year": item.get("date"),
                "indicator_code": (item.get("indicator") or {}).get("id"),
                "indicator_name": (item.get("indicator") or {}).get("value"),
                "value": item.get("value"),
            }
        )

    return pd.DataFrame(rows)
