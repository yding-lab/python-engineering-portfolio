"""API extraction utilities for the World Bank data pipeline."""

from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2"
COLUMNS = ["country_code", "country_name", "year", "indicator_code", "indicator_name", "value",
           "source_updated_at"]

log = logging.getLogger(__name__)


def make_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """HTTP session that retries transient failures (429 and 5xx) with exponential backoff."""
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


class ExtractionError(RuntimeError):
    """Raised when data cannot be extracted from the source API."""


def fetch_indicator_data(
    countries: Iterable[str],
    indicator: str,
    start_year: int,
    end_year: int,
    timeout: int = 30,
    session: requests.Session | None = None,
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
        Tidy table containing country, year, indicator, value, and the API's
        "last updated" date for the indicator (source_updated_at).

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

    session = session or make_session()
    rows = []
    page, pages = 1, 1
    while page <= pages:  # the API paginates; follow every page
        params = {
            "format": "json",
            "date": f"{start_year}:{end_year}",
            "per_page": 20000,
            "page": page,
        }
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise ExtractionError(f"World Bank API request failed: {exc}") from exc

        if not isinstance(payload, list) or len(payload) < 2:
            # The API reports errors as [{"message": [...]}] with HTTP 200.
            raise ExtractionError(f"Unexpected response format from World Bank API: {str(payload)[:300]}")

        meta, records = payload[0] or {}, payload[1]
        pages = int(meta.get("pages") or 1)
        updated = meta.get("lastupdated")
        for item in records or []:
            rows.append(
                {
                    "country_code": item.get("countryiso3code"),
                    "country_name": (item.get("country") or {}).get("value"),
                    "year": item.get("date"),
                    "indicator_code": (item.get("indicator") or {}).get("id"),
                    "indicator_name": (item.get("indicator") or {}).get("value"),
                    "value": item.get("value"),
                    "source_updated_at": updated,
                }
            )
        log.debug("fetched page %d of %d (%d rows so far)", page, pages, len(rows))
        page += 1

    return pd.DataFrame(rows, columns=COLUMNS)
