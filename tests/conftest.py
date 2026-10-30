import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def wb_item(country, year, value, indicator="NY.GDP.PCAP.CD"):
    """One record in the World Bank API's JSON shape. Values are made up for tests."""
    return {"countryiso3code": country, "country": {"value": f"Country {country}"}, "date": str(year),
            "indicator": {"id": indicator, "value": "GDP per capita (current US$)"}, "value": value}


@pytest.fixture()
def raw_df():
    """A small raw extract in the shape fetch_indicator_data returns (test data, not real figures)."""
    rows = []
    for country, base in (("USA", 100.0), ("CAN", 50.0)):
        for i, year in enumerate(range(2018, 2022)):
            rows.append({"country_code": country, "country_name": f"Country {country}", "year": str(year),
                         "indicator_code": "NY.GDP.PCAP.CD", "indicator_name": "GDP per capita (current US$)",
                         "value": base * (1.1 ** i), "source_updated_at": "2026-07-01"})
    return pd.DataFrame(rows)
