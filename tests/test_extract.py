import pytest

from conftest import wb_item
from wb_etl.extract import ExtractionError, fetch_indicator_data


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, pages):
        self.pages, self.calls = pages, []

    def get(self, url, params, timeout):
        self.calls.append(params)
        return FakeResponse(self.pages[params["page"] - 1])


def test_follows_pagination_and_records_source_date():
    meta = {"pages": 2, "lastupdated": "2026-07-01"}
    s = FakeSession([[meta, [wb_item("USA", 2020, 1.0)]], [meta, [wb_item("CAN", 2020, 2.0)]]])
    df = fetch_indicator_data(["usa", "can"], "NY.GDP.PCAP.CD", 2020, 2020, session=s)
    assert len(s.calls) == 2 and list(df.country_code) == ["USA", "CAN"]
    assert set(df.source_updated_at) == {"2026-07-01"}


def test_api_error_payload_is_an_extraction_error():
    s = FakeSession([[{"message": [{"id": "120", "value": "Invalid value"}]}]])
    with pytest.raises(ExtractionError, match="Unexpected response"):
        fetch_indicator_data(["USA"], "BAD.CODE", 2020, 2021, session=s)


def test_empty_result_has_the_expected_columns():
    df = fetch_indicator_data(["USA"], "NY.GDP.PCAP.CD", 2020, 2021, session=FakeSession([[{"pages": 1}, None]]))
    assert df.empty and "value" in df.columns


@pytest.mark.parametrize("countries, start, end", [([], 2020, 2021), (["USA"], 2022, 2021)])
def test_invalid_arguments(countries, start, end):
    with pytest.raises(ValueError):
        fetch_indicator_data(countries, "NY.GDP.PCAP.CD", start, end, session=FakeSession([]))
