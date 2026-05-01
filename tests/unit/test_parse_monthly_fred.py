from pipeline.parsers.fred.monthly import parse_monthly_fred
from providers.fred import FREDRawResponse, Observation
import pytest
from pipeline.routing import BaseFetcherReturn


def test_parse_monthly_fred_missing_data():

    fake_data = FREDRawResponse(
        observations=[
            Observation(date="2025-09-01", value="12.9"),
            Observation(date="2025-10-01", value="11.7"),
            Observation(date="2025-11-01", value="-"),
        ]
    )
    data = BaseFetcherReturn(fetch_result=fake_data, api_type="fred")

    result = parse_monthly_fred(data)
    assert "2025-11-01" not in result.parse_result
    assert result.parse_result["2025-09-01"] == 12.9
    assert result.parse_result["2025-10-01"] == 11.7


def test_parse_monthly_fred_error_convert_value():
    bad_data = FREDRawResponse(
        observations=[
            Observation(date="2021-01-01", value="#"),
            Observation(date="2025-12-01", value="REVISED"),
            Observation(date="2025-05-01", value="/"),
        ]
    )
    data = BaseFetcherReturn(fetch_result=bad_data, api_type="fred")
    result = parse_monthly_fred(data)
    assert "2021-01-01" not in result.parse_result
    assert "2025-05-01" not in result.parse_result

