from src.pipeline.parsers.fred.monthly import parse_monthly_fred, ErrorConvertValue
from src.providers.fred import FREDRawResponse, Observation
import pytest


def test_parse_monthly_fred_missing_data():

    fake_data = FREDRawResponse(
        observations=[
            Observation(date="2025-09-01", value="12.9"),
            Observation(date="2025-10-01", value="11.7"),
            Observation(date="2025-11-01", value="-"),
        ]
    )

    result = parse_monthly_fred(fake_data)
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
    with pytest.raises(ErrorConvertValue):
        parse_monthly_fred(bad_data)
