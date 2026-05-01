from pipeline.parsers import parse_monthly_bls
from providers.bls.model import BLSSeries, BLSResult, BLSRawData, BLSFootnotes
from pipeline.routing import BaseFetcherReturn


def test_parse_monthly_bls_happy_path():
    fake_data = BLSSeries(
        series=[
            BLSResult(
                seriesID="LNS14000000",
                data=[
                    BLSRawData(
                        year="2024",
                        period="M01",
                        periodName="January",
                        latest=None,
                        value="3.7",
                        footnotes=[BLSFootnotes(code=None, text=None)],
                    ),
                    BLSRawData(
                        year="2024",
                        period="M02",
                        periodName="February",
                        latest=None,
                        value="3.9",
                        footnotes=[BLSFootnotes(code=None, text=None)],
                    ),
                ],
            )
        ]
    )
    data = BaseFetcherReturn(fetch_result=fake_data, api_type="bls")

    result = parse_monthly_bls(data)

    assert result.parse_result["2024-01-01"] == 3.7
    assert result.parse_result["2024-02-01"] == 3.9


def test_parse_monthly_bls_skip_invalid_value():
    fake_data = BLSSeries(
        series=[
            BLSResult(
                seriesID="LNS14000000",
                data=[
                    BLSRawData(
                        year="2024",
                        period="M01",
                        periodName="January",
                        latest=None,
                        value="-",
                        footnotes=[BLSFootnotes(code=None, text=None)],
                    ),
                    BLSRawData(
                        year="2024",
                        period="M02",
                        periodName="February",
                        latest=None,
                        value="3.9",
                        footnotes=[BLSFootnotes(code=None, text=None)],
                    ),
                ],
            )
        ]
    )
    data = BaseFetcherReturn(fetch_result=fake_data, api_type="bls")

    result = parse_monthly_bls(data)

    assert "2024-01-01" not in result.parse_result
    assert result.parse_result["2024-02-01"] == 3.9


def test_parse_monthly_bls_skip_non_monthly():
    fake_data = BLSSeries(
        series=[
            BLSResult(
                seriesID="LNS14000000",
                data=[
                    BLSRawData(
                        year="2024",
                        period="Q01",
                        periodName="Quarter",
                        latest=None,
                        value="3.7",
                        footnotes=[BLSFootnotes(code=None, text=None)],
                    ),
                    BLSRawData(
                        year="2024",
                        period="M03",
                        periodName="March",
                        latest=None,
                        value="4.1",
                        footnotes=[],
                    ),
                ],
            )
        ]
    )
    data = BaseFetcherReturn(fetch_result=fake_data, api_type="bls")

    result = parse_monthly_bls(data)

    assert "2024-01-01" not in result.parse_result
    assert result.parse_result["2024-03-01"] == 4.1
