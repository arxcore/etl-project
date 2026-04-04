from src.pipeline.parsers.bls.monthly import parse_monthly_bls
from src.providers.bls.model import BLSSeries, BLSResult, BLSRawData


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
                        value="3.7",
                        footnotes=[],
                    ),
                    BLSRawData(
                        year="2024",
                        period="M02",
                        periodName="February",
                        value="3.9",
                        footnotes=[],
                    ),
                ],
            )
        ]
    )

    result = parse_monthly_bls(fake_data)

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
                        value="-",
                        footnotes=[],
                    ),
                    BLSRawData(
                        year="2024",
                        period="M02",
                        periodName="February",
                        value="3.9",
                        footnotes=[],
                    ),
                ],
            )
        ]
    )

    result = parse_monthly_bls(fake_data)

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
                        value="3.7",
                        footnotes=[],
                    ),
                    BLSRawData(
                        year="2024",
                        period="M03",
                        periodName="March",
                        value="4.1",
                        footnotes=[],
                    ),
                ],
            )
        ]
    )

    result = parse_monthly_bls(fake_data)

    assert "2024-01-01" not in result.parse_result
    assert result.parse_result["2024-03-01"] == 4.1
