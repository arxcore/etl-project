from src.pipeline.parsers.bea import parse_qsa_bea, ParseError
from src.providers.bea.model import BEARawRespons, BEAapi, BEAField, BEAResult
import pytest


def test_pars_qsa_bea():
    fake_data = BEARawRespons(
        BEAAPI=BEAapi(
            Results=BEAResult(
                Data=[
                    BEAField(TimePeriod="2020Q2", DataValue="200187"),
                    BEAField(TimePeriod="2020Q3", DataValue=""),
                    BEAField(TimePeriod="2020Q4", DataValue="200387"),
                ]
            )
        )
    )
    result = parse_qsa_bea(fake_data)
    assert result.parse_result["2020-06-01"] == 200187
    assert "2020-09-01" not in result.parse_result
    assert result.parse_result["2020-12-01"] == 200387


def test_pars_qsa_bea_raise_error():
    bad_data = BEARawRespons(
        BEAAPI=BEAapi(
            Results=BEAResult(
                Data=[
                    BEAField(TimePeriod="2020Q2", DataValue="Comingson"),
                    BEAField(TimePeriod="2020Q3", DataValue="."),
                    BEAField(TimePeriod="2020Q4", DataValue="  /"),
                ]
            )
        )
    )
    with pytest.raises(ParseError):
        parse_qsa_bea(bad_data)
