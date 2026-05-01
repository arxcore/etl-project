from pipeline.parsers.bea import parse_qsa_bea
from providers.bea.model import BEARawRespons, BEAapi, BEAField, BEAResult
from pipeline.routing import BaseFetcherReturn


def test_pars_qsa_bea_succs():
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
    data = BaseFetcherReturn(fetch_result=fake_data, api_type="bea")
    result = parse_qsa_bea(data)
    assert result.parse_result["2020-06-01"] == 200187
    assert "2020-09-01" not in result.parse_result
    assert result.parse_result["2020-12-01"] == 200387


def test_pars_qsa_bea_bad():
    fake_data = BEARawRespons(
        BEAAPI=BEAapi(
            Results=BEAResult(
                Data=[
                    BEAField(TimePeriod="2020Q2", DataValue="Comingson"),
                    BEAField(TimePeriod="2020Q3", DataValue="w"),
                    BEAField(TimePeriod="2020Q4", DataValue="  /"),
                ]
            )
        )
    )
    data = BaseFetcherReturn(fetch_result=fake_data, api_type="bea")
    result = parse_qsa_bea(data)
    assert "2020-06-01" not in result.parse_result
    assert "2020-09-01" not in result.parse_result
    assert "2020-12-01" not in result.parse_result


def test_pars_qsa_bea_with_negative_values():
    fake_data = BEARawRespons(
        BEAAPI=BEAapi(
            Results=BEAResult(
                Data=[
                    BEAField(TimePeriod="2021Q1", DataValue="-12345"),
                    BEAField(TimePeriod="2021Q2", DataValue="67890"),
                ]
            )
        )
    )
    data = BaseFetcherReturn(fetch_result=fake_data, api_type="bea")
    result = parse_qsa_bea(data)
    assert result.parse_result["2021-03-01"] == -12345
    assert result.parse_result["2021-06-01"] == 67890
