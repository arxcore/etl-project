from pipeline.parsers import parse_monthly_bls, parse_monthly_fred, parse_qsa_bea
import logging
from pipeline.routing import BaseParseReturn, BaseFetcherReturn
from providers.bea.model import BEARawRespons
from providers.bls.model import BLSSeries
from providers.fred import FREDRawResponse

logger = logging.getLogger(__name__)


class UnknownFrequencyType(Exception):
    pass


class UnknownApiType(Exception):
    pass


class RoutingParserError(Exception):
    pass


def bls_type(data: BLSSeries, freq: str):
    freq_type = {"monthly": lambda: parse_monthly_bls(data)}
    if freq not in freq_type:
        raise UnknownFrequencyType(f"Unknown Bls type to parsing, freq {freq}")
    results = freq_type[freq]()
    return results


def fred_type(data: FREDRawResponse, freq: str):
    freq_type = {"monthly": lambda: parse_monthly_fred(data)}
    if freq not in freq_type:
        raise UnknownApiType(f"Unnown Type Frequency FRED for {freq}")
    return freq_type[freq]()


def bea_type(data: BEARawRespons, freq: str):
    freq_type = {"QSA": lambda: parse_qsa_bea(data)}
    if freq not in freq_type:
        raise UnknownApiType(f"Unnown Type Frequency BEA for {freq}")
    return freq_type[freq]()


def parser_api_type(
    data: BaseFetcherReturn, api: str, freq: str | None = None
) -> BaseParseReturn:
    """
    Parser, Always return BaseParseReturn Dict[Str, Float]
    """
    if not freq:
        raise ValueError(f"Frequency not found for Api Type: {api}")
    logger.info("=" * 50)
    logger.info("Parsing Data..")
    # logger.info("Accept %s Data", len(data.fetch_result))
    routing_parse = {
        "bls": lambda: bls_type(BLSSeries.model_validate(data.fetch_result), freq),
        "fred": lambda: fred_type(
            FREDRawResponse.model_validate(data.fetch_result), freq
        ),
        "bea": lambda: bea_type(BEARawRespons.model_validate(data.fetch_result), freq),
    }
    try:
        if api not in routing_parse:
            raise UnknownApiType(
                f"Parsing Error, Invalid API Type in RoutingParse, API type: {api}"
            )
        parse = routing_parse[api]()
        if parse:
            logger.info("Parse Data Done...(%s Data)", len(parse.parse_result))
        return parse
    except Exception as e:
        raise RoutingParserError(
            f"Unknown Routing Parser Error For api type {api}-{e} "
        ) from e
