from providers.fred import FREDRawResponse
from pipeline.routing import BaseParseReturn
import logging

logger = logging.getLogger(__name__)


class ErrorConvertValue(Exception):
    pass


def parse_monthly_fred(data: FREDRawResponse) -> BaseParseReturn:
    logger.debug("FRED Parsing Accept (%s Data)", len(data.observations))
    result: dict[str, float] = {}

    for entry in data.observations:
        if entry.value in ["-", " ", ".", "NA", "N/A"]:
            logger.warning("invalid format value on parsing for date %s", entry.date)
            continue
        try:
            result[entry.date] = float(entry.value)
        except ValueError as e:
            raise ErrorConvertValue(
                f"canot convert value for date: {entry.date}"
            ) from e
    logger.debug("Parsing Done with (%s Data)", len(result))
    return BaseParseReturn(parse_result=result)
