from providers.fred import FREDRawResponse
from pipeline.routing import BaseParseReturn
import logging
from pipeline.parsers.registry import register, Providers, Frequency
from pipeline.routing.model import BaseFetcherReturn
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


@register(Providers.fred, Frequency.monthly)
def parse_monthly_fred(data: BaseFetcherReturn) -> BaseParseReturn:
    # Validation BaseFetcherReturn
    RAW_DATA = FREDRawResponse.model_validate(data.fetch_result)

    logger.debug("FRED Parsing Accept (%s Data)", len(RAW_DATA.observations))
    result: dict[str, float] = {}

    for entry in RAW_DATA.observations:
        if entry.value in ["-", " ", ".", "NA", "N/A"]:
            logger.warning("invalid format value on parsing for date %s", entry.date)
            continue
        try:
            result[entry.date] = float(entry.value)
        except ValueError as e:
            logger.error(f"canot convert value for date: {entry.date} {e}")
            continue
        except Exception as e:
            raise exc.FREDParserError(f"Parsing FRED Unknown ERROR {e}") from e

    logger.debug("Parsing Done with (%s Data)", len(result))
    return BaseParseReturn(parse_result=result)
