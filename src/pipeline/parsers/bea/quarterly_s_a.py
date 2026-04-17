from pipeline.routing import BaseParseReturn
from providers.bea.model import BEARawRespons
import logging
from pipeline.parsers.registry import Frequency, Providers, register
from pipeline.routing.model import BaseFetcherReturn
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


@register(Providers.bea, Frequency.qsa)
def parse_qsa_bea(data: BaseFetcherReturn) -> BaseParseReturn:
    """
    Parse data QuarterlySeasonallyAdjusted
    Return dict[str, float]
    """
    RAW_DATA = BEARawRespons.model_validate(data.fetch_result)

    logger.debug(
        "Parse data BEA QSA, Accept (%s data), Example: %s",
        len(RAW_DATA.BEAAPI.Results.Data),
        RAW_DATA.BEAAPI.Results.Data,
    )
    parse_data: dict[str, float] = {}
    missing_value: list[str] = []
    for item in RAW_DATA.BEAAPI.Results.Data:
        date = item.TimePeriod
        year, quarter = date.split("Q")
        month = int(quarter) * 3
        date_key = f"{year}-{month:02d}-01"
        str_value = item.DataValue
        if str_value in ["-", "N/A", "NA", "", " "]:
            logger.warning("Skipping Parsing data: %s", date)
            missing_value.append(date)
            continue
        try:
            value = float(str_value)
            parse_data[date_key] = value
        except ValueError as e:
            logger.error(f"Parse Error for data: {date} with value: {str_value}-{e}")
            continue
        except exc.BEAParserError:
            logger.exception("Parsing BEA QSA Unknown ERROR")
            raise

    logger.debug(
        "Parse data BEA QSA Done Sampl data  %s",
        (parse_data.items(), len(parse_data)),
    )

    if missing_value:
        logger.warning("Total Missing value for data %s", len(missing_value))
    return BaseParseReturn(parse_result=parse_data)
