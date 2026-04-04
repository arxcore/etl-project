from pipeline.routing import BaseParseReturn
from providers.bea.model import BEARawRespons
import logging

logger = logging.getLogger(__name__)


class ParseError(Exception):
    pass


def parse_qsa_bea(data: BEARawRespons) -> BaseParseReturn:
    """
    Parse data QuarterlySeasonallyAdjusted
    Return dict[str, float]
    """
    logger.debug(
        "Parse data BEA QSA, Accept (%s data), Example: %s",
        len(data.BEAAPI.Results.Data),
        data.BEAAPI.Results.Data,
    )
    parse_data: dict[str, float] = {}
    missing_value: list[str] = []
    for raw_data in data.BEAAPI.Results.Data:
        date = raw_data.TimePeriod
        year, quarter = date.split("Q")
        month = int(quarter) * 3
        date_key = f"{year}-{month:02d}-01"
        str_value = raw_data.DataValue
        if str_value in ["-", "N/A", "NA", "", " "]:
            logger.warning("Skipping Parsing data: %s", date)
            missing_value.append(date)
            continue
        try:
            value = float(str_value)
            parse_data[date_key] = value
        except ValueError as e:
            raise ParseError(
                f"Parse Error for data: {date} with value: {str_value}-{e}"
            ) from e

    logger.debug(
        "Parse data BEA QSA Done with data %s, (%s data)",
        (parse_data.items(), len(parse_data)),
    )

    if missing_value:
        logger.warning("Total Missing value for data %s", len(missing_value))
    return BaseParseReturn(parse_result=parse_data)
