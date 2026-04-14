from pipeline.routing import BaseFetcherReturn, BaseParseReturn
import logging
from pipeline.parsers import PARSE_REGISTER

logger = logging.getLogger(__name__)


class ParseProcessors:
    """Handling Parse Data with Diverent Frequency"""

    def __call__(
        self, raw_data: BaseFetcherReturn, api: str, freq: str | None = None
    ) -> BaseParseReturn:
        """Process Parse Data by api Type"""
        logger.info("=" * 50)

        if api not in PARSE_REGISTER:
            raise KeyError(f"api {api} not found in register parse")

        elif freq not in PARSE_REGISTER[api]:
            raise KeyError(f"freq {freq} not found in register parse for api {api}")
        try:
            parsed = PARSE_REGISTER[api][freq]
            return parsed(raw_data)
        except Exception as e:
            # TODO:
            # except Hierarky handling
            raise KeyError(f"api {api} not found in register parse") from e
