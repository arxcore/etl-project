from pipeline.routing import BaseFetcherReturn, BaseParseReturn
import logging
from pipeline.parsers.registry import PARSE_REGISTER
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


class ParseProcessors:
    """Handling Parse Data with Diverent Frequency"""

    def __call__(
        self, raw_data: BaseFetcherReturn, api: str, freq: str | None = None
    ) -> BaseParseReturn:
        """Process Parse Data by api Type"""
        logger.info("=" * 50)

        try:
            if api not in PARSE_REGISTER:
                raise exc.RoutingError(f"api {api} not found in register parse")

            elif freq not in PARSE_REGISTER[api]:
                raise exc.RoutingError(
                    f"freq {freq} not found in register parse for api {api}"
                )
            parsed = PARSE_REGISTER[api][freq]
            return parsed(raw_data)
        except exc.RoutingError:
            logger.exception("Routing failed for api %s freq %s", api, freq)
            raise
