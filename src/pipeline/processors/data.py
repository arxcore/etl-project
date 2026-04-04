from pydantic import BaseModel
from pipeline.routing.parser_manage import BaseParseReturn, parser_api_type
from datetime import datetime
from pipeline.routing.fetch_manage import RoutingFetcher
import logging
from providers import BaseMetaModel
from pipeline.routing.fetch_manage import BaseFetcherReturn

logger = logging.getLogger(__name__)


class StandardizedItems(BaseModel):
    name: str
    source: str
    date: str
    value: float
    frequency: str
    processed: str


class StandardizedResult(BaseModel):
    standardized_data: dict[str, StandardizedItems]


class StandardizedError(Exception):
    pass


class DataProcessors:
    """
    Data processors
    """

    @staticmethod
    def fetch_and_parse(name: str, meta: BaseMetaModel) -> StandardizedResult:
        """
        Args:
            name: Indicator name
            meta: Metadata Indicators
            category: category indicators
            country: country indicators
        Returns: StandardizedResult
        """

        api_type = meta.api
        api_id = meta.id

        # 1. fetch data
        routing = RoutingFetcher()

        raw_data: BaseFetcherReturn = routing.fetch_api_type(
            api_type,
            api_id,
            meta,
        )

        # 2. Parse Data
        try:
            frequency = meta.freq  # or meta.frequency
            parse_data: BaseParseReturn = parser_api_type(raw_data, api_type, frequency)

            standardized: dict[str, StandardizedItems] = {}
            error: list[str] = []
            for date_key, float_values in parse_data.parse_result.items():
                try:
                    standardized[date_key] = StandardizedItems(
                        name=name,
                        source=api_type,
                        date=date_key,
                        value=float_values,
                        frequency=frequency,
                        processed=datetime.now().isoformat(),
                    )
                except Exception as e:
                    logger.warning(
                        "Standardized Skiping data for name %s | Error %s", name, e
                    )
                    error.append(date_key)
                    continue
            if error:
                logger.warning(
                    "Standardized, Total Skiping Error %s ", len(error), error
                )
            if not standardized:
                raise StandardizedError(
                    f"Standardized result Unknown Error name: {name}, source: {api_type}"
                )
            logger.info("=" * 50)
            logger.info("Create Standardized For %s", name)
            logger.info(
                "Sample Standardized Data %s",
                list[tuple[str, StandardizedItems]](standardized.items())[:2],
            )
        except Exception as e:
            raise StandardizedError(f"Standardized Proccess Failed {e}") from e
        return StandardizedResult(standardized_data=standardized)
