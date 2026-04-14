from providers.metamodel import BaseMetaModel
from pipeline.routing.parser_manage import BaseParseReturn
from pydantic import BaseModel
import logging
from datetime import datetime

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


class StandardizerProcessors:
    @staticmethod
    def process_standardized_data(
        parsed: BaseParseReturn, meta: BaseMetaModel, name: str
    ) -> StandardizedResult:
        result: dict[str, StandardizedItems] = {}
        error: list[str] = []
        try:
            for date_key, float_values in parsed.parse_result.items():
                try:
                    result[date_key] = StandardizedItems(
                        name=name,
                        source=meta.api,
                        date=date_key,
                        value=float_values,
                        frequency=meta.freq,
                        processed=datetime.now().isoformat(),
                    )
                except Exception as e:
                    logger.warning(
                        "Standardized Skipping data for name %s | Error %s",
                        name,
                        e,
                    )
                    error.append(date_key)
                    continue
            if error:
                logger.warning(
                    "Standardized, Total Skipping Error %s ", len(error), error
                )
            if not result:
                raise StandardizedError(
                    f"Standardized result Unknown Error name: {name}, source: {meta.api}"
                )
            logger.info("=" * 50)
            logger.info("Create Standardized For %s", name)
            logger.info(
                "Sample Standardized Data %s",
                list[tuple[str, StandardizedItems]](result.items())[:2],
            )
        except StandardizedError:
            raise
        except Exception as e:
            raise StandardizedError(f"Standardized Proccess Failed {e}") from e
        return StandardizedResult(standardized_data=result)
