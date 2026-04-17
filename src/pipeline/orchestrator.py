import asyncio
from pipeline.processors.indicator import (
    FinalFormatItems,
    FinalFormatResult,
    IndicatorsProcessors,
)
import logging
from config.metadata import ALL_INDICATORS
from collections.abc import Coroutine
from typing import Any
import monitoring.exc_models as exc


logger = logging.getLogger(__name__)


class Orchest:
    """Orgenaize runing indicaor"""

    def __init__(self, indicator_processor: IndicatorsProcessors):
        self.processors = indicator_processor

    async def run_all(self) -> FinalFormatResult | None:
        """
        Running ALLConfig Data
        """
        # TODO:
        # add Semaphore
        # rate limit handling
        # handling max concurrent

        tasks: list[Coroutine[Any, Any, FinalFormatResult]] = []

        try:
            for country, categories in ALL_INDICATORS.items():
                for category, indicators in categories.items():
                    for indicators_name, meta in indicators.items():
                        # indicator: US_NFP, Unemploy
                        # meta: url, id, calc, dst..``
                        try:
                            tasks.append(
                                self.processors.process_indicators(
                                    indicators_name, meta, category, country
                                )
                            )
                        except exc.ProcessingFailed:
                            logger.exception("Failed to Procesed Indicators")
                            logger.warning("skippping Indicator %s", indicators_name)
                            continue

            results: list[FinalFormatResult | BaseException] = await asyncio.gather(
                *tasks, return_exceptions=True
            )

            data: list[FinalFormatItems] = []

            for result in results:
                if isinstance(result, Exception):
                    logger.error(
                        "Error task, skiping indicator %s", result, exc_info=True
                    )
                    continue
                elif isinstance(result, FinalFormatResult):
                    data.extend(result.format_result)
                    logger.info("Orchestrator...done (%s Data)", len(data))
                    return FinalFormatResult(format_result=data)
        except exc.PipelineCrash:
            logger.exception("Pipeline process carsh during operation")
            raise

    async def run_by_single(self, country: str, name: str) -> FinalFormatResult:
        "Running single process of indicator"

        data: list[FinalFormatItems] = []

        for category, indicators in ALL_INDICATORS[country].items():
            for indicator_name, meta in indicators.items():
                if indicator_name != name:
                    continue
                try:
                    records = await self.processors.process_indicators(
                        indicator_name, meta, category, country
                    )

                    data.extend(records.format_result)
                except exc.ProcessingFailed:
                    logger.exception("Failed to Procesed Indicators")
                    logger.warning("skipping  Indicators: %s", indicator_name)
                    continue

        logger.info("Orchestrator...done (%s Data)", len(data))
        return FinalFormatResult(format_result=data)
