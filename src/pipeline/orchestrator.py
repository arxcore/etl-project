import asyncio
from pipeline.processors.indicator import (
    StagingItems,
    StagingData,
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
        # load to database here after finish result

    async def __aenter__(self):
        await self.processors.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ):
        await self.processors.__aexit__(exc_type, exc_val, exc_tb)

    async def run_all(self) -> StagingData | None:
        """
        Running ALLConfig Data
        """
        # TODO:
        # DB Traking

        # create task for each indicator and run them concurrently
        tasks: list[Coroutine[Any, Any, StagingData]] = []
        try:
            # Iterate through ALL_INDICATORS and create tasks for each indicator
            for country, categories in ALL_INDICATORS.items():
                for category, indicators in categories.items():
                    for indicators_name, meta in indicators.items():
                        # indicator: US_NFP, Unemploy
                        # meta: url, id, calc, dst..``
                        tasks.append(
                            self.processors.process_indicators(
                                indicators_name, meta, category, country
                            )
                        )
            # Run all tasks concurrently and gather results
            results: list[StagingData | BaseException] = await asyncio.gather(
                *tasks, return_exceptions=True
            )

            data: list[StagingItems] = []

            # Process results, handling exceptions and collecting successful results
            for result in results:
                if isinstance(result, Exception):
                    logger.error(
                        "Error task, skiping indicator %s", result, exc_info=True
                    )
                    continue
                elif isinstance(result, StagingData):
                    data.extend(result.staging_result)

            logger.info("Orchestrator...done (%s Data)", len(data))
            return StagingData(staging_result=data)
        except exc.PipelineCrash:
            logger.exception("Pipeline process carsh during operation")
            raise

    async def run_by_single(self, country: str, name: str) -> StagingData:
        "Running single process of indicator"

        data: list[StagingItems] = []

        for category, indicators in ALL_INDICATORS[country].items():
            for indicator_name, meta in indicators.items():
                if indicator_name != name:
                    continue
                try:
                    records = await self.processors.process_indicators(
                        indicator_name, meta, category, country
                    )

                    data.extend(records.staging_result)
                except exc.ProcessingFailed:
                    logger.exception("Failed to Procesed Indicators")
                    logger.warning("skipping  Indicators: %s", indicator_name)
                    continue

        logger.info("Orchestrator...done (%s Data)", len(data))
        return StagingData(staging_result=data)
