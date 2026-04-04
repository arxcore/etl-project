from pipeline.processors.indicator import (
    FinalFormatItems,
    FinalFormatResult,
    IndicatorsProcessors,
)
import logging
from config.metadata import ALL_INDICATORS

logger = logging.getLogger(__name__)


class OrchestratorFailed(Exception):
    pass


def run_all() -> FinalFormatResult:
    """
    Running ALLConfig Data

    Flows:
    1.make cache -> 2.records -> 3.extend records result to data [] -> 4.save cache to json file
    """
    data: list[FinalFormatItems] = []
    processors = IndicatorsProcessors()

    for country, categories in ALL_INDICATORS.items():
        for category, indicators in categories.items():
            for indicators_name, meta in indicators.items():
                # indicator: US_NFP, Unemploy
                # meta: url, id, calc, dst..``
                try:
                    records = processors.process_indicators(
                        indicators_name, meta, category, country
                    )
                    data.extend(records.format_result)
                except Exception as e:
                    logger.error("Failed Processed of Indicators: %s", e, exc_info=True)
                    continue
    logger.info("Orchestrator...done (%s Data)", len(data))
    return FinalFormatResult(format_result=data)


def run_by_single(country: str, name: str) -> FinalFormatResult:
    data: list[FinalFormatItems] = []
    processors = IndicatorsProcessors()

    for category, indicators in ALL_INDICATORS[country].items():
        for indicator_name, meta in indicators.items():
            if indicator_name != name:
                continue
            try:
                records = processors.process_indicators(
                    indicator_name, meta, category, country
                )

                data.extend(records.format_result)
            except Exception as e:
                logger.error("Failed Procesed of Indicators: %s", e)
                logger.error("skipping  Indicators: %s", name)
                continue

    logger.info("Orchestrator...done (%s Data)", len(data))
    return FinalFormatResult(format_result=data)
