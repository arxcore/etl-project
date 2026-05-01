from pydantic import BaseModel
from pipeline.processors.standardized import StandardizedResult
from pipeline.processors.date import DateProcessors, FilterDatesResult
from pipeline.processors.standardized import StandardizerProcessors
from pipeline.calculation import DataCalculation
from datetime import datetime
import logging
from providers import BaseMetaModel
from pipeline.routing import (
    BaseFetcherReturn,
    BaseParseReturn,
    RawProcessors,
    ParseProcessors,
)
import monitoring.exc_models as exc


logger = logging.getLogger(__name__)


class FinalFormatItems(BaseModel):
    date: str
    year: int
    indicator: str
    country: str
    category: str
    value: float
    frequency: str


class FinalFormatResult(BaseModel):
    format_result: list[FinalFormatItems]


class IndicatorsProcessors:
    """Handling Process indicators"""

    # NOTE:
    # review this class
    def __init__(
        self,
        raw_processors: RawProcessors,
        parse_processors: ParseProcessors,
        standardizer: StandardizerProcessors,
    ):
        self.raw = raw_processors
        self.parse = parse_processors or ParseProcessors()
        self.standardized = standardizer

    async def __aenter__(self):
        await self.raw.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ):
        await self.raw.__aexit__(exc_type, exc_val, exc_tb)

    # ETL Procesed indicators
    async def process_indicators(
        self, name: str, meta: BaseMetaModel, category: str, country: str
    ) -> FinalFormatResult:
        """Main Process ETL indicatros"""

        try:
            # step 1. Raw Data
            # WARN:
            # call without context manager ???
            raw_process: BaseFetcherReturn = await self.raw.process_raw_data(meta)

            # step 2. parse data
            parsed_data: BaseParseReturn = self.parse(raw_process, meta.api, meta.freq)

            # step 3. standarized
            standardizer: StandardizedResult = (
                self.standardized.process_standardized_data(parsed_data, meta, name)
            )

            # step 4. filter dates
            filtered_dates = self.filter_dates(standardizer, name, meta)

            # step 5. calculation
            calculated = self.calculated_values(filtered_dates, name, meta)

            # step 6. final result standarized
            final_result = self.format_result(
                calculated, name, meta.freq, category, country
            )

            return final_result
        except exc.ProcessingFailed:
            logger.exception("Processing Indicator Failed for Name %s", name)
            raise

    def filter_dates(
        self, raw_formatted_data: StandardizedResult, name: str, meta: BaseMetaModel
    ) -> FilterDatesResult:
        """
        Filter Dates
        """
        # build range end years data
        now = datetime.now()
        end_year = now.year
        end_month = now.month

        try:
            filter_date = DateProcessors.date_filter(
                raw_formatted_data,
                start_year=meta.start_year,
                start_month=meta.start_month,
                end_year=end_year,
                end_month=end_month,
            )
        except exc.FilterError:
            logger.exception("Filter Dates Failed for Name %s", name)
            raise

        return filter_date

    def calculated_values(
        self, filter_date: FilterDatesResult, name: str, meta: BaseMetaModel
    ) -> dict[str, float]:
        """Calculation"""
        logger.info("=" * 50)
        logger.debug("Calculation Data..Proccesing")
        logger.debug("Accept (%s Data)", len(filter_date.filtered_date))

        method = str(meta.calc)
        calc_handling = DataCalculation.calculated_router(filter_date, method, name)

        logger.info("Calculation Data... Done")
        logger.info("Accept (%s Data)", len(filter_date.filtered_date))
        logger.debug("Sample Calculation Data %s", calc_handling)
        return calc_handling

    def format_result(
        self,
        data_entry: dict[str, float],
        name: str,
        frequency: str,
        category: str,
        country: str,
    ) -> FinalFormatResult:
        data: list[FinalFormatItems] = []
        logger.info("=" * 50)
        logger.info("Format Result.. Proccesing (%s Data)", len(data_entry))
        try:
            for date_key, value in data_entry.items():
                date_obj = datetime.strptime(date_key, "%Y-%m-%d")
                data.append(
                    FinalFormatItems(
                        date=date_key,
                        year=date_obj.year,
                        indicator=name,
                        country=country,
                        category=category,
                        value=value,
                        frequency=frequency,
                    )
                )
        except exc.FormatError:
            logger.exception("Format Result Failed for Name %s", name)
            raise
        logger.info("Format Result... Done With (%s Data)", len(data))
        logger.debug("Final Result Data, example %s", data[:10])

        return FinalFormatResult(format_result=data)
