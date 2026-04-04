from pydantic import BaseModel
from pipeline.processors.data import DataProcessors, StandardizedResult
from pipeline.processors.date import DateProcessors, FilterDatesResult
from pipeline.calculation import DataCalculation
from datetime import datetime
import logging
from providers import BaseMetaModel


logger = logging.getLogger(__name__)


class ProcessIndicatorsError(Exception):
    pass


class FilterError(Exception):
    pass


class CalculateError(Exception):
    pass


class FinalResultError(Exception):
    pass


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

    # ETL Procesed indicators
    def process_indicators(
        self, name: str, meta: BaseMetaModel, category: str, country: str
    ) -> FinalFormatResult:
        """Main Process ETL indicatros"""

        try:
            # step 1. Raw Data
            raw_formatted_data = DataProcessors.fetch_and_parse(name, meta)

            # step 2. filter dates
            filtered_dates = self.filter_dates(raw_formatted_data, name, meta)

            # step 3. calculation
            calculated = self.calculated_values(filtered_dates, name, meta)

            # step 4. final result standarized
            frequency = meta.freq  # meta["freq"] or meta["frequency"]
            final_result = self.format_result(
                calculated, name, frequency, category, country
            )

            return final_result
        except (FilterError, CalculateError, FinalResultError):
            raise
        except Exception as e:
            raise ProcessIndicatorsError(
                f"Process Inidicators Error Unknown, Name: {name}-{e}"
            ) from e

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
            filter_date = DateProcessors.universal_date_filter(
                raw_formatted_data,
                start_year=meta.start_year,
                start_month=meta.start_month,
                end_year=end_year,
                end_month=end_month,
            )
        except Exception as e:
            raise FilterError(f"Filter Dates Unknown Error Name: {name}-{e}") from e

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

        if not frequency or frequency == "frequency":
            raise ValueError(f"Unknown Type Frequency in Final Result for name: {name}")

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

        logger.info("Format Result... Done With (%s Data)", len(data))
        logger.debug("Final Result Data, example %s", data[:10])
        return FinalFormatResult(format_result=data)
