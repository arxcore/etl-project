from datetime import datetime
import re
import logging
from pydantic import BaseModel
from pipeline.processors.standardized import StandardizedResult

logger = logging.getLogger(__name__)


class ValidIsoFormatError(Exception):
    pass


class FilterDatesResult(BaseModel):
    filtered_date: dict[str, float]


ISO_DATA = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class DateProcessors:
    @staticmethod
    def check_if_iso_format(data: StandardizedResult):

        for k in data.standardized_data:
            if not ISO_DATA.match(k):
                raise ValidIsoFormatError(
                    f"Date ISO format is required. please convert it on parsing: {k}"
                )

    @staticmethod
    def universal_date_filter(
        data: StandardizedResult,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
    ) -> FilterDatesResult:

        logger.info("=" * 50)
        logger.info("Dates Filter..")
        logger.info("Accept (%s Data)", len(data.standardized_data))
        logger.info("End Year %s Month %s", end_year, end_month)
        DateProcessors.check_if_iso_format(data)

        start_dt = datetime(start_year, start_month, 1)  # default tanggal 1
        end_dt = datetime(end_year, end_month, 1)

        filtered: dict[str, float] = {}

        error: list[str] = []

        for date_key, entry in data.standardized_data.items():
            try:
                entry_dt = datetime.strptime(date_key, "%Y-%m-%d")

                if start_dt <= entry_dt <= end_dt:
                    filtered[date_key] = entry.value

            except ValueError as e:
                logger.error("error filter dates: %s, error: %s", date_key, e)
                error.append(date_key)
                continue

        logger.info(
            "Filter Dates Done.. succes %s error: %s",
            len(filtered),
            len(error),
        )
        logger.debug(
            "Sample Dates Filter Data %s",
            list(filtered.items())[:2],
        )

        return FilterDatesResult(filtered_date=filtered)
