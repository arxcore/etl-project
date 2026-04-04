from datetime import datetime
from datetime import timedelta
import logging
from pipeline.processors.date import FilterDatesResult


logger = logging.getLogger(__name__)


class CalculatedMethodUnknown(Exception):
    pass


class CalculatedError(Exception):
    pass


class DataCalculation:
    """Main Calculation for indicator data"""

    @staticmethod
    def raw_value(data_point: list[tuple[datetime, float]]) -> dict[str, float]:
        result: dict[str, float] = {}
        for date, value in data_point:
            result[date.strftime("%Y-%m-%d")] = value
        return result

    @staticmethod
    def _calc_pct_change_by_month_lag(
        data_point: list[tuple[datetime, float]],
        month_lag: int,
        label: str,
    ) -> dict[str, float]:
        """
        Generic percentage change using (year, month) keys.
        - month_lag=1  -> MoM
        - month_lag=3  -> QoQ (approx for monthly series)
        - month_lag=12 -> YoY
        """
        result: dict[str, float] = {}
        lookup = {(dt.year, dt.month): val for dt, val in data_point}

        for date, value in data_point:
            # convert (year, month) to absolute month index
            idx = date.year * 12 + (date.month - 1)
            prev_idx = idx - month_lag
            prev_year = prev_idx // 12
            prev_month = (prev_idx % 12) + 1

            prev_key = (prev_year, prev_month)
            if prev_key not in lookup:
                logger.warning("missing value for %s date: %s", label, prev_key)
                continue

            prev_value = lookup[prev_key]
            pct = (value - prev_value) / prev_value * 100
            result[date.strftime("%Y-%m-%d")] = round(pct, 4)

        result = dict(sorted(result.items(), reverse=True))
        return result

    @staticmethod
    def _calc_net_change_by_month_lag(
        data_point: list[tuple[datetime, float]], month_lag: int, label: str
    ) -> dict[str, float]:
        result: dict[str, float] = {}
        # build lookup
        lookup: dict[tuple[int, int], float] = {
            (date.year, date.month): value for date, value in data_point
        }
        for date, value in data_point:
            idx = date.year * 12 + (date.month - 1)
            prev_idx = idx - month_lag
            prev_year = prev_idx // 12
            prev_month = (prev_idx % 12) + 1

            prev_key = (prev_year, prev_month)
            if prev_key not in lookup:
                logger.warning(
                    "Missing Value For Dates %s, Calculate %s change", prev_key, label
                )
                continue

            prev_value = lookup[prev_key]
            net = value - prev_value

            result[date.strftime("%Y-%m-%d")] = round(net, 4)
        result = dict(sorted(result.items(), reverse=True))
        return result

    @staticmethod
    def calc_mom(data_point: list[tuple[datetime, float]]) -> dict[str, float]:
        return DataCalculation._calc_pct_change_by_month_lag(data_point, 1, "mom")

    @staticmethod
    def calc_qoq(data_point: list[tuple[datetime, float]]) -> dict[str, float]:
        return DataCalculation._calc_pct_change_by_month_lag(data_point, 3, "qoq")

    @staticmethod
    def calc_yoy(data_point: list[tuple[datetime, float]]) -> dict[str, float]:
        return DataCalculation._calc_pct_change_by_month_lag(data_point, 12, "yoy")

    @staticmethod
    def calc_net(data_point: list[tuple[datetime, float]]) -> dict[str, float]:
        return DataCalculation._calc_net_change_by_month_lag(data_point, 1, "net")

    @staticmethod
    def calc_wow(data_point: list[tuple[datetime, float]]) -> dict[str, float]:
        """
        Week-over-week percentage change using exact date keys.
        Works for daily/weekly series where dates are consecutive days/weeks.
        """
        result: dict[str, float] = {}
        lookup = {dt: val for dt, val in data_point}

        for date, value in data_point:
            prev_date = date - timedelta(days=7)
            if prev_date not in lookup:
                logger.warning("missing value for wow date: %s", prev_date.date())
                continue

            prev_value = lookup[prev_date]
            wow = (value - prev_value) / prev_value * 100
            result[date.strftime("%Y-%m-%d")] = round(wow, 4)

        result = dict(sorted(result.items(), reverse=True))

        return result

    @staticmethod
    def parse_date_key(
        filter_date: FilterDatesResult,
    ) -> list[tuple[datetime, float]]:

        data_point: list[tuple[datetime, float]] = []

        for date_key, value in filter_date.filtered_date.items():
            try:
                date_parse = datetime.strptime(date_key, "%Y-%m-%d")

                # save hasil parsing ke list sequence index unutk bisa sorting
                data_seq: tuple[datetime, float] = (date_parse, value)
                data_point.append(data_seq)
            except ValueError as e:
                logger.error("parsing date key error unknown: %s", e)
                continue

        return data_point

    @staticmethod
    def calculated_router(
        filter_date: FilterDatesResult, method: str, name: str
    ) -> dict[str, float]:
        """
        Process data for multi methode
        method: methode to calculation example:  mom/yoy..etc
        """
        logger.debug("Sample Data TO Calculate %s", filter_date.filtered_date)
        # parrsing date key str -> datetime
        date_parse = DataCalculation.parse_date_key(filter_date)

        calculated = {
            "raw": lambda: DataCalculation.raw_value(date_parse),
            "yoy": lambda: DataCalculation.calc_yoy(date_parse),
            "mom": lambda: DataCalculation.calc_mom(date_parse),
            "qoq": lambda: DataCalculation.calc_qoq(date_parse),
            "wow": lambda: DataCalculation.calc_wow(date_parse),
            "net": lambda: DataCalculation.calc_net(date_parse),
        }

        # return DataCalculation.raw_value(date_parse)
        if method not in calculated:
            raise CalculatedMethodUnknown(
                f"Unknown Type Calculated, for method {method}"
            )
        try:
            return calculated[method]()
        except ZeroDivisionError as e:
            raise CalculatedError(
                f"UnExpected zero value in calculated,, please check it for method {method}"
            ) from e
        except Exception as e:
            raise CalculatedError(
                f"Un-Expected Error Calculated, method {method}"
            ) from e
