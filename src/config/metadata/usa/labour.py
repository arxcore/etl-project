from config.constants import (
    MOM,
    MONTHLY,
    NET,
    RAW,
    SOURCE_BLS,
    # SOURCE_FRED,
    UNIT_PEOPLE,
    UNIT_PERCENT,
    # WEEKLY,
)
from providers.bls.model import BLSConfigModel

US_LABOUR: dict[str, BLSConfigModel] = {
    "US_NFP": BLSConfigModel(
        id="CES0000000001",
        api=SOURCE_BLS,
        start_year=2024,
        start_month=2,
        calc=NET,
        freq=MONTHLY,
        unit=UNIT_PEOPLE,
        description="Nonfarm Payrolls, Net Change",
    ),
    "US_Unemployment": BLSConfigModel(
        id="LNS14000000",
        api=SOURCE_BLS,
        start_year=2024,
        start_month=3,
        calc=RAW,
        freq=MONTHLY,
        unit=UNIT_PERCENT,
        description="Unemployment Rate",
    ),
    "US_AverageHourlyEarnings": BLSConfigModel(
        id="CES0500000003",
        api=SOURCE_BLS,
        start_year=2024,
        start_month=2,
        calc=MOM,
        freq=MONTHLY,
        unit=UNIT_PERCENT,
        description="Average Hourly Earnings, Month-over-Month Change",
    ),
}
"""
    "US_InitialJoblessClaim": {
        "id": "ICSA",
        "api": SOURCE_FRED,
        "start_year": 2025,
        "start_month": 1,
        "calc": None,
        "freq": WEEKLY,
        "unit": UNIT_PEOPLE,
        "description": "Initial Jobless Claims, Seasonally Adjusted",
    },
}
"""
