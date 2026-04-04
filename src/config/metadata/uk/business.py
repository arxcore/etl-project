from config.constants import MONTHLY, SOURCE_LINKS, YOY

UK_BUSINESS = {
    "UK_IndustrialProduction_YoY": {
        "url": "https://www.ons.gov.uk/file?uri=/economy/economicoutputandproductivity/output/datasets/indexofproduction/current/diop.xlsx",
        "cdid": "K27Q",
        "freq": MONTHLY,
        "api": SOURCE_LINKS,
        "calc": YOY,
        "start_year": 2023,
        "start_month": 3,
        "description": "INDUSTRUAL PRODUCT PRODUCTION YOY Change DATASET = DIOP",
    },
    "UK_IndustrialProduction_MoM": {
        "url": "https://www.ons.gov.uk/file?uri=/economy/economicoutputandproductivity/output/datasets/indexofproduction/current/diop.xlsx",
        "cdid": "K27Q",
        "freq": MONTHLY,
        "api": SOURCE_LINKS,
        "calc": None,
        "start_year": 2024,
        "start_month": 3,
        "description": "INDUSTRUAL PRODUCT PRODUCTION MoM Change DATASET = DIOP",
    },
}
