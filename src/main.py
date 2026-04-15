import argparse
import asyncio
import logging
import sys
from typing import Optional
import traceback
from pipeline.processors.indicator import FinalFormatResult, IndicatorsProcessors
from pipeline.processors.parse import ParseProcessors
from pipeline.processors.raw import RawProcessors
from pipeline.processors.standardized import StandardizerProcessors
from upload.postegres.psql import upload_to_db
from config.metadata import ALL_INDICATORS
from monitoring.base_logging.logger import configure_logging
from pipeline.orchestrator import Orchest
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


def list_of_indicators() -> None:
    """list available indicators"""

    for country, category in ALL_INDICATORS.items():
        print(f"\n-{country}:")

        for categories, indicator in category.items():
            print(f"    -{categories}:")

            for indicators in indicator.keys():
                print(f"         -{indicators}")


def valid_input(
    country: str,
    category: Optional[str] = None,
    indicator_name: Optional[str] = None,
) -> bool:
    """Validation Input Country, Category and Indicators"""
    if country not in ALL_INDICATORS:
        logger.error("country not found in metadata: %s", country)
        return False
    if category:
        if category not in ALL_INDICATORS[country]:
            logger.error("category not found in metadata:  %s", category)
            return False
    if indicator_name:
        found = False
        for _, categories in ALL_INDICATORS[country].items():
            if indicator_name in categories:
                found = True
                break
        if not found:
            logger.warning(
                "indicators not found: indicators %s | country: %s",
                indicator_name,
                country,
            )
            return False

    return True


def resolve_log_level(level_str: str) -> int:
    level_mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    key: str = level_str.lower().strip()
    if key not in level_mapping:
        raise ValueError(f"Unknown log level: {level_str!r}")
    return level_mapping[key]


def level_name(log_level: int) -> str:
    level_names = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }
    result = level_names.get(log_level)
    if result is None:
        raise ValueError(f"Unknown log level (int): {log_level}")

    return result


def apply_log_level(log_level: int) -> None:
    log_name: str = level_name(log_level)

    configure_logging(log_level)
    logger.info("Set Logging Level Name %s ", log_name)


def build_injection() -> Orchest:
    procc_raw = RawProcessors()
    procc_standardizer = StandardizerProcessors()
    procc_parse = ParseProcessors()
    procc_indicator = IndicatorsProcessors(procc_raw, procc_parse, procc_standardizer)
    return Orchest(procc_indicator)


async def main() -> FinalFormatResult | None:
    """Main Execute command line pipeline"""
    parse = argparse.ArgumentParser()
    main_group = parse.add_argument_group("structure data")
    main_group.add_argument("-c", "--country", help="country of indicator")

    main_group.add_argument("-n", "--name", help="name of indicators")

    log_group = parse.add_argument_group("Logging Option")
    log_group.add_argument(
        "-l",
        "--log",
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
        help="logging level to monitoring",
    )
    run_mode = parse.add_mutually_exclusive_group(required=False)
    run_mode.add_argument(
        "-r",
        "--run",
        choices=["single", "all"],
        default="all",
        help="select running pipeline mode, default=all ",
    )
    run_mode.add_argument(
        "--list", action="store_true", help="list of available indicators"
    )
    upload = parse.add_argument_group("Upload To DataBase")
    upload.add_argument(
        "-u", "--upload", action="store_true", help="Upload data to PostgreSQL"
    )
    args = parse.parse_args()

    # setup logging
    try:
        if args.log:
            target_level: int = resolve_log_level(args.log)
            apply_log_level(target_level)

    except Exception as e:
        logger.error("Errors setup logs: %s", e, exc_info=True)
    # List of Indicators Availabel on Config Data
    if args.list:
        print("List Available Indicators:")

        list_of_indicators()
        return
    # Country is required for Single mode indicators and etc.., (exclude run all)
    if not args.country and args.run == "single":
        logger.warning("country is required")
        parse.print_help()
        sys.exit(1)

    # Validate Warning, if run mode not give args Indicator Name
    if args.run == "single" and not args.name:
        logger.warning("name is required")
        parse.print_help()
        sys.exit(1)

    if args.run == "single":
        # Indicators Validation If args in ALL Indicators
        if not valid_input(args.country, indicator_name=args.name):
            parse.print_help()
            sys.exit(1)

    # Execute Pipeline
    orchest: Orchest = build_injection()

    data: FinalFormatResult | None = None
    try:
        # Run Single Indicator
        if args.run == "single":
            logger.info("=" * 50)
            logger.info(
                "Run Single Indicator with country: %s | name: %s",
                args.country,
                args.name,
            )
            data = await orchest.run_by_single(args.country, args.name)

        # Default Mode Run Pipeline
        elif args.run == "all":
            logger.info("Run ALL Config Indicators")
            data = await orchest.run_all()

        # store data to db
        if args.upload and data is not None:
            await upload_to_db(data)  # upload_to_db(data)

            logger.info(
                "Upload Data To Postgres with  (%s Data) ",
                len(data.format_result),
            )

        # Summary Final Results Pipeline Data
        logger.info("=" * 50)
        logger.info(
            "Final Data Records (%s Data)",
            len(data.format_result) if data is not None else 0,
        )

    except Exception as e:
        logger.exception("Error during execution pipeline: %s", e)
        print(f"\nFull traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
