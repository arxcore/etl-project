from config.settings import ResourceAPIkey
import logging
from providers.bls import BLSFetch
from providers.fred import FREDFetch
from providers.bea import BEAFetch
from providers import BaseMetaModel
from pipeline.routing import BaseFetcherReturn


logger = logging.getLogger(__name__)


class FetchRouterError(Exception):
    pass


class RoutingFetcher:
    def __init__(self, resource: ResourceAPIkey | None = None):
        self.resource = resource or ResourceAPIkey()
        self.fetch_router = {
            "bls": BLSFetch(self.resource.bls_api_key),
            "fred": FREDFetch(self.resource.fred_api_key),
            "bea": BEAFetch(self.resource.bea_api_key),
        }

    def fetch_api_type(
        self, api_type: str, api_id: str, meta: BaseMetaModel
    ) -> BaseFetcherReturn:
        """
        Fetch Data Diverence Source
        """
        logger.info("=" * 50)
        logger.info("Fetch Raw Data From %s |  ApiID %s", api_type, api_id)
        logger.info(
            "Start Year %s Month %s Frequency %s",
            meta.start_year,
            meta.start_month,
            meta.freq,
        )
        logger.info("=" * 50)
        try:
            if api_type not in self.fetch_router:
                raise FetchRouterError(f"Api type {api_type} not found in fetch router")
            fetcher_cls = self.fetch_router[api_type]
            raw_data = fetcher_cls.fetch_data(meta)

            return BaseFetcherReturn(api_type=api_type, fetch_result=raw_data)
        except Exception as e:
            raise FetchRouterError(
                f"Failed to fetch data for api type: {api_type}-{e}"
            ) from e
