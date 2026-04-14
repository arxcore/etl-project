from config.settings import Resources
from providers.bls import BLSProvider
from providers.fred import FREDProvider
from providers.bea import BEAProvider
from providers import BaseMetaModel
from pipeline.routing import BaseFetcherReturn
import logging

logger = logging.getLogger(__name__)


class RawProcessors:
    """Raw Processors Fetch ALL Prioviders Data"""

    def __init__(self, resource: Resources | None = None):
        self.resource = resource or Resources()
        self.providers = {
            "bls": BLSProvider(self.resource.bls_api_key),
            "fred": FREDProvider(self.resource.fred_api_key),
            "bea": BEAProvider(self.resource.bea_api_key),
        }

    async def process_raw_data(self, meta: BaseMetaModel) -> BaseFetcherReturn:
        """Fetch Raw Data from ALL Prioviders"""
        logger.info("=" * 50)
        logger.info("Fetch Raw Data From %s |  ApiID %s", meta.api, meta.id)
        logger.info(
            "Start Year %s Month %s Frequency %s",
            meta.start_year,
            meta.start_month,
            meta.freq,
        )
        logger.info("=" * 50)
        if meta.api not in self.providers:
            # TODO: refactore with exception hierarky patern
            raise ValueError(f"Source {meta.id} not Found")
        try:
            providers_cls = self.providers[meta.api]
            raw_data = await providers_cls.fetch_data(meta)
            return BaseFetcherReturn(api_type=meta.api, fetch_result=raw_data)
        except Exception as e:
            # TODO:
            logger.error("Error Fetch Data from Source %s, %s", meta.api, e)
            raise
