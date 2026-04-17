from datetime import datetime
import aiohttp
from providers.bea.model import BEARawRespons
from providers import BaseMetaModel
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    retry_if_exception_type,
    wait_exponential,
)
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


class BEAProvider:
    """
    Example Debug Params:
    "method": "GetParameterValues",
    "datasetname": "ITA",
    "ParameterName": "AreaOrCountry",
    "Year": "ALL",
    "ResultFormat": "JSON",
    """

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.url = "https://apps.bea.gov/api/data"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=70),
        retry=retry_if_exception_type(
            (
                aiohttp.ClientConnectionError,
                aiohttp.ServerTimeoutError,
                aiohttp.ClientResponseError,
            )
        ),
        reraise=True,
    )
    async def request_data(self, meta: BaseMetaModel):

        if not self.api_key:
            raise exc.ResourceNotFound(f"ApiKey Not found for {meta.api}")

        # build Start year and end year
        start_range = list(range(meta.start_year, datetime.now().year + 1))
        start_to_end = ",".join(str(y) for y in start_range)

        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "DataSetName": "ITA",
            "Indicator": meta.id,
            "AreaOrCountry": "AllCountries",
            "Year": start_to_end,
            "Frequency": meta.freq,
            "ResultFormat": "JSON",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                self.url, params=params, timeout=aiohttp.ClientTimeout(30)
            ) as response:
                response.raise_for_status()
                logger.info("BEA Response Status Code Return %s", response.status)

                data = await response.json()

        return BEARawRespons.model_validate(data)

    async def fetch_data(self, meta: BaseMetaModel) -> BEARawRespons:
        try:
            respons = await self.request_data(meta)
            result: BEARawRespons = respons
            logger.debug("BEA Raw Data: %s", result)
            logger.info(
                "BEA Raw Data... Accept (%s data)", len(result.BEAAPI.Results.Data)
            )

            return result
        except aiohttp.ServerTimeoutError as e:
            logger.error(f"Timeout request to BEA {e}")
            raise
        except aiohttp.ClientError as e:
            logger.error(f"Client Error for BEA requests {e}")
            raise
        except exc.BEARequestsError:
            logger.exception("Un-Expected Error BEA Requests")
            raise
