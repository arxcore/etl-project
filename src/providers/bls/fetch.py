from datetime import datetime
from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception_type,
    stop_after_attempt,
)
from providers.metamodel import BaseMetaModel
from providers.bls.model import BLSRawResponsedata, BLSSeries
import logging
import aiohttp
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


class BLSProvider:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(
            (
                aiohttp.ServerTimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientResponseError,
            )
        ),
        reraise=True,
    )
    async def request(
        self,
        meta: BaseMetaModel,
        start_year: int,
        end_year: int,
    ) -> BLSRawResponsedata:

        # validasi api key if is valid or missing
        if not self.api_key:
            raise exc.ResourceNotFound(
                "API KEY ERROR error api key please check on .env file"
            )

        payload: dict[str, list[str] | str | int] = {
            "seriesid": [meta.id],
            "apikey": self.api_key,
            "startyear": start_year,
            "endyear": end_year,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.url, json=payload, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response.raise_for_status()

                logger.info("BLS.gov Return Status Code: %s", response.status)

                data = await response.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            msg = data.get("message", ["Unknown api error"])
            raise exc.BLSRequestsError(msg[0] if msg else "Unknown api error")

        logger.debug("BLS RAW DATA JSON: %s", data)
        return BLSRawResponsedata.model_validate(data)

    async def fetch_data(self, meta: BaseMetaModel) -> BLSSeries:
        """
        returns:
            raw_data bls api -> list[dict]
        """

        start_year = meta.start_year

        end_year = datetime.now().year

        try:
            # cek request ke BLS APIs
            raw_data = await self.request(meta, start_year, end_year)
            result: BLSSeries = raw_data.Results

            logger.debug("BLS Final Raw-data: %s", result)
            return result

        except aiohttp.ServerTimeoutError as e:
            logger.error(f"Timeout from BLS requets {e}")
            raise
        except aiohttp.ClientError as re:
            logger.error(f"Client Error from BLS  {re}")
            raise

        except exc.BLSRequestsError:
            logger.exception("FetchDataBLS Error")
            raise
