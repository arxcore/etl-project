from datetime import datetime
import logging
from providers import BaseMetaModel
from providers.fred.model import FREDRawResponse
import aiohttp
from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception_type,
    stop_after_attempt,
)
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


class FREDProvider:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.url = "https://api.stlouisfed.org/fred/series/observations"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=70),
        retry=retry_if_exception_type(
            (
                aiohttp.ClientResponseError,
                aiohttp.ClientConnectionError,
                aiohttp.ServerTimeoutError,
            )
        ),
        reraise=True,
    )
    async def request(self, meta: BaseMetaModel, start_year: str, end_year: str):

        if not self.api_key:
            raise exc.ResourceNotFound(f"Api Key not found for name: {meta.api}")

        params: dict[str, str] = {
            "api_key": self.api_key,
            "file_type": "json",
            "series_id": meta.id,
            "observation_start": start_year,
            "observation_end": end_year,
            "sort_order": "desc",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                self.url, params=params, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response.raise_for_status()

                logger.info("FRED Return Status Codea: %s", response.status)
                data = await response.json()

        if "error_code" in data:
            error_msg = data.get("error_message", "Unknown Error")
            logger.error("FRED API Error: %s", error_msg)

            if data.get("error_code") == 429:
                raise exc.RateLimit(f"Ratelimit requests: {error_msg}")
            elif data.get("error_code") == 401:
                raise exc.AuthenticationError(
                    f"Authentication error from requests: {error_msg} "
                )
            else:
                logger.error(f"Unknown FRED Requests Error {error_msg}")

        return FREDRawResponse.model_validate(data)

    async def fetch_data(self, meta: BaseMetaModel):
        """
        Fetch Fred Data
        """
        start_year = f"{meta.start_year}-{meta.start_month:02d}-01"

        end_year = datetime.now().strftime("%Y-%m-%d")

        try:
            data_response = await self.request(meta, start_year, end_year)
            result: FREDRawResponse = data_response

            logger.debug("RaW Data FRED %s", result)
            logger.info("RAW data FRED %s", len(result.observations))

            return result

        except aiohttp.ServerTimeoutError as e:
            logger.error(f"Timeout Error from FRED requests {e}")
            raise
        except aiohttp.ClientError as e:
            logger.error(f"Client Error for FRED Api {e}")
            raise
        except exc.FREDRequestsError:
            logger.exception("Un-Expected Error FRED Requests")
            raise
