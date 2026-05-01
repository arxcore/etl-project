from datetime import datetime
import logging

from pydantic import ValidationError
from providers import BaseMetaModel
from providers.fred.model import FREDRawResponse
import aiohttp
from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception,
    stop_after_attempt,
)
import monitoring.exc_models as exc
from providers.retry_http import Retryable
from typing import Callable, cast

logger = logging.getLogger(__name__)


class FREDProvider:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.url = "https://api.stlouisfed.org/fred/series/observations"
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: object | None,
    ):
        if self.session:
            await self.session.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=70),
        retry=retry_if_exception(cast(Callable[[BaseException], bool], Retryable)),
        reraise=True,
    )
    async def fetch_data(self, meta: BaseMetaModel) -> FREDRawResponse:
        """Fetch FRED Data"""

        # chekc api key
        if not self.api_key:
            raise exc.ResourceNotFound(f"Api Key not found for name: {meta.api}")

        # build starr year and month
        start_year = f"{meta.start_year}-{meta.start_month:02d}-01"

        # build end year
        end_year = datetime.now().strftime("%Y-%m-%d")

        # build params
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
                # 4xx, 5xx
                response.raise_for_status()

                logger.info("FRED Return Status Codea: %s", response.status)

                # 1xx, 3xx, etc..
                if response.status != 200:
                    raise exc.FREDRequestsError("Unexpected Error Responns")
                try:
                    data = await response.json()

                    # WARNING:
                    # Find out more about the error message in this “error_code”
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
                            raise exc.FREDRequestsError(
                                f"Unknown FRED Requests Error {error_msg}"
                            )

                    return FREDRawResponse.model_validate(data)

                except ValidationError as e:
                    raise exc.FREDRequestsError(f"Validation Response Error {e}") from e
                except aiohttp.ContentTypeError as e:
                    raise exc.FREDRequestsError(f"Content Error {e}") from e
