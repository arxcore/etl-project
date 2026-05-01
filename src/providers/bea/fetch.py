from datetime import datetime
import aiohttp
from pydantic import ValidationError
from providers.bea.model import BEARawRespons
from providers import BaseMetaModel
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    retry_if_exception,
    wait_exponential,
)
import monitoring.exc_models as exc
from typing import Callable, cast
from providers.retry_http import Retryable

logger = logging.getLogger(__name__)


def is_retryable(error: Exception) -> bool:
    if isinstance(error, aiohttp.ClientResponseError):
        return error.status >= 500
    return isinstance(
        error, (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError)
    )


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
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        if self.session:
            await self.session.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=70),
        retry=retry_if_exception(cast(Callable[[BaseException], bool], Retryable)),
        reraise=True,
    )
    async def fetch_data(self, meta: BaseMetaModel) -> BEARawRespons:
        """Fetch data from BEA API"""

        # Check api key if not exists
        if not self.api_key:
            raise exc.ResourceNotFound(f"ApiKey Not found for {meta.api}")

        # build Start year and end year
        start_range = list(range(meta.start_year, datetime.now().year + 1))
        start_to_end = ",".join(str(y) for y in start_range)

        # build params
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

        if self.session is None:
            raise RuntimeError("BEA Provider Session is None")

        # aiiohttp Context Manager
        async with self.session.get(
            self.url, params=params, timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            # Retry if status code http >= 500
            # 4xx, 5xx
            response.raise_for_status()

            logger.info("BEA Response Status Code Return %s", response.status)

            # 1xx, 3xx, etc..
            if response.status != 200:
                raise exc.BEARequestsError("Unexpected Error Respons")

            try:
                data = await response.json()

                logger.debug("BEA Raw Data: %s", data)
                # Validate data
                result = BEARawRespons.model_validate(data)
                logger.info(
                    "BEA Raw Data... Accept (%s data)",
                    len(result.BEAAPI.Results.Data),
                )
                return result
            except aiohttp.ContentTypeError as e:
                # TODO:
                # test if content type not json fromat
                # wrap into providers handling
                raise exc.BEARequestsError(f"Content Error {e}") from e
            except ValidationError as e:
                # wrap
                raise exc.BEARequestsError(f"Validation Response Error {e}") from e
