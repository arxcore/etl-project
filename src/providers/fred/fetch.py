from datetime import datetime
import requests
import logging
from providers.bls.fetch import ApiKeyError
from providers import BaseMetaModel
from providers.fred.model import FREDRawResponse
from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception_type,
    stop_after_attempt,
)

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    pass


class RateLimitError(Exception):
    pass


class FredRequestError(Exception):
    pass


class FREDFetch:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.url = "https://api.stlouisfed.org/fred/series/observations"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=70),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def request(self, meta: BaseMetaModel, start_year: str, end_year: str):
        _paramss = {
            "api_key": self.api_key,
            "file_type": "json",
            "series_id": meta.id,
            "observation_start": start_year,
            "observation_end": end_year,
            "sort_order": "desc",
        }
        response = requests.get(self.url, params=_paramss, timeout=30)
        response.raise_for_status()

        logger.info("FRED Return Status Codea: %s", response.status_code)
        data = response.json()

        if "error_code" in data:
            error_msg = data.get("error_message", "Unknown Error")
            logger.error("FRED API Error: %s", error_msg)

            if data.get("error_code") == 429:
                raise RateLimitError(f"RateLimitError {error_msg}")
            elif data.get("error_code") == 401:
                raise AuthenticationError(f"AuthenticationError {error_msg} ")
            else:
                raise FredRequestError(f"Unknown FRED Error {error_msg}")

        return FREDRawResponse.model_validate(data)

    def fetch_data(self, meta: BaseMetaModel):
        """
        Fetch Fred Data
        """
        if not self.api_key:
            raise ApiKeyError(f"Api Key not found for name: {meta.api}")
        start_year = f"{meta.start_year}-{meta.start_month:02d}-01"

        end_year = datetime.now().strftime("%Y-%m-%d")

        try:
            data_response = self.request(meta, start_year, end_year)
            result: FREDRawResponse = data_response

            logger.debug("RaW Data FRED %s", result)
            logger.info("RAW data FRED %s", len(result.observations))

            return result
        except requests.RequestException as e:
            raise FredRequestError("Request to FRED Api unknown error") from e
        except Exception as e:
            raise FredRequestError("UnException Error to Fred Server") from e
