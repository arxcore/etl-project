from datetime import datetime
import requests
from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception_type,
    stop_after_attempt,
)
from providers.metamodel import BaseMetaModel
from providers.bls.model import BLSRawResponsedata, BLSSeries
import logging

logger = logging.getLogger(__name__)


class BlsFetchError(Exception):
    pass


class ApiKeyError(Exception):
    pass


class BLSFetch:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def request(
        self,
        meta: BaseMetaModel,
        api_key: str,
        url: str,
        start_year: int,
        end_year: int,
    ) -> BLSRawResponsedata:
        payload: dict[str, list[str] | str | int] = {
            "seriesid": [meta.id],
            "apikey": api_key,
            "startyear": start_year,
            "endyear": end_year,
        }
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()

        logger.info("BLS.gov Return Status Code: %s", response.status_code)

        data = response.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            msg = data.get("message", ["Unknown api error"])
            raise BlsFetchError(msg[0] if msg else "Unknown api error")

        logger.debug("BLS RAW DATA JSON: %s", data)
        return BLSRawResponsedata.model_validate(data)

    def fetch_data(self, meta: BaseMetaModel) -> BLSSeries:
        """
        returns:
            raw_data bls api -> list[dict]
        """
        # validasi api key if is valid or missing
        if not self.api_key:
            raise ApiKeyError("API KEY ERROR error api key please check on .env file")

        start_year = meta.start_year

        end_year = datetime.now().year

        try:
            # cek request ke BLS APIs
            raw_data = self.request(meta, self.api_key, self.url, start_year, end_year)
            result: BLSSeries = raw_data.Results

            logger.debug("BLS Final Raw-data: %s", result)
            return result

        except requests.ConnectionError as e:
            raise BlsFetchError(f"Please check conection internet {e}") from e

        except requests.exceptions.RequestException as re:
            raise BlsFetchError(f"Requests Except Unknown Error {re}") from re

        except Exception as e:
            raise BlsFetchError(f"FetchDataBLS Error {e}") from e
