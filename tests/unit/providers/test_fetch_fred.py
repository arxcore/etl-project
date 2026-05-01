import aiohttp
import pytest
from unittest.mock import AsyncMock, MagicMock
from pytest_mock import MockFixture, MockerFixture
from providers.fred import FREDProvider
from providers.fred.model import FREDRawResponse
from providers import BaseMetaModel

fake_respons = {"observations": [{"date": "2024-02-01", "value": "5.33"}]}
meta: BaseMetaModel = BaseMetaModel(
    id="FEDFUNDS",
    api="fred",
    calc="raw",
    start_year=2024,
    start_month=2,
    freq="monthly",
    unit="percent",
    description="fed rate",
)


@pytest.mark.asyncio
async def test_fetch_fred_succs(mocker: MockFixture):
    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.raise_for_status = MagicMock()
    mock_respons.json = AsyncMock(return_value=fake_respons)
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock)
    async with FREDProvider(api_key="123") as provider:
        data = await provider.fetch_data(meta)
        assert isinstance(data, FREDRawResponse)


@pytest.mark.asyncio
async def test_fetch_fred_error(mocker: MockerFixture):
    mock_respons = AsyncMock()
    mock_respons.raise_for_status = MagicMock(
        side_effect=aiohttp.ClientResponseError(
            status=400, request_info=MagicMock(), history=()
        )
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock)
    async with FREDProvider(api_key="213") as provider:
        with pytest.raises(aiohttp.ClientResponseError):
            await provider.fetch_data(meta)


@pytest.mark.asyncio
async def test_fetch_fred_retry(mocker: MockerFixture):
    """Retry status http 5xx Test"""
    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.json = AsyncMock(return_value=fake_respons)
    mock_respons.raise_for_status = MagicMock(
        side_effect=[
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            # succes respons
            None,
        ]
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    retry = mocker.patch("aiohttp.ClientSession.get", return_value=mock)

    async with FREDProvider(api_key="123") as provider:
        result = await provider.fetch_data(meta)
        assert isinstance(result, FREDRawResponse)
    assert retry.call_count == 5
