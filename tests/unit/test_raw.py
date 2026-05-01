from unittest import mock
import aiohttp
from pipeline.routing import RawProcessors
import providers
from providers.metamodel import BaseMetaModel
import pytest
from pytest_mock import MockerFixture
from unittest.mock import AsyncMock, MagicMock, patch

m1: BaseMetaModel = BaseMetaModel(
    id="CUUR0000SA0",
    api="bls",
    freq="monthly",
    calc="yoy",
    start_year=2020,
    start_month=1,
    unit="%",
    description="cpi yoy",
)
m2: BaseMetaModel = BaseMetaModel(
    id="CUUR",
    api="bea",
    freq="monthly",
    calc="yoy",
    start_year=2020,
    start_month=1,
    unit="%",
    description="cur",
)

m3: BaseMetaModel = BaseMetaModel(
    id="SA0",
    api="fred",
    freq="monthly",
    calc="yoy",
    start_year=2020,
    start_month=1,
    unit="%",
    description="sao",
)
meta = [m1, m2, m3]


@pytest.mark.asyncio
async def test_raw_lifo_cycle(mocker: MockerFixture):
    mock_bls = AsyncMock()
    mock_bls.__aenter__.return_value = "ok"

    mock_bea = AsyncMock()
    mock_bea.__aenter__.side_effect = Exception("crash")

    mock_fred = AsyncMock()
    mock_fred.__aenter__.return_value = "ok"

    mocker.patch("pipeline.routing.raw.BLSProvider", return_value=mock_bls)
    mocker.patch("pipeline.routing.raw.BEAProvider", return_value=mock_bea)
    mocker.patch("pipeline.routing.raw.FREDProvider", return_value=mock_fred)

    with pytest.raises(Exception, match="crash"):
        async with RawProcessors(resource=None) as r:
            for m in meta:
                await r.process_raw_data(m)

    # chek bls session wass closed if bea provider raised an exception
    mock_bls.__aexit__.assert_called_once()
    # chek bea session wass closed if bea provider raised an exception
    mock_bea.__aexit__.assert_not_called()
    # chek fred session wass closed if bea provider raised an exception
    mock_fred.__aexit__.assert_not_called()
