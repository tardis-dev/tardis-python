import importlib
import re
import urllib.error

import pytest
from aioresponses import aioresponses

from tardis_dev import (
    find_instrument_symbols,
    find_instrument_symbols_async,
    get_instrument_info,
    get_instrument_info_async,
)

instrument_info_module = importlib.import_module("tardis_dev.instrument_info")

BINANCE_INSTRUMENTS_URL = re.compile(r"^https://api\.tardis\.dev/v1/instruments/binance\?filter=.*$")


@pytest.mark.asyncio
async def test_get_instrument_info_async_rejects_ambiguous_arguments():
    with pytest.raises(ValueError, match="either 'filter' or 'symbol'"):
        await get_instrument_info_async("binance", filter={"active": True}, symbol="btcusdt")

    with pytest.raises(ValueError, match="single exchange"):
        await get_instrument_info_async(["binance", "coinbase"], symbol="btcusdt")


@pytest.mark.asyncio
async def test_find_instrument_symbols_async_selects_id_or_dataset_id():
    with aioresponses() as mocked:
        mocked.get(BINANCE_INSTRUMENTS_URL, payload=[{"id": "btcusdt", "datasetId": "BTCUSDT"}, {"id": "ethusdt"}])
        mocked.get(BINANCE_INSTRUMENTS_URL, payload=[{"id": "btcusdt", "datasetId": "BTCUSDT"}, {"id": "ethusdt"}])

        id_result = await find_instrument_symbols_async(["binance"], {"active": True})
        dataset_result = await find_instrument_symbols_async(["binance"], {"active": True}, selector="datasetId")

    assert id_result == [{"exchange": "binance", "symbols": ["btcusdt", "ethusdt"]}]
    assert dataset_result == [{"exchange": "binance", "symbols": ["BTCUSDT", "ethusdt"]}]


@pytest.mark.asyncio
async def test_find_instrument_symbols_async_raises_http_error_for_non_200():
    with aioresponses() as mocked:
        mocked.get(BINANCE_INSTRUMENTS_URL, status=401)

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            await find_instrument_symbols_async(["binance"], {"active": True})

        assert exc_info.value.code == 401


def test_find_instrument_symbols_sync_wrapper_runs(monkeypatch):
    async def fake_find_instrument_symbols_async(**kwargs):
        return [{"exchange": kwargs["exchanges"][0], "symbols": ["btcusdt"]}]

    monkeypatch.setattr(instrument_info_module, "find_instrument_symbols_async", fake_find_instrument_symbols_async)

    assert find_instrument_symbols(["binance"], {"active": True}) == [{"exchange": "binance", "symbols": ["btcusdt"]}]


def test_get_instrument_info_sync_wrapper_runs(monkeypatch):
    async def fake_get_instrument_info_async(**kwargs):
        return [{"id": "btcusdt", "exchange": kwargs["exchange"]}]

    monkeypatch.setattr(instrument_info_module, "get_instrument_info_async", fake_get_instrument_info_async)

    assert get_instrument_info("binance", filter={"active": True}) == [{"id": "btcusdt", "exchange": "binance"}]


@pytest.mark.asyncio
async def test_find_instrument_symbols_sync_wrapper_raises_in_running_loop():
    with pytest.raises(RuntimeError, match="find_instrument_symbols_async"):
        find_instrument_symbols(["binance"], {"active": True})


@pytest.mark.asyncio
async def test_get_instrument_info_sync_wrapper_raises_in_running_loop():
    with pytest.raises(RuntimeError, match="get_instrument_info_async"):
        get_instrument_info("binance", filter={"active": True})


@pytest.mark.asyncio
async def test_find_instrument_symbols_async_rejects_invalid_selector():
    with pytest.raises(ValueError, match="selector"):
        await find_instrument_symbols_async(["binance"], {"active": True}, selector="nativeId")


@pytest.mark.live
@pytest.mark.asyncio
async def test_get_instrument_info_async_returns_live_bitmex_metadata():
    result = await get_instrument_info_async("bitmex", symbol="XBTUSD")

    assert result["id"] == "XBTUSD"
    assert result["datasetId"] == "XBTUSD"
    assert result["exchange"] == "bitmex"
    assert result["baseCurrency"] == "BTC"
    assert result["quoteCurrency"] == "USD"
    assert result["type"] == "perpetual"
    assert result["contractType"] == "inverse_perpetual"
    assert result["underlyingType"] == "native"
    assert result["active"] is True


@pytest.mark.live
@pytest.mark.asyncio
async def test_find_instrument_symbols_async_returns_live_bitmex_symbols():
    filter_payload = {
        "baseCurrency": "BTC",
        "quoteCurrency": "USD",
        "type": "perpetual",
        "contractType": "inverse_perpetual",
        "underlyingType": "native",
        "active": True,
    }

    assert await find_instrument_symbols_async(["bitmex"], filter_payload) == [{"exchange": "bitmex", "symbols": ["XBTUSD"]}]
    assert await find_instrument_symbols_async(["bitmex"], filter_payload, selector="datasetId") == [
        {"exchange": "bitmex", "symbols": ["XBTUSD"]}
    ]
