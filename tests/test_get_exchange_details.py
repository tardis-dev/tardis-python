import pytest

from tardis_dev import get_exchange_details, get_exchange_details_async


@pytest.mark.live
@pytest.mark.asyncio
async def test_get_exchange_details_async_returns_live_json():
    result = await get_exchange_details_async("deribit")

    assert result["id"] == "deribit"
    assert result["name"] == "Deribit"
    assert "datasets" in result
    assert "trades" in result["availableChannels"]


@pytest.mark.live
def test_get_exchange_details_sync_wrapper_runs_on_live_data():
    result = get_exchange_details("deribit")

    assert result["id"] == "deribit"
    assert result["name"] == "Deribit"


@pytest.mark.asyncio
async def test_get_exchange_details_sync_wrapper_raises_in_running_loop():
    with pytest.raises(RuntimeError, match="get_exchange_details_async"):
        get_exchange_details("deribit")
