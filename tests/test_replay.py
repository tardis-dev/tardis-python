import importlib
import gzip
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tardis_dev import Channel, replay
from tardis_dev.replay import (
    _fetch_data_to_replay,
    _format_replay_query_date,
    _get_filters_hash,
    _get_slice_cache_path,
    _serialize_filters,
)

replay_module = importlib.import_module("tardis_dev.replay")

LIVE_REPLAY_EXCHANGE = "bitmex"
LIVE_REPLAY_FROM = "2019-05-01T00:00:00.000Z"
LIVE_REPLAY_TO = "2019-05-01T00:01:00.000Z"


def _live_replay_filters():
    return [Channel("trade", ["BTCUSD"])]


class _FakeSession:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.live
@pytest.mark.asyncio
async def test_replay_live_data_yields_messages(tmp_path: Path):
    cache_dir = tmp_path / "cache"
    results = []
    async for item in replay(
        exchange=LIVE_REPLAY_EXCHANGE,
        from_date=LIVE_REPLAY_FROM,
        to_date=LIVE_REPLAY_TO,
        filters=_live_replay_filters(),
        cache_dir=str(cache_dir),
    ):
        results.append(item)
        if len(results) >= 1:
            break

    assert len(results) == 1
    assert all(item is not None for item in results)
    assert all(item.local_timestamp.year == 2019 for item in results)
    assert results[0].message["table"] == "trade"
    assert results[0].message["action"] == "partial"
    assert isinstance(results[0].message["data"], list)
    assert len(results[0].message["data"]) > 0


@pytest.mark.live
@pytest.mark.asyncio
async def test_replay_auto_cleanup_removes_live_processed_slices(tmp_path: Path):
    cache_dir = tmp_path / "cache"
    filters = _live_replay_filters()
    results = []
    async for item in replay(
        exchange=LIVE_REPLAY_EXCHANGE,
        from_date=LIVE_REPLAY_FROM,
        to_date=LIVE_REPLAY_TO,
        filters=filters,
        cache_dir=str(cache_dir),
        auto_cleanup=True,
    ):
        results.append(item)

    slice_path = Path(
        _get_slice_cache_path(
            str(cache_dir),
            LIVE_REPLAY_EXCHANGE,
            datetime(2019, 5, 1, 0, 0),
            filters,
        )
    )
    day_dir = cache_dir / "feeds" / LIVE_REPLAY_EXCHANGE / _get_filters_hash(filters) / "2019" / "05" / "01"

    assert len(results) > 0
    assert not slice_path.exists()
    assert not day_dir.exists()


def test_replay_rejects_invalid_date_order():
    async def collect():
        async for _ in replay(exchange="bitmex", from_date="2019-06-02", to_date="2019-06-01"):
            pass

    with pytest.raises(ValueError):
        import asyncio

        asyncio.run(collect())


def test_replay_cache_path_uses_normalized_filter_hash():
    filters = [Channel("trade", ["ETHUSD", "BTCUSD"]), Channel("trade", ["BTCUSD"]), Channel("book")]
    slice_path = _get_slice_cache_path("/tmp/cache", "bitmex", datetime(2019, 6, 1, 0, 0), filters)
    filters_hash = _get_filters_hash(filters)

    assert filters_hash in slice_path
    assert slice_path.endswith("2019/06/01/00/00.json.gz")
    assert _serialize_filters(filters) == [
        {"channel": "book", "symbols": None},
        {"channel": "trade", "symbols": ["BTCUSD", "ETHUSD"]},
    ]


def test_replay_rejects_invalid_filter_items():
    async def collect():
        async for _ in replay(exchange="bitmex", from_date="2019-06-01", to_date="2019-06-02", filters=["bad"]):
            pass

    with pytest.raises(ValueError, match="filters"):
        import asyncio

        asyncio.run(collect())


def test_replay_formats_query_date_as_utc_z():
    assert _format_replay_query_date(datetime.fromisoformat("2019-05-01T00:00:00+00:00")) == "2019-05-01T00:00:00.000Z"


@pytest.mark.asyncio
async def test_replay_accepts_naive_datetime_inputs_as_utc(monkeypatch, tmp_path: Path):
    cache_dir = tmp_path / "cache"
    filters = _live_replay_filters()
    captured = {}

    slice_path = Path(
        _get_slice_cache_path(
            str(cache_dir),
            LIVE_REPLAY_EXCHANGE,
            datetime(2019, 5, 1, 0, 0, tzinfo=timezone.utc),
            filters,
        )
    )
    slice_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(slice_path, "wb") as file:
        file.write(b'2019-05-01T00:00:00.0000000Z {"table":"trade","action":"partial","data":[{"symbol":"BTCUSD"}]}\n')

    async def fake_fetch_data_to_replay(**kwargs):
        captured.update(kwargs)
        return None

    monkeypatch.setattr(replay_module, "_fetch_data_to_replay", fake_fetch_data_to_replay)

    results = []
    async for item in replay(
        exchange=LIVE_REPLAY_EXCHANGE,
        from_date=datetime(2019, 5, 1, 0, 0),
        to_date=datetime(2019, 5, 1, 0, 1),
        filters=filters,
        cache_dir=str(cache_dir),
    ):
        results.append(item)

    assert len(results) == 1
    assert captured["from_date"] == datetime(2019, 5, 1, 0, 0, tzinfo=timezone.utc)
    assert captured["to_date"] == datetime(2019, 5, 1, 0, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_replay_converts_timezone_aware_datetime_inputs_to_utc(monkeypatch, tmp_path: Path):
    cache_dir = tmp_path / "cache"
    filters = _live_replay_filters()
    captured = {}

    utc_from_date = datetime(2019, 5, 1, 0, 0, tzinfo=timezone.utc)
    utc_to_date = datetime(2019, 5, 1, 0, 1, tzinfo=timezone.utc)

    slice_path = Path(_get_slice_cache_path(str(cache_dir), LIVE_REPLAY_EXCHANGE, utc_from_date, filters))
    slice_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(slice_path, "wb") as file:
        file.write(b'2019-05-01T00:00:00.0000000Z {"table":"trade","action":"partial","data":[{"symbol":"BTCUSD"}]}\n')

    async def fake_fetch_data_to_replay(**kwargs):
        captured.update(kwargs)
        return None

    monkeypatch.setattr(replay_module, "_fetch_data_to_replay", fake_fetch_data_to_replay)

    results = []
    async for item in replay(
        exchange=LIVE_REPLAY_EXCHANGE,
        from_date=datetime(2019, 5, 1, 2, 0, tzinfo=timezone(timedelta(hours=2))),
        to_date=datetime(2019, 5, 1, 2, 1, tzinfo=timezone(timedelta(hours=2))),
        filters=filters,
        cache_dir=str(cache_dir),
    ):
        results.append(item)

    assert len(results) == 1
    assert captured["from_date"] == utc_from_date
    assert captured["to_date"] == utc_to_date


@pytest.mark.asyncio
async def test_replay_raw_mode_returns_bytes(monkeypatch, tmp_path: Path):
    cache_dir = tmp_path / "cache"
    filters = _live_replay_filters()
    slice_path = Path(
        _get_slice_cache_path(
            str(cache_dir),
            LIVE_REPLAY_EXCHANGE,
            datetime(2019, 5, 1, 0, 0),
            filters,
        )
    )
    slice_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(slice_path, "wb") as file:
        file.write(b'2019-05-01T00:00:00.0000000Z {"table":"trade","action":"partial","data":[{"symbol":"BTCUSD"}]}\n')

    async def fake_fetch_data_to_replay(**kwargs):
        return None

    monkeypatch.setattr(replay_module, "_fetch_data_to_replay", fake_fetch_data_to_replay)

    results = []
    async for item in replay(
        exchange=LIVE_REPLAY_EXCHANGE,
        from_date=LIVE_REPLAY_FROM,
        to_date=LIVE_REPLAY_TO,
        filters=filters,
        cache_dir=str(cache_dir),
        decode_response=False,
    ):
        results.append(item)

    assert len(results) == 1
    assert results[0] is not None
    assert isinstance(results[0].local_timestamp, bytes)
    assert isinstance(results[0].message, bytes)
    assert results[0].local_timestamp == b"2019-05-01T00:00:00.0000000Z"
    assert results[0].message == b'{"table":"trade","action":"partial","data":[{"symbol":"BTCUSD"}]}\n'


@pytest.mark.asyncio
async def test_fetch_data_to_replay_prefetches_last_then_first(monkeypatch):
    offsets = []

    async def fake_create_session(api_key: str, timeout: int):
        return _FakeSession()

    async def fake_fetch_slice_if_not_cached(**kwargs):
        offsets.append(kwargs["offset"])

    monkeypatch.setattr(replay_module, "create_session", fake_create_session)
    monkeypatch.setattr(replay_module, "_fetch_slice_if_not_cached", fake_fetch_slice_if_not_cached)

    await _fetch_data_to_replay(
        exchange="bitmex",
        from_date=datetime(2019, 6, 1, 0, 0),
        to_date=datetime(2019, 6, 1, 0, 4),
        filters=None,
        endpoint="https://api.tardis.dev/v1",
        cache_dir="/tmp/cache",
        api_key="",
        timeout=60,
        http_proxy=None,
        filters_hash="hash",
    )

    assert offsets[:2] == [3, 0]
    assert sorted(offsets[2:]) == [1, 2]
