import asyncio
import hashlib
import importlib
import gzip
import os
import re
import urllib.error
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from tardis_dev import default_file_name, download_datasets, download_datasets_async
from tests.test_http import capture_retry_delays

download_datasets_module = importlib.import_module("tardis_dev.download_datasets")

LIVE_DATASET_EXCHANGE = "deribit"
LIVE_DATASET_TYPE = "trades"
LIVE_DATASET_SYMBOL = "BTC-PERPETUAL"
LIVE_DATASET_FROM = "2024-01-01"
LIVE_DATASET_TO = "2024-01-02"
DATASET_RANGE_SIZE = 64 * 1024 * 1024


class _FakeSession:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


def test_default_file_name_sanitizes_symbol():
    filename = default_file_name("deribit", "trades", __import__("datetime").datetime(2024, 1, 1), "BTC/USDT:PERP", "csv")

    assert filename == "deribit_trades_2024-01-01_BTC-USDT-PERP.csv.gz"


@pytest.mark.live
@pytest.mark.asyncio
async def test_download_datasets_async_downloads_live_file(tmp_path: Path):
    await download_datasets_async(
        exchange=LIVE_DATASET_EXCHANGE,
        data_types=[LIVE_DATASET_TYPE],
        symbols=[LIVE_DATASET_SYMBOL.lower()],
        from_date=LIVE_DATASET_FROM,
        to_date=LIVE_DATASET_TO,
        download_dir=str(tmp_path),
    )

    file_path = tmp_path / "deribit_trades_2024-01-01_BTC-PERPETUAL.csv.gz"
    assert file_path.exists()

    with gzip.open(file_path, "rt", encoding="utf-8") as file:
        header = file.readline().strip()
        first_row = file.readline().strip()

    assert header == "exchange,symbol,timestamp,local_timestamp,id,side,price,amount"
    assert first_row.startswith("deribit,BTC-PERPETUAL,")


def test_download_datasets_async_skips_existing_files(tmp_path: Path):
    existing = tmp_path / "deribit_trades_2024-01-01_BTC-PERPETUAL.csv.gz"
    existing.write_bytes(b"existing")

    download_datasets(
        exchange="deribit",
        data_types=["trades"],
        symbols=["BTC-PERPETUAL"],
        from_date="2024-01-01",
        to_date="2024-01-02",
        download_dir=str(tmp_path),
        skip_if_exists=True,
    )

    assert existing.read_bytes() == b"existing"


@pytest.mark.asyncio
async def test_download_datasets_async_prefetches_last_then_first_day(monkeypatch, tmp_path: Path):
    calls = []

    async def fake_create_session(api_key: str, timeout: int):
        return _FakeSession()

    async def fake_download_dataset_file(*, url: str, dest_path: str, **kwargs):
        calls.append((url, dest_path))
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
        Path(dest_path).write_bytes(url.encode("utf-8"))

    monkeypatch.setattr(download_datasets_module, "create_session", fake_create_session)
    monkeypatch.setattr(download_datasets_module, "download_dataset_file", fake_download_dataset_file)

    await download_datasets_async(
        exchange="deribit",
        data_types=["trades"],
        symbols=["BTC-PERPETUAL"],
        from_date="2024-01-01",
        to_date="2024-01-04",
        download_dir=str(tmp_path),
    )

    assert [call[0] for call in calls[:2]] == [
        "https://datasets.tardis.dev/v1/deribit/trades/2024/01/03/BTC-PERPETUAL.csv.gz",
        "https://datasets.tardis.dev/v1/deribit/trades/2024/01/01/BTC-PERPETUAL.csv.gz",
    ]
    assert sorted(call[0] for call in calls[2:]) == [
        "https://datasets.tardis.dev/v1/deribit/trades/2024/01/02/BTC-PERPETUAL.csv.gz"
    ]


@pytest.mark.asyncio
async def test_download_datasets_async_cancels_pending_tasks_on_error(monkeypatch, tmp_path: Path):
    cancelled_days = []

    async def fake_create_session(api_key: str, timeout: int):
        return _FakeSession()

    async def fake_download_dataset_if_needed(**kwargs):
        day = kwargs["date"].day

        if day in (1, 5):
            return

        if day == 2:
            raise RuntimeError("download failed")

        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled_days.append(day)
            raise

    monkeypatch.setattr(download_datasets_module, "create_session", fake_create_session)
    monkeypatch.setattr(download_datasets_module, "_download_dataset_if_needed", fake_download_dataset_if_needed)

    with pytest.raises(RuntimeError, match="download failed"):
        await download_datasets_async(
            exchange="deribit",
            data_types=["trades"],
            symbols=["BTC-PERPETUAL"],
            from_date="2024-01-01",
            to_date="2024-01-06",
            download_dir=str(tmp_path),
            concurrency=2,
        )

    assert cancelled_days == [3]


@pytest.mark.live
def test_download_datasets_sync_wrapper_runs_on_live_data(tmp_path: Path):
    download_datasets(
        exchange=LIVE_DATASET_EXCHANGE,
        data_types=[LIVE_DATASET_TYPE],
        symbols=[LIVE_DATASET_SYMBOL],
        from_date=LIVE_DATASET_FROM,
        to_date=LIVE_DATASET_TO,
        download_dir=str(tmp_path),
    )

    assert (tmp_path / "deribit_trades_2024-01-01_BTC-PERPETUAL.csv.gz").exists()


@pytest.mark.asyncio
async def test_download_datasets_sync_wrapper_raises_in_running_loop():
    with pytest.raises(RuntimeError, match="download_datasets_async"):
        download_datasets(
            exchange="deribit",
            data_types=["trades"],
            symbols=["BTC-PERPETUAL"],
            from_date="2024-01-01",
            to_date="2024-01-02",
        )


def _range_response(request: web.Request, body: bytes, headers: Optional[dict] = None) -> web.Response:
    start, end = map(int, re.fullmatch(r"bytes=(\d+)-(\d+)", request.headers["Range"]).groups())
    if start >= len(body):
        return web.Response(status=416)
    last_byte = min(end, len(body) - 1)
    return web.Response(
        status=206,
        body=body[start : last_byte + 1],
        headers={**(headers or {}), "Content-Range": f"bytes {start}-{last_byte}/{len(body)}"},
    )


@asynccontextmanager
async def _serve(handler):
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        yield str(server.make_url("/")).rstrip("/")
    finally:
        await server.close()


async def _download_dataset_from(endpoint: str, download_dir: Path, verify_md5: bool = False) -> bytes:
    await download_datasets_async(
        exchange=LIVE_DATASET_EXCHANGE,
        data_types=[LIVE_DATASET_TYPE],
        symbols=[LIVE_DATASET_SYMBOL],
        from_date=LIVE_DATASET_FROM,
        to_date=LIVE_DATASET_TO,
        api_key="api-key",
        endpoint=endpoint,
        download_dir=str(download_dir),
        verify_md5=verify_md5,
    )
    return (download_dir / "deribit_trades_2024-01-01_BTC-PERPETUAL.csv.gz").read_bytes()


@pytest.mark.asyncio
async def test_download_datasets_async_downloads_large_dataset_in_byte_ranges(tmp_path: Path):
    dataset = os.urandom(2 * DATASET_RANGE_SIZE + 1000)
    ranges = []

    async def handle(request: web.Request) -> web.Response:
        ranges.append(request.headers.get("Range"))
        return _range_response(request, dataset)

    async with _serve(handle) as api_url:
        assert await _download_dataset_from(f"{api_url}/v1", tmp_path) == dataset

    assert ranges[0] == f"bytes=0-{DATASET_RANGE_SIZE - 1}"
    assert sorted(ranges[1:]) == sorted(
        [
            f"bytes={DATASET_RANGE_SIZE}-{2 * DATASET_RANGE_SIZE - 1}",
            f"bytes={2 * DATASET_RANGE_SIZE}-{3 * DATASET_RANGE_SIZE - 1}",
        ]
    )


@pytest.mark.asyncio
async def test_download_datasets_async_reuses_storage_redirect_and_renews_it_after_it_expires_past_failed_renewal(
    tmp_path: Path,
):
    dataset = os.urandom(2 * DATASET_RANGE_SIZE + 1000)
    api_authorizations = []
    storage_authorizations = []

    async def handle_storage(request: web.Request) -> web.Response:
        storage_authorizations.append(request.headers.get("Authorization"))
        if request.query["signature"] == "first" and request.headers["Range"] != f"bytes=0-{DATASET_RANGE_SIZE - 1}":
            return web.Response(status=403, text="Request has expired")
        return _range_response(request, dataset)

    async with _serve(handle_storage) as storage_url:

        async def handle_api(request: web.Request) -> web.Response:
            api_authorizations.append(request.headers.get("Authorization"))
            if len(api_authorizations) == 2:
                return web.Response(status=500, text='{"code":100,"message":"Internal error"}')
            signature = "first" if len(api_authorizations) == 1 else "renewed"
            raise web.HTTPFound(f"{storage_url}/dataset?signature={signature}")

        async with _serve(handle_api) as api_url:
            assert await _download_dataset_from(f"{api_url}/v1", tmp_path) == dataset

    # The first range, then one range for the failed renewal and one for the renewal, all with the key.
    assert api_authorizations == ["Bearer api-key"] * 3
    assert set(storage_authorizations) == {None}


@pytest.mark.asyncio
async def test_download_datasets_async_fails_with_api_error_when_api_refuses_to_renew_expired_redirect(tmp_path: Path):
    dataset = os.urandom(2 * DATASET_RANGE_SIZE + 1000)
    api_requests = 0

    async def handle_storage(request: web.Request) -> web.Response:
        if request.headers["Range"] != f"bytes=0-{DATASET_RANGE_SIZE - 1}":
            return web.Response(status=403, text="Request has expired")
        return _range_response(request, dataset)

    async with _serve(handle_storage) as storage_url:

        async def handle_api(request: web.Request) -> web.Response:
            nonlocal api_requests
            api_requests += 1
            if api_requests == 1:
                raise web.HTTPFound(f"{storage_url}/dataset")
            return web.json_response({"code": 11, "message": "Invalid API key"}, status=401)

        async with _serve(handle_api) as api_url:
            with pytest.raises(urllib.error.HTTPError) as error:
                await _download_dataset_from(f"{api_url}/v1", tmp_path)

    assert error.value.code == 401
    assert api_requests == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exported_again_size",
    [2 * DATASET_RANGE_SIZE + 2000, DATASET_RANGE_SIZE + 1000, 2 * DATASET_RANGE_SIZE + 1000],
    ids=["larger", "smaller", "same-size"],
)
async def test_download_datasets_async_starts_over_when_dataset_is_exported_again_during_download(
    tmp_path: Path, exported_again_size: int
):
    exported = os.urandom(2 * DATASET_RANGE_SIZE + 1000)
    exported_again = os.urandom(exported_again_size)
    requests = 0

    async def handle(request: web.Request) -> web.Response:
        nonlocal requests
        requests += 1
        dataset = exported if requests == 1 else exported_again
        return _range_response(request, dataset, {"x-md5": f'"{hashlib.md5(dataset).hexdigest()}"'})

    async with _serve(handle) as api_url:
        assert await _download_dataset_from(f"{api_url}/v1", tmp_path) == exported_again


@pytest.mark.asyncio
async def test_download_datasets_async_with_verify_md5_downloads_again_when_ranges_do_not_match_announced_md5(
    tmp_path: Path,
):
    dataset = os.urandom(DATASET_RANGE_SIZE + 1000)
    corrupted = dataset[:DATASET_RANGE_SIZE] + os.urandom(1000)
    announced = {"x-md5": f'"{hashlib.md5(dataset).hexdigest()}"'}
    ranges = []

    async def handle(request: web.Request) -> web.Response:
        ranges.append(request.headers["Range"])
        return _range_response(request, corrupted if len(ranges) == 2 else dataset, announced)

    async with _serve(handle) as api_url:
        assert await _download_dataset_from(f"{api_url}/v1", tmp_path, verify_md5=True) == dataset

    first_range = f"bytes=0-{DATASET_RANGE_SIZE - 1}"
    second_range = f"bytes={DATASET_RANGE_SIZE}-{2 * DATASET_RANGE_SIZE - 1}"
    assert ranges == [first_range, second_range, first_range, second_range]


@pytest.mark.asyncio
async def test_download_datasets_async_with_verify_md5_keeps_dataset_whose_storage_etag_is_multipart(tmp_path: Path):
    requests = 0

    async def handle(request: web.Request) -> web.Response:
        nonlocal requests
        requests += 1
        return _range_response(request, b"dataset", {"ETag": '"0123456789abcdef0123456789abcdef-3"'})

    async with _serve(handle) as api_url:
        assert await _download_dataset_from(f"{api_url}/v1", tmp_path, verify_md5=True) == b"dataset"

    assert requests == 1


@pytest.mark.asyncio
async def test_download_datasets_async_waits_for_retry_after_when_a_range_is_rate_limited(tmp_path: Path, monkeypatch):
    delays = capture_retry_delays(monkeypatch)
    dataset = os.urandom(DATASET_RANGE_SIZE + 1000)
    requests = []

    async def handle(request: web.Request) -> web.Response:
        requests.append(f"{request.path_qs} {request.headers['Range']}")
        if len(requests) == 2:
            return web.Response(status=429, text="rate limited", headers={"Retry-After": "7"})
        return _range_response(request, dataset)

    async with _serve(handle) as api_url:
        assert await _download_dataset_from(f"{api_url}/v1", tmp_path) == dataset

    dataset_url = "/v1/deribit/trades/2024/01/01/BTC-PERPETUAL.csv.gz"
    second_range = f"bytes={DATASET_RANGE_SIZE}-{2 * DATASET_RANGE_SIZE - 1}"
    assert requests == [
        f"{dataset_url} bytes=0-{DATASET_RANGE_SIZE - 1}",
        f"{dataset_url} {second_range}",
        f"{dataset_url}?retryAttempt=1 {second_range}",
    ]
    assert delays == [7.0]
