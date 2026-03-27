import asyncio
import importlib
import gzip
from pathlib import Path

import pytest

from tardis_dev import default_file_name, download_datasets, download_datasets_async

download_datasets_module = importlib.import_module("tardis_dev.download_datasets")

LIVE_DATASET_EXCHANGE = "deribit"
LIVE_DATASET_TYPE = "trades"
LIVE_DATASET_SYMBOL = "BTC-PERPETUAL"
LIVE_DATASET_FROM = "2024-01-01"
LIVE_DATASET_TO = "2024-01-02"


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

    async def fake_reliable_download(*, session, url: str, dest_path: str, http_proxy=None, **kwargs):
        calls.append((url, dest_path))
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
        Path(dest_path).write_bytes(url.encode("utf-8"))

    monkeypatch.setattr(download_datasets_module, "create_session", fake_create_session)
    monkeypatch.setattr(download_datasets_module, "reliable_download", fake_reliable_download)

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
