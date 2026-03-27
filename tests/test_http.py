import urllib.error
from pathlib import Path

import pytest
from aioresponses import aioresponses

from tardis_dev._http import create_session, reliable_download


@pytest.mark.asyncio
async def test_create_session_omits_authorization_header_when_api_key_missing():
    async with await create_session("", 5) as session:
        assert "Authorization" not in session.headers


@pytest.mark.asyncio
async def test_reliable_download_uses_node_backoff_for_500(tmp_path: Path, monkeypatch):
    destination = tmp_path / "slice.json.gz"
    url = "https://example.com/data"
    delays = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        return None

    monkeypatch.setattr("tardis_dev._http.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("tardis_dev._http.random.random", lambda: 0.0)

    with aioresponses() as mocked:
        mocked.get(url, status=500)
        mocked.get(url, body=b"payload")

        async with await create_session("", 5) as session:
            await reliable_download(session, url, str(destination))

    assert destination.read_bytes() == b"payload"
    assert delays == [4.0]


@pytest.mark.asyncio
async def test_reliable_download_retries_iso_400_for_gz_with_retry_attempt_query(tmp_path: Path, monkeypatch):
    destination = tmp_path / "dataset.csv.gz"
    url = "https://example.com/dataset.csv.gz"
    delays = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        return None

    monkeypatch.setattr("tardis_dev._http.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("tardis_dev._http.random.random", lambda: 0.0)

    with aioresponses() as mocked:
        mocked.get(url, status=400, body="Please provide valid ISO 8601 format.")
        mocked.get(f"{url}?retryAttempt=1", body=b"payload")

        async with await create_session("", 5) as session:
            await reliable_download(session, url, str(destination))

    assert destination.read_bytes() == b"payload"
    assert delays == [2.0]


@pytest.mark.asyncio
async def test_reliable_download_does_not_retry_400(tmp_path: Path, monkeypatch):
    destination = tmp_path / "slice.json.gz"
    url = "https://example.com/data"

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("tardis_dev._http.asyncio.sleep", no_sleep)

    with aioresponses() as mocked:
        mocked.get(url, status=400, body="bad request")

        async with await create_session("", 5) as session:
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                await reliable_download(session, url, str(destination))

    assert exc_info.value.code == 400
    assert not destination.exists()


@pytest.mark.asyncio
async def test_reliable_download_uses_429_delay(tmp_path: Path, monkeypatch):
    destination = tmp_path / "slice.json.gz"
    url = "https://example.com/data"
    delays = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        return None

    monkeypatch.setattr("tardis_dev._http.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("tardis_dev._http.random.random", lambda: 0.0)

    with aioresponses() as mocked:
        mocked.get(url, status=429, body="rate limited")
        mocked.get(url, body=b"payload")

        async with await create_session("", 5) as session:
            await reliable_download(session, url, str(destination))

    assert destination.read_bytes() == b"payload"
    assert delays == [62.0]


@pytest.mark.asyncio
async def test_reliable_download_cleans_temp_file_on_replace_error(tmp_path: Path, monkeypatch):
    destination = tmp_path / "slice.json.gz"
    url = "https://example.com/data"

    def raising_replace(src: str, dst: str) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr("tardis_dev._http.os.replace", raising_replace)

    with aioresponses() as mocked:
        mocked.get(url, body=b"payload")

        async with await create_session("", 5) as session:
            with pytest.raises(OSError):
                await reliable_download(session, url, str(destination), max_attempts=1)

    temp_files = list(tmp_path.glob("*.unconfirmed"))
    assert not destination.exists()
    assert temp_files == []
