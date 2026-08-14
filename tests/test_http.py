import asyncio
import urllib.error
from pathlib import Path

import aiohttp
import pytest
from aioresponses import aioresponses

from tardis_dev._http import create_session, reliable_download


@pytest.mark.asyncio
async def test_create_session_uses_requested_accept_encoding_and_omits_authorization_header_when_api_key_missing():
    async with await create_session("", 5, "zstd, gzip") as session:
        assert "Authorization" not in session.headers
        assert session.headers["Accept-Encoding"] == "zstd, gzip"


@pytest.mark.asyncio
async def test_reliable_download_appends_zstd_extension_for_replay_cache(tmp_path: Path):
    destination = tmp_path / "slice.json"
    url = "https://example.com/data"

    with aioresponses() as mocked:
        mocked.get(url, body=b"payload", headers={"Content-Encoding": "zstd"})

        async with await create_session("", 5) as session:
            final_path = await reliable_download(session, url, str(destination), append_content_encoding_extension=True)

    assert final_path.endswith(".zst")
    assert Path(final_path).read_bytes() == b"payload"
    assert not destination.exists()


@pytest.mark.asyncio
async def test_reliable_download_falls_back_to_gzip_extension_when_content_encoding_is_missing(tmp_path: Path):
    destination = tmp_path / "slice.json"
    url = "https://example.com/data"

    with aioresponses() as mocked:
        mocked.get(url, body=b"")

        async with await create_session("", 5) as session:
            final_path = await reliable_download(session, url, str(destination), append_content_encoding_extension=True)

    assert final_path.endswith(".gz")
    assert Path(final_path).read_bytes() == b""
    assert not destination.exists()


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
async def test_reliable_download_does_not_cache_response_shorter_than_content_length(tmp_path: Path):
    async def send_truncated_response(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Encoding: zstd\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabc")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(send_truncated_response, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    destination = tmp_path / "slice.json"

    try:
        async with await create_session("", 5) as session:
            with pytest.raises(aiohttp.ClientPayloadError):
                await reliable_download(
                    session,
                    f"http://127.0.0.1:{port}/data",
                    str(destination),
                    max_attempts=1,
                    append_content_encoding_extension=True,
                )
    finally:
        server.close()
        await server.wait_closed()

    assert not destination.with_suffix(".json.zst").exists()
    assert list(tmp_path.glob("*.unconfirmed")) == []


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
@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
async def test_reliable_download_does_not_retry_non_retryable_http_errors(tmp_path: Path, monkeypatch, status_code: int):
    destination = tmp_path / "slice.json.gz"
    url = "https://example.com/data"

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("tardis_dev._http.asyncio.sleep", no_sleep)

    with aioresponses() as mocked:
        mocked.get(url, status=status_code, body="non retryable")

        async with await create_session("", 5) as session:
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                await reliable_download(session, url, str(destination))

    assert exc_info.value.code == status_code
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
