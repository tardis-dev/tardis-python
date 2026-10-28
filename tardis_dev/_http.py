import asyncio
import hashlib
import logging
import os
import pathlib
import random
import re
import secrets
import urllib.error
from typing import Optional

import aiofiles
import aiohttp

logger = logging.getLogger(__name__)

DATASET_RANGE_SIZE = 64 * 1024 * 1024
# A dataset is rarely exported again while it downloads, so repeated mismatches mean another restart won't help.
MAX_DATASET_RESTARTS = 3


class _DatasetMismatchError(Exception):
    # The downloaded ranges don't make up one dataset file, because it was exported again during the download or doesn't
    # match its announced MD5, so the download starts over.
    pass


async def create_session(api_key: str, timeout: int, accept_encoding: str = "gzip") -> aiohttp.ClientSession:
    from tardis_dev import __version__

    headers = {
        "Accept-Encoding": accept_encoding,
        "User-Agent": f"tardis-dev/{__version__} (+https://github.com/tardis-dev/tardis-python)",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    return aiohttp.ClientSession(
        auto_decompress=False,
        # Time out on silence, not on total duration, so slow-to-prepare slices and large downloads are not cut off.
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout),
        headers=headers,
        trust_env=True,
    )


async def reliable_download(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: str,
    http_proxy: Optional[str] = None,
    max_attempts: int = 30,
    append_content_encoding_extension: bool = False,
    return_headers: bool = False,
):
    return await _with_download_retries(
        url,
        dest_path,
        max_attempts,
        lambda attempt_url: _download(
            session,
            attempt_url,
            dest_path,
            http_proxy,
            append_content_encoding_extension=append_content_encoding_extension,
            return_headers=return_headers,
        ),
    )


async def _with_download_retries(url: str, dest_path: str, max_attempts: int, attempt):
    attempts = 0

    while True:
        attempts += 1
        try:
            return await attempt(_get_retry_url(url, attempts))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_non_retryable_download_error(exc) or attempts >= max_attempts:
                raise

            next_attempt_delay = _get_next_attempt_delay(exc, attempts)

            logger.debug(
                "download attempt failed for %s, retrying in %.2fs: %s",
                dest_path,
                next_attempt_delay,
                exc,
            )
            await asyncio.sleep(next_attempt_delay)


def _get_retry_url(url: str, attempts: int) -> str:
    if attempts > 1 and url.endswith("gz"):
        return f"{url}?retryAttempt={attempts - 1}"

    return url


def _is_non_retryable_download_error(exc: Exception) -> bool:
    if isinstance(exc, _DatasetMismatchError):
        return True

    if not isinstance(exc, urllib.error.HTTPError):
        return False

    if exc.code in (401, 403, 404, 416):
        return True

    if exc.code == 429:
        # A Retry-After longer than a few minutes means an exhausted quota, so report it now instead of retrying.
        retry_after = _get_retry_after_seconds(exc)
        return retry_after is not None and retry_after > 300

    return exc.code == 400 and "ISO 8601 format" not in exc.msg


def _get_next_attempt_delay(exc: Exception, attempts: int) -> float:
    random_ingredient = random.random() * 0.5
    attempts_delay = min(2**attempts, 120)
    next_attempt_delay = random_ingredient + attempts_delay

    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 429:
            retry_after = _get_retry_after_seconds(exc)
            if retry_after is not None:
                return retry_after + random.random()
            next_attempt_delay += 60
        elif exc.code == 500:
            next_attempt_delay *= 2

    return next_attempt_delay


def _get_retry_after_seconds(exc: urllib.error.HTTPError) -> Optional[int]:
    # The API sends Retry-After as whole seconds.
    retry_after = exc.headers.get("Retry-After", "")
    return int(retry_after) if retry_after.isdigit() else None


async def _download(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: str,
    http_proxy: Optional[str],
    *,
    append_content_encoding_extension: bool,
    return_headers: bool,
):
    async with session.get(url, proxy=http_proxy) as response:
        if response.status != 200:
            error_text = await response.text()
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=response.headers, fp=None)

        headers = {key.lower(): value for key, value in response.headers.items()}
        final_path = dest_path
        if append_content_encoding_extension:
            content_encoding = response.headers.get("Content-Encoding")
            if content_encoding == "zstd":
                final_path = f"{dest_path}.zst"
            elif content_encoding is None or content_encoding == "gzip":
                final_path = f"{dest_path}.gz"
            else:
                raise urllib.error.HTTPError(
                    url,
                    code=400,
                    msg=f"Unsupported data feed content encoding: {content_encoding}",
                    hdrs=None,
                    fp=None,
                )

        pathlib.Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
        temp_path = f"{dest_path}{secrets.token_hex(8)}.unconfirmed"

        try:
            async with aiofiles.open(temp_path, "wb") as temp_file:
                async for chunk in response.content.iter_any():
                    await temp_file.write(chunk)

            os.replace(temp_path, final_path)
            if return_headers:
                return final_path, headers
            return final_path
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


async def download_dataset_file(
    session: aiohttp.ClientSession,
    storage_session: aiohttp.ClientSession,
    url: str,
    dest_path: str,
    request_slots: asyncio.Semaphore,
    http_proxy: Optional[str] = None,
    max_attempts: int = 30,
    verify_md5: bool = False,
):
    """Downloads byte ranges in parallel into one temp file, and a failed range retries alone."""
    pathlib.Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
    temp_path = f"{dest_path}{secrets.token_hex(8)}.unconfirmed"

    try:
        for restarts in range(MAX_DATASET_RESTARTS + 1):
            try:
                announced_md5 = await _download_dataset_ranges(
                    session, storage_session, url, temp_path, request_slots, http_proxy, max_attempts
                )
                if verify_md5:
                    await asyncio.to_thread(_check_dataset_md5, temp_path, announced_md5, url)
                break
            except _DatasetMismatchError:
                if restarts == MAX_DATASET_RESTARTS:
                    raise

        os.replace(temp_path, dest_path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


async def _download_dataset_ranges(
    session: aiohttp.ClientSession,
    storage_session: aiohttp.ClientSession,
    url: str,
    temp_path: str,
    request_slots: asyncio.Semaphore,
    http_proxy: Optional[str],
    max_attempts: int,
):
    async def download_first_range(attempt_url: str):
        async with request_slots:
            return await _download_dataset_range(session, attempt_url, temp_path, 0, http_proxy)

    dataset_size, storage_url, announced_md5 = await _with_download_retries(
        url, temp_path, max_attempts, download_first_range
    )
    if dataset_size is None or dataset_size <= DATASET_RANGE_SIZE:
        return announced_md5

    # A low-cost account gets a storage redirect that stays valid for two hours, and every redirect counts the whole
    # dataset toward its transfer limit, so the remaining ranges reuse it and only an expired one is renewed.
    renewal: Optional[asyncio.Task] = None

    async def renew_storage_url(attempt_url: str, start: int):
        nonlocal storage_url, renewal
        try:
            renewed_range = await _download_dataset_range(
                session, attempt_url, temp_path, start, http_proxy, dataset_size, announced_md5
            )
            _, storage_url, _ = renewed_range
            return renewed_range
        finally:
            renewal = None

    async def download_remaining_range(attempt_url: str, start: int):
        nonlocal renewal
        async with request_slots:
            # Read the URL only once the range has a slot, so a range that waited long uses the newest URL.
            range_url = storage_url
            if range_url is None:
                return await _download_dataset_range(
                    session, attempt_url, temp_path, start, http_proxy, dataset_size, announced_md5
                )

            try:
                return await _download_dataset_range(
                    storage_session, range_url, temp_path, start, http_proxy, dataset_size, announced_md5
                )
            except urllib.error.HTTPError as exc:
                if exc.code != 403:
                    raise
                # The first range to find the URL expired downloads through the API again, like the first range, and
                # the redirect it gets replaces the URL. The others wait for it and retry with the new URL, or fail
                # this attempt with its error.
                if renewal is None and storage_url == range_url:
                    renewal = asyncio.create_task(renew_storage_url(attempt_url, start))
                    return await renewal
                if renewal is not None:
                    await renewal
                raise RuntimeError("Dataset redirect expired") from exc

    tasks = [
        asyncio.create_task(
            _with_download_retries(
                url, temp_path, max_attempts, lambda attempt_url, start=start: download_remaining_range(attempt_url, start)
            )
        )
        for start in range(DATASET_RANGE_SIZE, dataset_size, DATASET_RANGE_SIZE)
    ]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        # Wait until the cancelled ranges stop, so a restart or cleanup never overlaps a range still writing.
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    return announced_md5


async def _download_dataset_range(
    session: aiohttp.ClientSession,
    url: str,
    temp_path: str,
    start: int,
    http_proxy: Optional[str],
    expected_dataset_size: Optional[int] = None,
    expected_md5: Optional[str] = None,
):
    end = start + DATASET_RANGE_SIZE - 1
    async with session.get(url, proxy=http_proxy, headers={"Range": f"bytes={start}-{end}"}) as response:
        # A later range that comes back whole or out of bounds belongs to a dataset exported again with another size.
        if start > 0 and response.status in (200, 416):
            raise _DatasetMismatchError(f"Dataset changed during download: bytes={start}-{end} returned {response.status}")

        # 200 means the storage ignored the range and sends the whole dataset.
        if response.status not in (200, 206):
            error_text = await response.text()
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=response.headers, fp=None)

        dataset_size = None
        if response.status == 206:
            content_range = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
            if content_range is not None:
                dataset_size = int(content_range[3])
            if (
                content_range is None
                or int(content_range[1]) != start
                or int(content_range[2]) != min(end, dataset_size - 1)
            ):
                raise ValueError(
                    f"Unexpected Content-Range for bytes={start}-{end}: {response.headers.get('Content-Range')}"
                )
            if expected_dataset_size is not None and dataset_size != expected_dataset_size:
                raise _DatasetMismatchError(
                    f"Dataset size changed from {expected_dataset_size} to {dataset_size} bytes during download"
                )

        # A dataset exported again with the same size announces another MD5.
        md5 = _get_announced_md5(response.headers)
        if expected_md5 is not None and md5 is not None and md5 != expected_md5:
            raise _DatasetMismatchError(f"Dataset MD5 changed from {expected_md5} to {md5} during download")

        async with aiofiles.open(temp_path, "wb" if start == 0 else "r+b") as temp_file:
            await temp_file.seek(start)
            async for chunk in response.content.iter_any():
                await temp_file.write(chunk)

        return dataset_size, str(response.url) if response.history else None, md5


def _check_dataset_md5(temp_path: str, announced_md5: Optional[str], url: str):
    # Ranges are written in parallel, so the MD5 is computed from the finished file.
    if announced_md5 is None:
        return

    with open(temp_path, "rb") as temp_file:
        actual_md5 = hashlib.file_digest(temp_file, "md5").hexdigest()
    if actual_md5 != announced_md5:
        raise _DatasetMismatchError(f"MD5 mismatch for {url}: expected {announced_md5}, got {actual_md5}")


def _get_announced_md5(headers) -> Optional[str]:
    # The API sends x-md5, while a redirect to the storage returns the storage's x-amz-meta-md5 and ETag.
    # A multipart ETag ("<hash>-<parts>") is not the file MD5, so such a file is saved without a check.
    md5 = next((headers[name] for name in ("x-md5", "x-amz-meta-md5", "etag") if name in headers), "")
    md5 = md5.replace('"', "").lower()
    return md5 if re.fullmatch(r"[0-9a-f]{32}", md5) else None
