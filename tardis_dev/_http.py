import asyncio
import logging
import os
import pathlib
import random
import secrets
import urllib.error
from typing import Optional

import aiofiles
import aiohttp


logger = logging.getLogger(__name__)


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
        timeout=aiohttp.ClientTimeout(total=timeout),
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
) -> str:
    attempts = 0

    while True:
        attempts += 1
        try:
            return await _download(
                session,
                _get_retry_url(url, attempts),
                dest_path,
                http_proxy,
                append_content_encoding_extension=append_content_encoding_extension,
            )
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
    if not isinstance(exc, urllib.error.HTTPError):
        return False

    if exc.code == 401:
        return True

    return exc.code == 400 and "ISO 8601 format" not in exc.msg


def _get_next_attempt_delay(exc: Exception, attempts: int) -> float:
    random_ingredient = random.random() * 0.5
    attempts_delay = min(2**attempts, 120)
    next_attempt_delay = random_ingredient + attempts_delay

    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 429:
            next_attempt_delay += 60
        elif exc.code == 500:
            next_attempt_delay *= 2

    return next_attempt_delay


async def _download(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: str,
    http_proxy: Optional[str],
    *,
    append_content_encoding_extension: bool,
) -> str:
    async with session.get(url, proxy=http_proxy) as response:
        if response.status != 200:
            error_text = await response.text()
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=None, fp=None)

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
            return final_path
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
