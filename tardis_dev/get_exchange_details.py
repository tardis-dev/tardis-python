import asyncio
import gzip
import json
import urllib.error
from typing import Any, Dict, Optional

from tardis_dev._http import create_session
from tardis_dev._options import DEFAULT_ENDPOINT


def get_exchange_details(
    exchange: str,
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    timeout: int = 60,
    http_proxy: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            get_exchange_details_async(
                exchange=exchange,
                endpoint=endpoint,
                timeout=timeout,
                http_proxy=http_proxy,
            )
        )

    raise RuntimeError(
        "get_exchange_details() cannot be called from a running event loop. Use get_exchange_details_async() instead."
    )


async def get_exchange_details_async(
    exchange: str,
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    timeout: int = 60,
    http_proxy: Optional[str] = None,
) -> Dict[str, Any]:
    async with await create_session("", timeout) as session:
        async with session.get(f"{endpoint}/exchanges/{exchange}", proxy=http_proxy) as response:
            body = await response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)

            if response.status != 200:
                error_text = body.decode("utf-8", errors="replace")
                raise urllib.error.HTTPError(
                    f"{endpoint}/exchanges/{exchange}",
                    code=response.status,
                    msg=error_text,
                    hdrs=None,
                    fp=None,
                )

            return json.loads(body.decode("utf-8"))
