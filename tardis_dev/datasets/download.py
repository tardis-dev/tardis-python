import asyncio
import logging
import os
import pathlib
import random
import secrets
import urllib
from datetime import datetime, timedelta
from time import time
from typing import List

import aiofiles
import aiohttp
import dateutil.parser

logger = logging.getLogger(__name__)

default_timeout = aiohttp.ClientTimeout(total=30 * 60)


def default_file_name(exchange: str, data_type: str, date: datetime, symbol: str, format: str):
    return f"{exchange}_{data_type}_{date.strftime('%Y-%m-%d')}_{symbol}.{format}.gz"


def download(
    exchange: str,
    data_types: List[str],
    symbols: List[str],
    from_date: str,
    to_date: str,
    format: str = "csv",
    api_key: str = "",
    download_dir = "./datasets",
    download_url_base = "datasets.tardis.dev",
    get_filename = default_file_name,
    timeout = default_timeout,
    concurrency = 5,
    http_proxy = None
):

    asyncio.run(
        download_async(
            exchange, data_types, symbols, from_date, to_date, format, api_key, download_dir, get_filename, timeout, download_url_base, concurrency, http_proxy
        )
    )


async def download_async(
    exchange: str,
    data_types: List[str],
    symbols: List[str],
    from_date: str,
    to_date: str,
    format: str,
    api_key: str,
    download_dir,
    get_filename,
    timeout,
    download_url_base,
    concurrency,
    http_proxy
):
    headers = {"Authorization": f"Bearer {api_key}" if api_key else ""}

    async with aiohttp.ClientSession(auto_decompress=False, headers=headers, timeout=timeout, trust_env=True) as session:
        end_date = dateutil.parser.isoparse(to_date)

        for symbol in symbols:
            symbol = symbol.replace(":", "-").replace("/", "-").upper()

            for data_type in data_types:
                start_time = time()

                logger.debug(
                    "download started for %s %s %s from %s to %s", exchange, data_type, symbol, from_date, to_date,
                )

                fetch_csv_tasks = set()
                current_date = dateutil.parser.isoparse(from_date)
                while True:
                    if len(fetch_csv_tasks) >= concurrency:
                        # if there are going to be more pending fetch downloads than concurrency limit
                        # wait before adding another one
                        done, fetch_csv_tasks = await asyncio.wait(fetch_csv_tasks, return_when=asyncio.FIRST_COMPLETED)
                        # need to check the result that may throw if task finished with an error
                        done.pop().result()

                    url = f"https://{download_url_base}/v1/{exchange}/{data_type}/{current_date.strftime('%Y/%m/%d')}/{symbol}.{format}.gz"

                    download_path = f"{download_dir}/{get_filename(exchange,data_type,current_date,symbol,format)}"

                    fetch_csv_tasks.add(
                        asyncio.get_event_loop().create_task(_reliably_download_file(session, url, download_path, http_proxy))
                    )

                    current_date = current_date + timedelta(days=1)

                    if current_date >= end_date:
                        break

                # finally wait for the remaining fetch data download tasks
                await asyncio.gather(*fetch_csv_tasks)

                end_time = time()

                logger.debug(
                    "download finished for %s %s %s from %s to %s, total time: %s seconds",
                    exchange,
                    data_type,
                    symbol,
                    from_date,
                    to_date,
                    end_time - start_time,
                )


async def _reliably_download_file(session, url, download_path, http_proxy):
    MAX_ATTEMPTS = 5
    attempts = 0

    if os.path.exists(download_path):
        return


    while True:
        attempts = attempts + 1

        try:
            await _download(session, url, download_path, http_proxy)
            break

        except asyncio.CancelledError:
            break

        except Exception as ex:
            too_many_requests = False

            if attempts == MAX_ATTEMPTS or isinstance(ex, RuntimeError):
                raise ex

            if isinstance(ex, urllib.error.HTTPError):
                # do not retry when we've got bad or unauthorized request or enough attempts
                if ex.code == 400 or ex.code == 401:
                    raise ex
                if ex.code == 429:
                    too_many_requests = True

            attempts_delay = 2 ** attempts
            next_attempts_delay = random.random() + attempts_delay

            if too_many_requests:
                # when too many requests error received wait longer than normal
                next_attempts_delay += 3 * attempts

            logger.exception(
                "download file attempt error, next attempt delay: %is, url: %s download path: %s",
                next_attempts_delay,
                url,
                download_path,
            )

            await asyncio.sleep(next_attempts_delay)


async def _download(session, url, download_path, http_proxy):
    async with session.get(url, proxy=http_proxy) as response:
        if response.status != 200:
            error_text = await response.text()
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=None, fp=None)
        # ensure that directory where we want to download data
        pathlib.Path(download_path).parent.mkdir(parents=True, exist_ok=True)
        temp_download_path = f"{download_path}{secrets.token_hex(8)}.unconfirmed"

        try:    
            # write response stream to unconfirmed temp file
            async with aiofiles.open(temp_download_path, "wb") as temp_file:
                async for data in response.content.iter_any():
                    await temp_file.write(data)

            # rename temp file to desired name only if file has been fully and successfully saved
            # it there is an error during renaming file it means that target file aready exists
            # and we're fine as only successfully save files exist
            try:
                os.replace(temp_download_path, download_path)
            except Exception as ex:
                logger.debug("download replace error: %s", ex)
        finally:
            # cleanup temp files if still exists
            if os.path.exists(temp_download_path):
                os.remove(temp_download_path)

