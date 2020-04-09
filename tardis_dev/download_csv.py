import asyncio
import logging
import os
import pathlib
import random
import secrets
import urllib
from datetime import datetime, timedelta
from time import time

import aiofiles
import aiohttp

logger = logging.getLogger(__name__)
CONCURRENCY_LIMIT = 20


def default_file_name(exchange: str, data_type: str, date: datetime, symbol: str, auto_decompress: bool):
    suffix = "csv" if auto_decompress else "csv.gz"
    return f"{exchange}_{data_type}_{date.strftime('%Y-%m-%d')}_{symbol}.{suffix}"


async def download_csv(
    exchange: str,
    data_type: str,
    symbol: str,
    from_date: str,
    to_date: str,
    api_key: str = "",
    download_dir="./datasets",
    get_filename=default_file_name,
    auto_decompress=False,
):
    start_time = time()

    logger.debug(
        "csv download started for %s %s %s from %s to %s", exchange, data_type, symbol, from_date, to_date,
    )

    headers = {"Authorization": f"Bearer {api_key}" if api_key else ""}

    async with aiohttp.ClientSession(auto_decompress=auto_decompress, headers=headers) as session:
        current_date = datetime.fromisoformat(from_date)
        end_date = datetime.fromisoformat(to_date)

        fetch_csv_tasks = set()

        while True:
            if len(fetch_csv_tasks) >= CONCURRENCY_LIMIT:
                # if there are going to be more pending fetch downloads than concurrency limit
                # wait before adding another one
                done, fetch_data_tasks = await asyncio.wait(fetch_csv_tasks, return_when=asyncio.FIRST_COMPLETED)
                # need to check the result that may throw if task finished with an error
                done.pop().result()

            csv_url = (
                f"https://csv.tardis.dev/datasets/v1/{exchange}/{data_type}/{current_date.strftime('%Y/%m/%d')}/{symbol}"
            )

            download_path = f"{download_dir}/{get_filename(exchange,data_type,current_date,symbol,auto_decompress)}"

            fetch_csv_tasks.add(asyncio.create_task(_reliably_download_file(session, csv_url, download_path)))

            current_date = current_date + timedelta(days=1)

            if current_date >= end_date:
                break

        # finally wait for the remaining fetch data download tasks
        await asyncio.gather(*fetch_csv_tasks)

        end_time = time()

    logger.debug(
        "csv download finished for %s %s %s from %s to %s, total time: %s seconds",
        exchange,
        data_type,
        symbol,
        from_date,
        to_date,
        end_time - start_time,
    )


async def _reliably_download_file(session, url, download_path):
    MAX_ATTEMPTS = 5
    attempts = 0

    while True:
        attempts = attempts + 1
        try:
            await _download(session, url, download_path)
            break

        except asyncio.CancelledError:
            break

        except Exception as ex:
            if attempts == MAX_ATTEMPTS or isinstance(ex, RuntimeError):
                raise ex

            if isinstance(ex, urllib.error.HTTPError):
                # do not retry when we've got bad or unauthorized request or enough attempts
                if ex.code == 400 or ex.code == 401:
                    raise ex

            attempts_delay = 2 ** attempts
            next_attempts_delay = random.random() + attempts_delay

            logger.debug(
                "download file attempt error: %s, next attempt delay: %is, url: %s download path: %s",
                ex,
                next_attempts_delay,
                url,
                download_path,
            )

            await asyncio.sleep(next_attempts_delay)


async def _download(session, url, download_path):
    logger.debug("downloading %s to %s", url, download_path)

    async with session.get(url) as response:
        if response.status != 200:
            error_text = await response.text()
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=None, fp=None)

        # ensure that directory where we want to download data
        pathlib.Path(download_path).parent.mkdir(parents=True, exist_ok=True)
        temp_download_path = f"{download_path}{secrets.token_hex(8)}.unconfirmed"
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
            logger.debug("download rename error: %s", ex)
