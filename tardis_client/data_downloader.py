import os
import logging
import urllib
import urllib.parse
import pathlib
import asyncio
import aiohttp
import aiofiles
import secrets
import random
import json
import tardis_client

from datetime import datetime, timedelta
from time import time
from tardis_client.handy import get_slice_cache_path


logger = logging.getLogger(__name__)


async def fetch_data_to_replay(exchange, from_date, to_date, filters, endpoint, cache_dir, api_key):
    timeout = aiohttp.ClientTimeout(total=60)
    headers = {
        "Authorization": f"Bearer {api_key}" if api_key else "",
        "User-Agent": f"tardis-client/{tardis_client.__version__} (+https://github.com/tardis-dev/tardis-python)",
    }

    minutes_diff = int(round((to_date - from_date).total_seconds() / 60))
    offset = 0
    FET_DATA_CONCURRENCY_LIMIT = 60
    fetch_data_tasks = set()

    start_time = time()

    logger.debug(
        "fetch data started for '%s' exchange from: %s, to: %s, filters: %s",
        exchange,
        from_date.isoformat(),
        to_date.isoformat(),
        filters,
    )

    async with aiohttp.ClientSession(auto_decompress=False, timeout=timeout, headers=headers) as session:
        # loop below will fetch data slices if not cached already concurrently up to the conecurrency limit
        while offset < minutes_diff:
            if len(fetch_data_tasks) >= FET_DATA_CONCURRENCY_LIMIT:
                # if there are going to be more pending fetch data downloads than concurrency limit
                # wait before adding another one
                done, fetch_data_tasks = await asyncio.wait(fetch_data_tasks, return_when=asyncio.FIRST_COMPLETED)
                # need to check the result that may throw if task finished with an error
                done.pop().result()

            fetch_data_tasks.add(
                asyncio.create_task(
                    _fetch_data_if_not_cached(session, endpoint, cache_dir, exchange, from_date, offset, filters)
                )
            )
            offset += 1

        # finally wait for the remaining fetch data download tasks
        await asyncio.gather(*fetch_data_tasks)

        end_time = time()

    logger.debug(
        "fetch data finished for '%s' exchange - from: %s, to: %s, filters: %s, total time: %s seconds",
        exchange,
        from_date.isoformat(),
        to_date.isoformat(),
        filters,
        end_time - start_time,
    )


async def _fetch_data_if_not_cached(session, endpoint, cache_dir, exchange, from_date, offset, filters):
    slice_date = from_date + timedelta(seconds=offset * 60)
    cache_path = get_slice_cache_path(cache_dir, exchange, slice_date, filters)

    # fetch and cache slice only if it's not cached already
    if os.path.isfile(cache_path) == False:
        await _reliably_fetch_and_cache_slice(session, endpoint, exchange, from_date, offset, filters, cache_path)
        logger.debug("fetched data slice for date %s from the API and cached - %s", slice_date, cache_path)
    else:
        logger.debug("data slice for date %s already in local cache - %s", slice_date, cache_path)


async def _reliably_fetch_and_cache_slice(session, endpoint, exchange, from_date, offset, filters, cache_path):
    fetch_url = f"{endpoint}/v1/data-feeds/{exchange}?from={from_date.isoformat()}&offset={offset}"

    if filters is not None and len(filters) > 0:
        # convert fitlers to dictionary so can be json serialized (use names required by the API: channel, symbols)
        filters_serializable = [{"channel": filter.name, "symbols": filter.symbols} for filter in filters]
        filters_serialized = json.dumps(filters_serializable, separators=(",", ":"))
        filters_url_encoded = urllib.parse.quote(filters_serialized, safe="~()*!.'")
        fetch_url += f"&filters={filters_url_encoded}"

    MAX_ATTEMPTS = 5
    attempts = 0

    while True:
        attempts += 1
        too_many_requests = False
        try:
            await _fetch_and_cache_slice(session, url=fetch_url, cache_path=cache_path)
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
                if ex.code == 429:
                    too_many_requests = True

            random_ingridient = random.random()
            attempts_delay = 2 ** attempts
            next_attempts_delay = random_ingridient + attempts_delay

            if too_many_requests:
                # when too many requests error received wait longer than normal
                next_attempts_delay += 3 * attempts
            logger.debug(
                "_fetch_and_cache_slice error: %s, next attempt delay: %is, path: %s", ex, next_attempts_delay, cache_path
            )

            await asyncio.sleep(next_attempts_delay)


async def _fetch_and_cache_slice(session, url, cache_path):
    async with session.get(url) as response:
        if response.status != 200:
            error_text = await response.text()
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=None, fp=None)

        # ensure that directory where we want to cache data slice exists
        pathlib.Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
        temp_cache_path = f"{cache_path}{secrets.token_hex(8)}.unconfirmed"
        # write response stream to unconfirmed temp file
        async with aiofiles.open(temp_cache_path, "wb") as temp_file:
            async for data in response.content.iter_any():
                await temp_file.write(data)

        # rename temp file to desired name only if file has been fully and successfully saved
        # it there is an error during renaming file it means that target file aready exists
        # and we're fine as only successfully save files exist
        try:
            os.rename(temp_cache_path, cache_path)
        except Exception as ex:
            logger.debug("_fetch_and_cache_slice rename error: %s", ex)
