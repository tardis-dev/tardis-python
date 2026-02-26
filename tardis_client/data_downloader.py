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


class _AdaptiveConcurrency:
    def __init__(self, maximum, minimum=1):
        self._limit = maximum
        self._minimum = minimum
        self._maximum = maximum
        self._last_throttle = 0.0

    def on_success(self):
        self._limit = min(self._maximum, self._limit + 1)

    def on_throttle(self):
        now = time()
        if now - self._last_throttle < 2.0:
            return
        self._last_throttle = now
        self._limit = max(self._minimum, self._limit * 7 // 10)

    @property
    def limit(self):
        return self._limit


async def fetch_data_to_replay(exchange, from_date, to_date, filters, endpoint, cache_dir, api_key, http_timeout, http_proxy):
    timeout = aiohttp.ClientTimeout(total=http_timeout)
    headers = {
        "Authorization": f"Bearer {api_key}" if api_key else "",
        "User-Agent": f"tardis-client/{tardis_client.__version__} (+https://github.com/tardis-dev/tardis-python)",
    }

    minutes_diff = int(round((to_date - from_date).total_seconds() / 60))
    offset = 0
    ac = _AdaptiveConcurrency(maximum=60)
    fetch_data_tasks = set()

    start_time = time()

    logger.debug(
        "fetch data started for '%s' exchange from: %s, to: %s, filters: %s",
        exchange,
        from_date.isoformat(),
        to_date.isoformat(),
        filters,
    )

    async with aiohttp.ClientSession(auto_decompress=False, timeout=timeout, headers=headers, trust_env=True) as session:
        try:
            # loop below will fetch data slices if not cached already concurrently up to the adaptive limit
            while offset < minutes_diff:
                while len(fetch_data_tasks) >= ac.limit:
                    # drain until in-flight count is below the current adaptive limit
                    done, fetch_data_tasks = await asyncio.wait(fetch_data_tasks, return_when=asyncio.FIRST_COMPLETED)
                    # retrieve all results so no "exception was never retrieved" warnings
                    first_error = None
                    for task in done:
                        try:
                            task.result()
                        except Exception as ex:
                            if first_error is None:
                                first_error = ex
                    ac.on_success()
                    if first_error is not None:
                        raise first_error

                fetch_data_tasks.add(
                    asyncio.create_task(
                        _fetch_data_if_not_cached(session, endpoint, cache_dir, exchange, from_date, offset, filters, http_proxy, ac)
                    )
                )
                offset += 1

            # finally wait for the remaining fetch data download tasks
            await asyncio.gather(*fetch_data_tasks)
        except BaseException:
            # cancel all pending tasks so they don't keep making requests
            for task in fetch_data_tasks:
                task.cancel()
            # await them to suppress "Task was destroyed but it is pending" warnings
            await asyncio.gather(*fetch_data_tasks, return_exceptions=True)
            raise

        end_time = time()

    logger.debug(
        "fetch data finished for '%s' exchange - from: %s, to: %s, filters: %s, total time: %s seconds",
        exchange,
        from_date.isoformat(),
        to_date.isoformat(),
        filters,
        end_time - start_time,
    )


async def _fetch_data_if_not_cached(session, endpoint, cache_dir, exchange, from_date, offset, filters, http_proxy, ac):
    slice_date = from_date + timedelta(seconds=offset * 60)
    cache_path = get_slice_cache_path(cache_dir, exchange, slice_date, filters)

    # fetch and cache slice only if it's not cached already
    if os.path.isfile(cache_path) == False:
        await _reliably_fetch_and_cache_slice(session, endpoint, exchange, from_date, offset, filters, cache_path, http_proxy, ac)
        logger.debug("fetched data slice for date %s from the API and cached - %s", slice_date, cache_path)
    else:
        logger.debug("data slice for date %s already in local cache - %s", slice_date, cache_path)


async def _reliably_fetch_and_cache_slice(session, endpoint, exchange, from_date, offset, filters, cache_path, http_proxy, ac):
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
            await _fetch_and_cache_slice(session, url=fetch_url, cache_path=cache_path, http_proxy=http_proxy)
            break

        except asyncio.CancelledError:
            break

        except Exception as ex:
            if attempts == MAX_ATTEMPTS or isinstance(ex, RuntimeError):
                raise ex

            if isinstance(ex, urllib.error.HTTPError):
                # do not retry when we've got bad or unauthorized request or enough attempts
                if (ex.code == 400 and 'ISO 8601 format' not in ex.msg) or ex.code == 401:
                    raise ex
                if ex.code == 429:
                    too_many_requests = True
                    ac.on_throttle()

            random_ingridient = random.random()
            attempts_delay = 2 ** attempts
            next_attempts_delay = random_ingridient + attempts_delay

            if too_many_requests:
                # when too many requests error received wait longer than normal
                next_attempts_delay = 61
            logger.debug(
                "_fetch_and_cache_slice error: %s, next attempt delay: %is, path: %s", ex, next_attempts_delay, cache_path
            )

            await asyncio.sleep(next_attempts_delay)


async def _fetch_and_cache_slice(session, url, cache_path, http_proxy):
    async with session.get(url,proxy=http_proxy) as response:
        if response.status != 200:
            error_text = await response.text()
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=None, fp=None)

        # ensure that directory where we want to cache data slice exists
        pathlib.Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
        temp_cache_path = f"{cache_path}{secrets.token_hex(8)}.unconfirmed"
        try:
            # write response stream to unconfirmed temp file
            async with aiofiles.open(temp_cache_path, "wb") as temp_file:
                async for data in response.content.iter_any():
                    await temp_file.write(data)

            # atomically replace temp file with the final cache path
            try:
                os.replace(temp_cache_path, cache_path)
            except OSError as ex:
                # if another task already wrote this file, that's fine
                if os.path.isfile(cache_path):
                    logger.debug("_fetch_and_cache_slice rename skipped, file already exists: %s", ex)
                else:
                    raise
        finally:
            # cleanup partial temp file on cancellation or error
            if os.path.exists(temp_cache_path):
                os.remove(temp_cache_path)