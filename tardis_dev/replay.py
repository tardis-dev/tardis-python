import asyncio
import gzip
import hashlib
import io
import json as json_module
import logging
import os
import shutil
import urllib.parse
from datetime import datetime, timedelta, timezone
from time import time
from typing import Any, AsyncIterator, Dict, List, Literal, NamedTuple, Optional, Sequence, Union

import dateutil.parser
import zstandard

from tardis_dev._http import create_session, reliable_download
from tardis_dev._options import DEFAULT_CACHE_DIR, DEFAULT_ENDPOINT
from tardis_dev.channel import Channel


logger = logging.getLogger(__name__)

DATE_MESSAGE_SPLIT_INDEX = 28
ReplayCompression = Literal["gzip", "zstd"]
DEFAULT_DATA_FEED_SLICE_SIZE = 1


class Response(NamedTuple):
    local_timestamp: Union[datetime, bytes]
    message: Union[Any, bytes]


class CachedSlice(NamedTuple):
    path: str
    slice_size: int


class SliceDownloadResult(NamedTuple):
    slice_size: int
    suggested_slice_size: int


async def replay(
    exchange: str,
    from_date: Union[str, datetime],
    to_date: Union[str, datetime],
    filters: Optional[Sequence[Channel]] = None,
    *,
    api_key: str = "",
    cache_dir: str = DEFAULT_CACHE_DIR,
    endpoint: str = DEFAULT_ENDPOINT,
    timeout: int = 60,
    http_proxy: Optional[str] = None,
    compression: ReplayCompression = "zstd",
    decode_response: bool = True,
    with_disconnects: bool = False,
    auto_cleanup: bool = False,
    json=json_module,
) -> AsyncIterator[Optional[Response]]:
    parsed_from_date = _parse_date("from_date", from_date)
    parsed_to_date = _parse_date("to_date", to_date)
    _validate_replay_args(exchange, parsed_from_date, parsed_to_date, filters)
    normalized_filters = _normalize_filters(filters)
    filters_hash = _get_normalized_filters_hash(normalized_filters)

    parsed_from_date = parsed_from_date.replace(second=0, microsecond=0)
    parsed_to_date = parsed_to_date.replace(second=0, microsecond=0)
    current_slice_date = parsed_from_date
    cached_slices: Dict[datetime, CachedSlice] = {}
    start_time = time()
    last_message_was_disconnect = False

    logger.debug(
        "replay started for %s from %s to %s with filters %s",
        exchange,
        parsed_from_date.isoformat(),
        parsed_to_date.isoformat(),
        normalized_filters,
    )

    fetch_data_task = asyncio.create_task(
        _fetch_data_to_replay(
            exchange=exchange,
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            filters=normalized_filters,
            endpoint=endpoint,
            cache_dir=cache_dir,
            api_key=api_key,
            timeout=timeout,
            http_proxy=http_proxy,
            filters_hash=filters_hash,
            compression=compression,
            cached_slices=cached_slices,
        )
    )

    try:
        while current_slice_date < parsed_to_date:
            current_slice = None

            while current_slice is None:
                await asyncio.sleep(0)

                if fetch_data_task.done() and fetch_data_task.exception():
                    raise fetch_data_task.exception()

                current_slice = cached_slices.get(current_slice_date)
                if current_slice is None:
                    await asyncio.sleep(0.1)

            current_slice_path = current_slice.path
            for line in _iterate_slice_lines(current_slice_path):
                if len(line) <= 1:
                    if with_disconnects and not last_message_was_disconnect:
                        last_message_was_disconnect = True
                        yield None
                    continue

                last_message_was_disconnect = False

                if decode_response:
                    timestamp = datetime.fromisoformat(line[0 : DATE_MESSAGE_SPLIT_INDEX - 2].decode("utf-8"))
                    yield Response(timestamp, json.loads(line[DATE_MESSAGE_SPLIT_INDEX + 1 :]))
                else:
                    yield Response(line[0:DATE_MESSAGE_SPLIT_INDEX], line[DATE_MESSAGE_SPLIT_INDEX + 1 :])

            if auto_cleanup:
                _remove_processed_slice(current_slice_path)

            cached_slices.pop(current_slice_date, None)
            current_slice_date = current_slice_date + timedelta(minutes=current_slice.slice_size)

        await fetch_data_task
    finally:
        if fetch_data_task.done():
            await asyncio.gather(fetch_data_task, return_exceptions=True)
        else:
            fetch_data_task.cancel()
            await asyncio.gather(fetch_data_task, return_exceptions=True)

        if auto_cleanup:
            _clear_replay_cache_range(
                cache_dir=cache_dir,
                exchange=exchange,
                filters=normalized_filters,
                filters_hash=filters_hash,
                from_date=parsed_from_date,
                to_date=parsed_to_date,
            )

        logger.debug(
            "replay finished for %s from %s to %s in %.2fs",
            exchange,
            parsed_from_date.isoformat(),
            parsed_to_date.isoformat(),
            time() - start_time,
        )


async def _fetch_data_to_replay(
    *,
    exchange: str,
    from_date: datetime,
    to_date: datetime,
    filters: Optional[Sequence[Channel]],
    endpoint: str,
    cache_dir: str,
    api_key: str,
    timeout: int,
    http_proxy: Optional[str],
    filters_hash: str,
    compression: ReplayCompression = "zstd",
    cached_slices: Optional[Dict[datetime, CachedSlice]] = None,
) -> None:
    minutes_diff = int(round((to_date - from_date).total_seconds() / 60))
    concurrency_limit = 60

    if minutes_diff <= 0:
        return

    async with await create_session(api_key, timeout, "gzip" if compression == "gzip" else "zstd, gzip") as session:
        replay_cached_slices = cached_slices if cached_slices is not None else {}
        fetch_data_tasks = set()
        try:
            last_slice = await _fetch_slice_if_not_cached(
                session=session,
                endpoint=endpoint,
                cache_dir=cache_dir,
                exchange=exchange,
                from_date=from_date,
                offset=minutes_diff - 1,
                filters=filters,
                http_proxy=http_proxy,
                filters_hash=filters_hash,
                compression=compression,
                cached_slices=replay_cached_slices,
                requested_slice_size=DEFAULT_DATA_FEED_SLICE_SIZE,
                use_cache=False,
            )
            first_slice = (
                last_slice
                if minutes_diff == 1
                else await _fetch_slice_if_not_cached(
                    session=session,
                    endpoint=endpoint,
                    cache_dir=cache_dir,
                    exchange=exchange,
                    from_date=from_date,
                    offset=0,
                    filters=filters,
                    http_proxy=http_proxy,
                    filters_hash=filters_hash,
                    compression=compression,
                    cached_slices=replay_cached_slices,
                    requested_slice_size=DEFAULT_DATA_FEED_SLICE_SIZE,
                    use_cache=False,
                )
            )

            replay_slice_size = (
                DEFAULT_DATA_FEED_SLICE_SIZE
                if not filters
                else max(first_slice.suggested_slice_size, last_slice.suggested_slice_size)
            )

            offset = 1
            while offset < minutes_diff - 1:
                requested_slice_size = min(replay_slice_size, minutes_diff - 1 - offset)
                while len(fetch_data_tasks) >= concurrency_limit:
                    done, fetch_data_tasks = await asyncio.wait(fetch_data_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()

                fetch_data_tasks.add(
                    asyncio.create_task(
                        _fetch_slice_if_not_cached(
                            session=session,
                            endpoint=endpoint,
                            cache_dir=cache_dir,
                            exchange=exchange,
                            from_date=from_date,
                            offset=offset,
                            filters=filters,
                            http_proxy=http_proxy,
                            filters_hash=filters_hash,
                            compression=compression,
                            cached_slices=replay_cached_slices,
                            requested_slice_size=requested_slice_size,
                            use_cache=True,
                        )
                    )
                )
                offset += requested_slice_size

            await asyncio.gather(*fetch_data_tasks)
        except BaseException:
            for task in fetch_data_tasks:
                task.cancel()

            await asyncio.gather(*fetch_data_tasks, return_exceptions=True)
            raise


async def _fetch_slice_if_not_cached(
    *,
    session,
    endpoint: str,
    cache_dir: str,
    exchange: str,
    from_date: datetime,
    offset: int,
    filters: Optional[Sequence[Channel]],
    http_proxy: Optional[str],
    filters_hash: str,
    compression: ReplayCompression = "zstd",
    cached_slices: Optional[Dict[datetime, CachedSlice]] = None,
    requested_slice_size: int = DEFAULT_DATA_FEED_SLICE_SIZE,
    use_cache: bool = True,
) -> SliceDownloadResult:
    slice_date = from_date + timedelta(minutes=offset)
    replay_cached_slices = cached_slices if cached_slices is not None else {}
    cache_zstd_path = _get_slice_cache_path(
        cache_dir,
        exchange,
        slice_date,
        filters,
        filters_hash=filters_hash,
        content_encoding="zstd",
        slice_size=requested_slice_size,
    )
    cache_gzip_path = _get_slice_cache_path(
        cache_dir,
        exchange,
        slice_date,
        filters,
        filters_hash=filters_hash,
        slice_size=requested_slice_size,
    )

    if use_cache:
        cached_slice_path = (
            cache_zstd_path
            if os.path.isfile(cache_zstd_path)
            else cache_gzip_path if os.path.isfile(cache_gzip_path) else None
        )
        if cached_slice_path is not None:
            replay_cached_slices[slice_date] = CachedSlice(cached_slice_path, requested_slice_size)
            return SliceDownloadResult(requested_slice_size, DEFAULT_DATA_FEED_SLICE_SIZE)

    fetch_url = (
        f"{endpoint}/data-feeds/{exchange}?from={_format_replay_query_date(from_date)}"
        f"&offset={offset}&compression={compression}"
    )
    if requested_slice_size > DEFAULT_DATA_FEED_SLICE_SIZE:
        fetch_url += f"&sliceSize={requested_slice_size}"

    if filters:
        filters_serialized = json_module.dumps(_serialize_normalized_filters(filters), separators=(",", ":"))
        filters_url_encoded = urllib.parse.quote(filters_serialized, safe="~()*!.'")
        fetch_url += f"&filters={filters_url_encoded}"

    cache_base_path = cache_gzip_path.removesuffix(".gz")

    download_path, response_headers = await reliable_download(
        session=session,
        url=fetch_url,
        dest_path=cache_base_path,
        http_proxy=http_proxy,
        append_content_encoding_extension=True,
        return_headers=True,
    )
    response_slice_size = int(response_headers["x-slice-size"])
    suggested_slice_size = int(response_headers.get("x-suggested-slice-size", DEFAULT_DATA_FEED_SLICE_SIZE))
    replay_cached_slices[slice_date] = CachedSlice(download_path, response_slice_size)
    return SliceDownloadResult(response_slice_size, suggested_slice_size)


def _normalize_filters(filters: Optional[Sequence[Channel]]) -> Optional[List[Channel]]:
    if filters is None:
        return None

    optimized_filters_by_name: Dict[str, Optional[List[str]]] = {}

    for channel in filters:
        existing_symbols = optimized_filters_by_name.get(channel.name)
        current_symbols = list(channel.symbols) if channel.symbols is not None else None

        if channel.name in optimized_filters_by_name:
            if existing_symbols is not None and current_symbols is not None:
                existing_symbols.extend(current_symbols)
            elif current_symbols is not None:
                optimized_filters_by_name[channel.name] = current_symbols
        else:
            optimized_filters_by_name[channel.name] = current_symbols

    normalized_filters: List[Channel] = []
    for name in sorted(optimized_filters_by_name):
        symbols = optimized_filters_by_name[name]
        normalized_filters.append(Channel(name=name, symbols=sorted(set(symbols)) if symbols is not None else None))

    return normalized_filters


def _validate_replay_args(
    exchange: str,
    from_date: datetime,
    to_date: datetime,
    filters: Optional[Sequence[Any]],
) -> None:
    if not exchange:
        raise ValueError("Invalid 'exchange' argument. Please provide exchange name.")

    if from_date >= to_date:
        raise ValueError(
            "Invalid 'from_date' and 'to_date' arguments combination. 'to_date' must be later than 'from_date'."
        )

    if filters is None:
        return

    if not isinstance(filters, Sequence):
        raise ValueError("Invalid 'filters' argument. Please provide valid filters Channel list.")

    for channel in filters:
        if not isinstance(channel, Channel):
            raise ValueError("Invalid 'filters' argument. Please provide valid filters Channel list.")

        if channel.symbols is None:
            continue

        if isinstance(channel.symbols, (str, bytes)):
            raise ValueError(f"Invalid 'symbols[]' argument: {channel.symbols}. Please provide list of symbol strings.")

        if not isinstance(channel.symbols, Sequence) or any(not isinstance(symbol, str) for symbol in channel.symbols):
            raise ValueError(f"Invalid 'symbols[]' argument: {channel.symbols}. Please provide list of symbol strings.")


def _parse_date(name: str, value: Union[str, datetime]) -> datetime:
    if value is None:
        raise ValueError(
            f"Invalid '{name}' argument: {value}. Please provide valid ISO date string. "
            "https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat"
        )

    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = dateutil.parser.isoparse(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid '{name}' argument: {value}. Please provide valid ISO date string. "
                "https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat"
            ) from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _get_slice_cache_path(
    cache_dir: str,
    exchange: str,
    date: datetime,
    filters: Optional[Sequence[Channel]],
    *,
    filters_hash: Optional[str] = None,
    content_encoding: Optional[str] = None,
    slice_size: int = DEFAULT_DATA_FEED_SLICE_SIZE,
) -> str:
    slice_size_suffix = "" if slice_size == DEFAULT_DATA_FEED_SLICE_SIZE else f".size-{slice_size}"
    return os.path.join(
        cache_dir,
        "feeds",
        exchange,
        filters_hash if filters_hash is not None else _get_filters_hash(filters),
        f"{_format_date_to_path(date)}{slice_size_suffix}.json{'.zst' if content_encoding == 'zstd' else '.gz'}",
    )


def _get_filters_hash(filters: Optional[Sequence[Channel]]) -> str:
    return _get_normalized_filters_hash(_normalize_filters(filters))


def _get_normalized_filters_hash(filters: Optional[Sequence[Channel]]) -> str:
    filters_serialized = json_module.dumps(_serialize_normalized_filters(filters), separators=(",", ":"))
    return hashlib.sha256(filters_serialized.encode("utf-8")).hexdigest()


def _serialize_filters(filters: Optional[Sequence[Channel]]) -> List[Dict[str, Any]]:
    normalized_filters = _normalize_filters(filters)
    return _serialize_normalized_filters(normalized_filters)


def _serialize_normalized_filters(filters: Optional[Sequence[Channel]]) -> List[Dict[str, Any]]:
    normalized_filters = list(filters) if filters is not None else None
    if not normalized_filters:
        return []

    return [
        {"channel": channel.name, "symbols": list(channel.symbols) if channel.symbols is not None else None}
        for channel in normalized_filters
    ]


def _format_date_to_path(date: datetime) -> str:
    return os.path.join(
        str(date.year),
        _double_digit(date.month),
        _double_digit(date.day),
        _double_digit(date.hour),
        _double_digit(date.minute),
    )


def _format_replay_query_date(date: datetime) -> str:
    if date.tzinfo is None:
        return date.replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    return date.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _double_digit(value: int) -> str:
    return f"0{value}" if value < 10 else f"{value}"


def _remove_processed_slice(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def _iterate_slice_lines(path: str):
    if path.endswith(".zst"):
        with open(path, "rb") as compressed_file:
            with zstandard.ZstdDecompressor().stream_reader(compressed_file) as file:
                with io.BufferedReader(file) as buffered_file:
                    yield from buffered_file
        return

    with gzip.open(path, "rb") as file:
        yield from file


def _clear_replay_cache_range(
    *,
    cache_dir: str,
    exchange: str,
    filters: Optional[Sequence[Channel]],
    filters_hash: Optional[str],
    from_date: datetime,
    to_date: datetime,
) -> None:
    current_date = from_date
    replay_filters_hash = filters_hash if filters_hash is not None else _get_filters_hash(filters)

    while current_date < to_date:
        day_dir = os.path.join(
            cache_dir,
            "feeds",
            exchange,
            replay_filters_hash,
            str(current_date.year),
            _double_digit(current_date.month),
            _double_digit(current_date.day),
        )
        shutil.rmtree(day_dir, ignore_errors=True)
        _cleanup_empty_dirs(day_dir, cache_dir)
        current_date = current_date + timedelta(days=1)


def _cleanup_empty_dirs(path: str, cache_dir: str) -> None:
    current_dir = os.path.dirname(path)
    cache_root = os.path.abspath(cache_dir)

    while os.path.abspath(current_dir).startswith(cache_root) and os.path.abspath(current_dir) != cache_root:
        try:
            os.rmdir(current_dir)
        except OSError:
            break

        current_dir = os.path.dirname(current_dir)
