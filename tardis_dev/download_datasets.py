import asyncio
import logging
import os
import re
import urllib.parse
from datetime import datetime, timedelta
from time import time
from typing import Callable, Literal, Optional, Sequence

import dateutil.parser

from tardis_dev._http import create_session, reliable_download
from tardis_dev._options import DEFAULT_DATASETS_ENDPOINT


logger = logging.getLogger(__name__)


def default_file_name(exchange: str, data_type: str, date: datetime, symbol: str, format: str) -> str:
    sanitized_symbol = re.sub(r'[:\\/?*<>|"]', "-", symbol)
    return f"{exchange}_{data_type}_{date.strftime('%Y-%m-%d')}_{sanitized_symbol}.{format}.gz"


def download_datasets(
    exchange: str,
    data_types: Sequence[str],
    symbols: Sequence[str],
    from_date: str,
    to_date: str,
    *,
    api_key: str = "",
    download_dir: str = "./datasets",
    endpoint: str = DEFAULT_DATASETS_ENDPOINT,
    timeout: int = 30 * 60,
    http_proxy: Optional[str] = None,
    format: Literal["csv"] = "csv",
    concurrency: int = 20,
    get_filename: Callable[[str, str, datetime, str, str], str] = default_file_name,
    skip_if_exists: bool = True,
) -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(
            download_datasets_async(
                exchange=exchange,
                data_types=data_types,
                symbols=symbols,
                from_date=from_date,
                to_date=to_date,
                api_key=api_key,
                download_dir=download_dir,
                endpoint=endpoint,
                timeout=timeout,
                http_proxy=http_proxy,
                format=format,
                concurrency=concurrency,
                get_filename=get_filename,
                skip_if_exists=skip_if_exists,
            )
        )
        return

    raise RuntimeError(
        "download_datasets() cannot be called from a running event loop. Use download_datasets_async() instead."
    )


async def download_datasets_async(
    exchange: str,
    data_types: Sequence[str],
    symbols: Sequence[str],
    from_date: str,
    to_date: str,
    *,
    api_key: str = "",
    download_dir: str = "./datasets",
    endpoint: str = DEFAULT_DATASETS_ENDPOINT,
    timeout: int = 30 * 60,
    http_proxy: Optional[str] = None,
    format: Literal["csv"] = "csv",
    concurrency: int = 20,
    get_filename: Callable[[str, str, datetime, str, str], str] = default_file_name,
    skip_if_exists: bool = True,
) -> None:
    if concurrency < 1:
        raise ValueError("Invalid 'concurrency' argument. Please provide value greater than 0.")

    start_date = _parse_date("from_date", from_date)
    end_date = _parse_date("to_date", to_date)
    days_count_to_fetch = int((end_date - start_date).total_seconds() // timedelta(days=1).total_seconds())
    if days_count_to_fetch < 1:
        raise ValueError(
            "Invalid 'from_date' and 'to_date' arguments combination. 'to_date' must be later than 'from_date'."
        )

    async with await create_session(api_key, timeout) as session:
        for symbol in symbols:
            normalized_symbol = _normalize_symbol(symbol)

            for data_type in data_types:
                logger.debug(
                    "download started for %s %s %s from %s to %s",
                    exchange,
                    data_type,
                    normalized_symbol,
                    from_date,
                    to_date,
                )

                start_time = time()
                if days_count_to_fetch > 1:
                    await _download_dataset_if_needed(
                        session=session,
                        exchange=exchange,
                        data_type=data_type,
                        date=start_date + timedelta(days=days_count_to_fetch - 1),
                        symbol=normalized_symbol,
                        format=format,
                        endpoint=endpoint,
                        download_dir=download_dir,
                        get_filename=get_filename,
                        skip_if_exists=skip_if_exists,
                        http_proxy=http_proxy,
                    )

                await _download_dataset_if_needed(
                    session=session,
                    exchange=exchange,
                    data_type=data_type,
                    date=start_date,
                    symbol=normalized_symbol,
                    format=format,
                    endpoint=endpoint,
                    download_dir=download_dir,
                    get_filename=get_filename,
                    skip_if_exists=skip_if_exists,
                    http_proxy=http_proxy,
                )

                download_tasks = set()
                try:
                    for offset in range(1, days_count_to_fetch - 1):
                        while len(download_tasks) >= concurrency:
                            done, download_tasks = await asyncio.wait(download_tasks, return_when=asyncio.FIRST_COMPLETED)
                            for task in done:
                                task.result()

                        current_date = start_date + timedelta(days=offset)
                        download_tasks.add(
                            asyncio.create_task(
                                _download_dataset_if_needed(
                                    session=session,
                                    exchange=exchange,
                                    data_type=data_type,
                                    date=current_date,
                                    symbol=normalized_symbol,
                                    format=format,
                                    endpoint=endpoint,
                                    download_dir=download_dir,
                                    get_filename=get_filename,
                                    skip_if_exists=skip_if_exists,
                                    http_proxy=http_proxy,
                                )
                            )
                        )

                    await asyncio.gather(*download_tasks)
                except BaseException:
                    for task in download_tasks:
                        task.cancel()

                    await asyncio.gather(*download_tasks, return_exceptions=True)
                    raise

                logger.debug(
                    "download finished for %s %s %s from %s to %s in %.2fs",
                    exchange,
                    data_type,
                    normalized_symbol,
                    from_date,
                    to_date,
                    time() - start_time,
                )


def _normalize_symbol(symbol: str) -> str:
    return symbol.replace(":", "-").replace("/", "-").upper()


async def _download_dataset_if_needed(
    *,
    session,
    exchange: str,
    data_type: str,
    date: datetime,
    symbol: str,
    format: str,
    endpoint: str,
    download_dir: str,
    get_filename: Callable[[str, str, datetime, str, str], str],
    skip_if_exists: bool,
    http_proxy: Optional[str],
) -> None:
    url = (
        f"{endpoint}/{exchange}/{data_type}/{date.strftime('%Y/%m/%d')}/"
        f"{urllib.parse.quote(symbol, safe='')}.{format}.gz"
    )
    download_path = os.path.join(download_dir, get_filename(exchange, data_type, date, symbol, format))

    if skip_if_exists and os.path.exists(download_path):
        return

    await reliable_download(
        session=session,
        url=url,
        dest_path=download_path,
        http_proxy=http_proxy,
    )


def _parse_date(name: str, value: str) -> datetime:
    try:
        return dateutil.parser.isoparse(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid '{name}' argument: {value}. Please provide valid ISO date string. "
            "https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat"
        ) from exc
