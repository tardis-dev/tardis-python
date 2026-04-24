import asyncio
import gzip
import json
import urllib.error
import urllib.parse
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, TypedDict, Union

from tardis_dev._http import create_session
from tardis_dev._options import DEFAULT_ENDPOINT


InstrumentInfo = Dict[str, Any]
InstrumentInfoFilterValue = Union[str, Sequence[str]]


class InstrumentInfoFilter(TypedDict, total=False):
    baseCurrency: InstrumentInfoFilterValue
    quoteCurrency: InstrumentInfoFilterValue
    type: InstrumentInfoFilterValue
    contractType: InstrumentInfoFilterValue
    underlyingType: InstrumentInfoFilterValue
    active: bool
    availableSince: str
    availableTo: str


class InstrumentSymbols(TypedDict):
    exchange: str
    symbols: List[str]


InstrumentSymbolSelector = Literal["id", "datasetId"]


def get_instrument_info(
    exchange: Union[str, Sequence[str]],
    *,
    filter: Optional[Mapping[str, Any]] = None,
    symbol: Optional[str] = None,
    api_key: str = "",
    endpoint: str = DEFAULT_ENDPOINT,
    timeout: int = 60,
    http_proxy: Optional[str] = None,
) -> Union[InstrumentInfo, List[InstrumentInfo]]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            get_instrument_info_async(
                exchange=exchange,
                filter=filter,
                symbol=symbol,
                api_key=api_key,
                endpoint=endpoint,
                timeout=timeout,
                http_proxy=http_proxy,
            )
        )

    raise RuntimeError(
        "get_instrument_info() cannot be called from a running event loop. Use get_instrument_info_async() instead."
    )


async def get_instrument_info_async(
    exchange: Union[str, Sequence[str]],
    *,
    filter: Optional[Mapping[str, Any]] = None,
    symbol: Optional[str] = None,
    api_key: str = "",
    endpoint: str = DEFAULT_ENDPOINT,
    timeout: int = 60,
    http_proxy: Optional[str] = None,
) -> Union[InstrumentInfo, List[InstrumentInfo]]:
    if filter is not None and symbol is not None:
        raise ValueError("Provide either 'filter' or 'symbol', not both.")

    if symbol is not None and not isinstance(exchange, str):
        raise ValueError("'symbol' can only be used with a single exchange.")

    async with await create_session(api_key, timeout) as session:
        if isinstance(exchange, str):
            return await _get_instrument_info(
                session=session,
                exchange=exchange,
                filter=filter,
                symbol=symbol,
                endpoint=endpoint,
                http_proxy=http_proxy,
            )

        results = await asyncio.gather(
            *(
                _get_instrument_info(
                    session=session,
                    exchange=exchange_id,
                    filter=filter,
                    endpoint=endpoint,
                    http_proxy=http_proxy,
                )
                for exchange_id in exchange
            )
        )

    return [instrument for instruments in results for instrument in instruments]


def find_instrument_symbols(
    exchanges: Sequence[str],
    filter: Mapping[str, Any],
    *,
    selector: InstrumentSymbolSelector = "id",
    api_key: str = "",
    endpoint: str = DEFAULT_ENDPOINT,
    timeout: int = 60,
    http_proxy: Optional[str] = None,
) -> List[InstrumentSymbols]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            find_instrument_symbols_async(
                exchanges=exchanges,
                filter=filter,
                selector=selector,
                api_key=api_key,
                endpoint=endpoint,
                timeout=timeout,
                http_proxy=http_proxy,
            )
        )

    raise RuntimeError(
        "find_instrument_symbols() cannot be called from a running event loop. Use find_instrument_symbols_async() instead."
    )


async def find_instrument_symbols_async(
    exchanges: Sequence[str],
    filter: Mapping[str, Any],
    *,
    selector: InstrumentSymbolSelector = "id",
    api_key: str = "",
    endpoint: str = DEFAULT_ENDPOINT,
    timeout: int = 60,
    http_proxy: Optional[str] = None,
) -> List[InstrumentSymbols]:
    _validate_selector(selector)

    async with await create_session(api_key, timeout) as session:
        return await asyncio.gather(
            *(
                _find_instrument_symbols_for_exchange(
                    session=session,
                    exchange=exchange,
                    filter=filter,
                    selector=selector,
                    endpoint=endpoint,
                    http_proxy=http_proxy,
                )
                for exchange in exchanges
            )
        )


def _validate_selector(selector: str) -> None:
    if selector not in ("id", "datasetId"):
        raise ValueError("Invalid 'selector' argument. Supported values are 'id' and 'datasetId'.")


async def _find_instrument_symbols_for_exchange(
    *,
    session,
    exchange: str,
    filter: Mapping[str, Any],
    selector: InstrumentSymbolSelector,
    endpoint: str,
    http_proxy: Optional[str],
) -> InstrumentSymbols:
    instruments = await _get_instrument_info(
        session=session,
        exchange=exchange,
        filter=filter,
        symbol=None,
        endpoint=endpoint,
        http_proxy=http_proxy,
    )

    return {
        "exchange": exchange,
        "symbols": [_get_symbol(instrument, selector) for instrument in instruments],
    }


async def _get_instrument_info(
    *,
    session,
    exchange: str,
    filter: Optional[Mapping[str, Any]],
    symbol: Optional[str] = None,
    endpoint: str,
    http_proxy: Optional[str],
) -> Union[InstrumentInfo, List[InstrumentInfo]]:
    url = _get_instrument_info_url(endpoint, exchange, filter, symbol)

    async with session.get(url, proxy=http_proxy) as response:
        body = await response.read()
        if response.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        if response.status != 200:
            error_text = body.decode("utf-8", errors="replace")
            raise urllib.error.HTTPError(url, code=response.status, msg=error_text, hdrs=None, fp=None)

        return json.loads(body.decode("utf-8"))


def _get_instrument_info_url(
    endpoint: str,
    exchange: str,
    filter: Optional[Mapping[str, Any]],
    symbol: Optional[str],
) -> str:
    url = f"{endpoint}/instruments/{urllib.parse.quote(exchange, safe='')}"
    if symbol is not None:
        return f"{url}/{urllib.parse.quote(symbol, safe='')}"

    if filter is not None:
        encoded_filter = urllib.parse.quote(json.dumps(filter, separators=(",", ":")))
        return f"{url}?filter={encoded_filter}"

    return url


def _get_symbol(instrument: Mapping[str, Any], selector: InstrumentSymbolSelector) -> str:
    if selector == "datasetId":
        return instrument.get("datasetId") or instrument["id"]

    return instrument["id"]
