# tardis-dev

[![PyPi](https://img.shields.io/pypi/v/tardis-dev.svg)](https://pypi.org/project/tardis-dev/)
[![Python](https://img.shields.io/pypi/pyversions/tardis-dev.svg)](https://pypi.org/project/tardis-dev/)
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>

<br/>

Python `tardis-dev` package provides convenient access to tick-level historical cryptocurrency market data in exchange-native format. It focuses on two primary workflows: replaying historical market data and downloading historical market data as CSV files. If you need normalized replay or real-time streaming, use the Node.js client or Tardis Machine.

`replay()` accepts ISO date strings or Python `datetime` values. Naive datetimes are treated as UTC.

<br/>

```python
import asyncio
from tardis_dev import Channel, replay


async def main():
    async for local_timestamp, message in replay(
        exchange="binance",
        from_date="2024-03-01",
        to_date="2024-03-02",
        filters=[Channel("trade", ["btcusdt"]), Channel("depth", ["btcusdt"])],
        api_key="YOUR_API_KEY",
    ):
        print(local_timestamp, message)


asyncio.run(main())
```

<br/>

## Features

- historical tick-level [market data replay](https://docs.tardis.dev/python-client/replaying-historical-data) backed by Tardis.dev [HTTP API](https://docs.tardis.dev/api/http-api-reference#data-feeds-exchange)

<br/>

- [historical market data downloads as CSV files](https://docs.tardis.dev/python-client/quickstart#csv-dataset-downloads)

<br/>

- support for many cryptocurrency exchanges — see [docs.tardis.dev](https://docs.tardis.dev/historical-data-details/overview) for the full list

<br/>
<br/>
<br/>

## Installation

Requires Python 3.9+ installed.

```bash
pip install tardis-dev
```

<br/>
<br/>

## Development

This repository uses [`uv`](https://docs.astral.sh/uv/) for dependency management, locking, builds, and release automation.

```bash
uv sync --locked --group dev
uv run black --check tardis_dev tests
uv run pytest tests/ -q -m "not live"
uv build --no-sources
```

<br/>
<br/>

## Documentation

### [See official docs](https://docs.tardis.dev/python-client/quickstart)

- [Quickstart](https://docs.tardis.dev/python-client/quickstart)
- [Replaying Historical Data](https://docs.tardis.dev/python-client/replaying-historical-data)
- [CSV Dataset Downloads](https://docs.tardis.dev/python-client/quickstart#csv-dataset-downloads)
- [Migration Notice](https://docs.tardis.dev/python-client/migration-notice)

<br/>
<br/>

## Examples

### Replay historical market data

```python
import asyncio
from tardis_dev import Channel, replay


async def main():
    async for local_timestamp, message in replay(
        exchange="binance",
        from_date="2024-03-01",
        to_date="2024-03-02",
        filters=[Channel("trade", ["btcusdt"]), Channel("depth", ["btcusdt"])],
    ):
        print(local_timestamp, message)


asyncio.run(main())
```

<br/>

### Download CSV datasets

```python
from tardis_dev import download_datasets


download_datasets(
    exchange="binance",
    data_types=["trades", "incremental_book_L2"],
    symbols=["BTCUSDT"],
    from_date="2024-03-01",
    to_date="2024-03-02",
    api_key="YOUR_API_KEY",
)
```

<br/>

### Migration from `tardis-client`

This package is the v3 API. Existing `tardis-client` and `tardis_dev.datasets.download()` users should migrate to the new top-level functions:

- replay: `TardisClient().replay(...)` -> `replay(...)`
- datasets: `from tardis_dev import datasets; datasets.download(...)` -> `from tardis_dev import download_datasets`
- cache cleanup: `tardis_client.clear_cache()` -> `clear_cache()`

See [Migration Notice](https://docs.tardis.dev/python-client/migration-notice) for the full migration guide.

<br/>
<br/>

## See the [tardis-dev docs](https://docs.tardis.dev/python-client/quickstart) for more examples.
