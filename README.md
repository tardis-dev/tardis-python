# tardis-client

[![PyPi](https://img.shields.io/pypi/v/tardis-client.svg)](https://pypi.org/project/tardis-client/)

Python client for [tardis.dev](https://tardis.dev)- historical tick-level cryptocurrency market data replay API.
Provides fast, high level and developer friendly wrapper for more low level [REST API](https://docs.tardis.dev/api#http-api) with local file based caching build in.

## Installation

Requires Python 3.7.0+ installed.

```sh
pip install tardis-client
```

## Usage

```python
import asyncio
from tardis_client import TardisClient, Channel

async def replay():
    tardis_client = TardisClient()

    # replay method returns Async Generator
    # https://rickyhan.com/jekyll/update/2018/01/27/python36.html
    messages = tardis_client.replay(
        exchange="bitmex",
        from_date="2019-06-01",
        to_date="2019-06-02",
        filters=[Channel(name="trade", symbols=["XBTUSD","ETHUSD"]), Channel("orderBookL2", ["XBTUSD"])],
    )

    # this will print all trades and orderBookL2 messages for XBTUSD
    # and all trades for ETHUSD for bitmex exchange
    # between 2019-06-01T00:00:00.000Z and 2019-06-02T00:00:00.000Z (whole first day of June 2019)
    async for local_timestamp, message in messages:
        # local timestamp is a Python datetime that marks timestamp when given message has been received
        # message is a message object as provided by exchange real-time stream
        print(message)

asyncio.run(replay())
```

## API

`tardis-client` package provides `TardisClient` and `Channel` classes.

```python
from tardis_client import TardisClient, Channel
```

### TardisClient

Optional client constructor parameters.

| name                   | type     | default value               | description                                                                                                                                                     |
| ---------------------- | -------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `api_key` (optional)   | `string` | `""`                        | optional `string` containing API key for [tardis.dev](https://tardis.dev) API. If not provided only first day of each month of data is accessible (free access) |
| `cache_dir` (optional) | `string` | `<os.tmpdir>/.tardis-cache` | optional `string` with path to local dir that will be used as cache location. If not provided default `temp` dir for given OS will be used.                     |

Example:

```python
# creates new client instance with access only to sample data (first day of each month)
tardis_client = TardisClient()

# creates new client with access to all data for given API key
tardis_client = TardisClient(api_key="YOUR_API_KEY")

# creates new client with custom cache dir
tardis_client = TardisClient(cache_dir="./cache")
```

- ### `tardis_client.clear_cache()`

  Removes local file cache dir and it's contents.

  Example:

  ```python
  tardis_client = TardisClient()

  tardis_client.clear_cache()
  ```

## FAQ

#### How to debug it if something went wrong?

Please enable Python debug logging to get `tardis-client` produce debug logs that help with figuring out what's going on internally - `logging.basicConfig(level=logging.DEBUG)`.

#### Where can I find more details about tardis.dev API?

Check out [API docs](https://docs.tardis.dev/api).

## License

MPL-2.0
