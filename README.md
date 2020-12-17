# tardis-client

[![PyPi](https://img.shields.io/pypi/v/tardis-client.svg)](https://pypi.org/project/tardis-client/)
[![Python](https://img.shields.io/pypi/pyversions/tardis-client.svg)](https://pypi.org/project/tardis-client/)
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>

Python client for [tardis.dev](https://tardis.dev) - historical tick-level cryptocurrency market data replay API.
Provides fast, high level and developer friendly wrapper for more low level [HTTP API](https://docs.tardis.dev/api#http-api) with local file based caching build in.

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

[![Try on repl.it](https://repl-badge.jajoosam.repl.co/try.png)](https://repl.it/@TardisThad/tardis-python-client-example)

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

- ### `tardis_client.replay(exchange, from_date, to_date, filters=[])`

  Replays historical market data messages for given replay arguments.

  Returns [Async Generator](https://rickyhan.com/jekyll/update/2018/01/27/python36.html) with named tuples (`namedtuple("Response", ["local_timestamp", "message"])`).

  - `local_timestamp` is a Python datetime object specyfying when message has been received from the exchange real-time data feed.

  - `message` is Python dict with parsed JSON that has exactly the same format as message provided by particular exchange's real-time data feed.

    #### `replay` method parameters:

    | name                 | type                              | default value | description                                                                                                                                                                                       |
    | -------------------- | --------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
    | `exchange`           | `string`                          | -             | requested exchange name - Use [/exchanges](https://docs.tardis.dev/api/http#exchanges) API call to get allowed exchanges ids                                                                      |
    | `from_date`          | `string`                          | -             | requested UTC start date of data feed - [valid ISO date string](https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat), eg: `2019-04-05` or `2019-05-05T00:00:00`           |
    | `to_date`            | `string`                          | -             | requested UTC end date of data feed - [valid ISO date string](https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat), eg: `2019-04-05` or `2019-05-05T00:00:00`             |
    | `filters` (optional) | [`List[Channel]`](#channel-class) | []            | optional filters of requested data feed. Use [/exchanges/:exchange](https://docs.tardis.dev/api/http#exchanges-exchange) API call to get allowed channel names and symbols for requested exchange |

    ##### `Channel` class

    `Channel` class constructor parameters.

    | name      | type           | description                                                                                                                                         |
    | --------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
    | `name`    | `string`       | Use [/exchanges/:exchange](https://docs.tardis.dev/api#exchanges-exchange) API call to get allowed channel names and symbols for requested exchange |
    | `symbols` | `List[string]` | Use [/exchanges/:exchange](https://docs.tardis.dev/api#exchanges-exchange) API call to get allowed channel names and symbols for requested exchange |

    ```python
    Channel(name="trade", symbols=["XBTUSD","ETHUSD"])
    Channel("orderBookL2", ["XBTUSD"])
    ```

## FAQ

#### How to debug it if something went wrong?

`tardis-client` uses Python logging on `DEBUG` level for that purpose. In doubt please create issue in this repository with steps how to reproduce the issue.

#### Where can I find more details about tardis.dev API?

Check out [API docs](https://docs.tardis.dev/api).

## License

MPL-2.0
