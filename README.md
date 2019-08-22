# tardis-client

Python client for tardis.dev - historical tick-level cryptocurrency market data replay API.

## Usage

```python
import asyncio
from tardis_client import TardisClient, Channel


async def replay():
    tardis_client = TardisClient()

    messages = tardis_client.replay(
        exchange="bitmex",
        from_date="2019-06-01",
        to_date="2019-06-02",
        filters=[Channel(name="trade", symbols=["XBTUSD","ETHUSD"]), Channel("orderBookL2", ["XBTUSD"])],
    )

    async for local_timestamp, message in messages:
        print(message)


asyncio.run(replay())
```
