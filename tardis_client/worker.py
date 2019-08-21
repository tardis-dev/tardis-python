import asyncio


async def _fetch_data_async(exchange, from_date, to_date, filters, endpoint, cache_dir, api_key):
    await asyncio.sleep(1)


def fetch_data_to_replay(exchange, from_date, to_date, filters, endpoint, cache_dir, api_key):
    asyncio.run(_fetch_data_async(exchange, from_date, to_date, filters, endpoint, cache_dir, api_key))
