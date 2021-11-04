import asyncio
import aiohttp


def get_exchange_details(exchange: str, http_proxy = None):
    return asyncio.get_event_loop().run_until_complete(get_exchange_details_async(exchange, http_proxy))


async def get_exchange_details_async(exchange, http_proxy):
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(f"https://api.tardis.dev/v1/exchanges/{exchange}", proxy=http_proxy) as response:
            return await response.json()
