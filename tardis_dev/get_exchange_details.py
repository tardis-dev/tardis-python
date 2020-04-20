import asyncio
import aiohttp


def get_exchange_details(exchange: str):
    return asyncio.get_event_loop().run_until_complete(get_exchange_details_async(exchange))


async def get_exchange_details_async(exchange):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.tardis.dev/v1/exchanges/{exchange}") as response:
            return await response.json()
