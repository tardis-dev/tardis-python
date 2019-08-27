from typing import List
from tardis_client.reconstructors.bitmex import BitmexMarketReconstructor
from tardis_client.reconstructors.market_reconstructor import MarketReconstructor

reconstructors = {"bitmex": BitmexMarketReconstructor}


def get_market_reconstructor(exchange: str, symbols: List[str]) -> MarketReconstructor:
    return reconstructors[exchange](symbols)
