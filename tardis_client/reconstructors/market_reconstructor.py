from typing import List, NamedTuple, Any, Optional, Union
from sortedcontainers import SortedDict
from enum import Enum
from decimal import Decimal
from datetime import datetime
from tardis_client.channel import Channel
from tardis_client.consts import ASK, BID


class BOOK_UPDATE_TYPE(Enum):
    NEW = 1
    CHANGE = 2
    DELETE = 3


class MESSAGE_TYPE(Enum):
    BOOK_DELTA = 1
    TRADES = 2


class Trade(NamedTuple):
    symbol: str
    side: str
    amount: float
    price: float
    timestamp: datetime


class BookUpdate(NamedTuple):
    symbol: str
    side: str
    update_type: BOOK_UPDATE_TYPE
    price_level: float
    amount: float


class MarketResponse(NamedTuple):
    local_timestamp: datetime
    message_type: MESSAGE_TYPE
    message: Union[List[Trade], List[BookUpdate]]
    order_book_state: Any


class MarketReconstructor:
    def __init__(self, symbols):
        self._books = {}
        self._books_views = {}
        self._symbols = symbols

        for symbol in symbols:
            self._books[symbol] = {ASK: SortedDict(), BID: SortedDict()}

            self._books_views[symbol] = {ASK: self._books[symbol][ASK].items(), BID: self._books[symbol][BID].items()}

    def get_filters(self) -> List[Channel]:
        raise NotImplementedError

    def reconstruct(local_timestamp, message) -> Optional[MarketResponse]:
        raise NotImplementedError
