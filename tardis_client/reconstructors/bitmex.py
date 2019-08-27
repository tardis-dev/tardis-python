from typing import Optional, Any
from datetime import datetime

from tardis_client.consts import ASK, BID
from tardis_client.channel import Channel

from tardis_client.reconstructors.market_reconstructor import (
    MarketReconstructor,
    MarketResponse,
    MESSAGE_TYPE,
    Trade,
    BookUpdate,
    BOOK_UPDATE_TYPE,
)


class BitmexMarketReconstructor(MarketReconstructor):
    def __init__(self, symbols):
        super().__init__(symbols)
        self._id_to_price_map = {}
        self._action_to_update_type_map = {
            "partial": BOOK_UPDATE_TYPE.NEW,
            "insert": BOOK_UPDATE_TYPE.NEW,
            "delete": BOOK_UPDATE_TYPE.DELETE,
            "update": BOOK_UPDATE_TYPE.CHANGE,
        }

    def get_filters(self):
        return [Channel("orderBookL2", self._symbols), Channel("trade", self._symbols)]

    def reconstruct(self, local_timestamp, message) -> Optional[MarketResponse]:
        table = message["table"]
        action = message["action"]
        is_trade = table == "trade"
        is_order_book_delta = table == "orderBookL2"
        is_partial = action == "partial"

        if is_trade == False and is_order_book_delta == False:
            return
        # ignore trade partials, we could end up with duplicated trades otherwise
        if is_trade and is_partial:
            return

        message_type = MESSAGE_TYPE.TRADES if is_trade else MESSAGE_TYPE.BOOK_DELTA
        items = []

        for item in message["data"]:
            symbol = item["symbol"]
            # ignore data items for symbols we're not requested for
            # that could happen for example for partial messages that contained multiple symbols in single message
            if symbol not in self._symbols:
                continue

            if is_trade:
                items.append(self._map_trade(item))
            else:

                if action == "partial" or action == "insert":
                    # bitmex update messages do not contain price only id, so we need to keep state about mapping from id to price level locally
                    self._id_to_price_map[item["id"]] = item["price"]

                price_level = item["price"] if "price" in item else self._id_to_price_map[item["id"]]
                # ignore book update when we don't know price level for it, in theory it could happen
                #  when there was WS reconnection and after reconnection there was an immediate book update before partial message with book snapshot
                # and that update didn't have matching price level, as we could miss insert message durring reconnection
                # slim chances but in theory possible

                if price_level is None:
                    continue

                book_update = self._map_order_book_update(item, price_level, self._action_to_update_type_map[action])
                items.append(book_update)

                self._apply_book_update_to_order_book(book_update)

        # return order book state for symbol that is in first normalized item - BitMEX messages are uniform (same symbol for all items in the message)
        symbol = items[0].symbol
        order_book_state = self._books_views[symbol]

        return MarketResponse(
            local_timestamp=local_timestamp, message_type=message_type, message=items, order_book_state=order_book_state
        )

    def _map_trade(self, item: Any) -> Trade:
        return Trade(
            symbol=item["symbol"],
            side="buy" if item["side"] == "Buy" else "sell",
            amount=item["size"],
            price=item["price"],
            timestamp=datetime.fromisoformat(item["timestamp"][:-1]),
        )

    def _map_order_book_update(self, item: Any, price_level: float, update_type: BOOK_UPDATE_TYPE) -> BookUpdate:
        side = BID if item["side"] == "Buy" else ASK
        amount = 0 if update_type == BOOK_UPDATE_TYPE.DELETE else item["size"]

        return BookUpdate(symbol=item["symbol"], side=side, update_type=update_type, price_level=price_level, amount=amount)

    def _apply_book_update_to_order_book(self, book_update: BookUpdate):
        matching_book = self._books[book_update.symbol]

        if book_update.update_type == BOOK_UPDATE_TYPE.DELETE:
            del matching_book[book_update.side][book_update.price_level]
        else:
            matching_book[book_update.side][book_update.price_level] = book_update.amount

