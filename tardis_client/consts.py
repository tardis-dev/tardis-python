EXCHANGES = [
    "bitmex",
    "binance",
    "binance-futures",
    "deribit",
    "bitstamp",
    "coinbase",
    "cryptofacilities",
    "kraken",
    "bitfinex",
    "bitfinex-derivatives",
    "okex",
    "binance-jersey",
    "binance-dex",
    "ftx",
    "gemini",
    "bitflyer",
]

BINANCE_CHANNELS = ["trade", "ticker", "depth", "miniTicker", "depthSnapshot"]

BINANCE_DEX_CHANNELS = ["trades", "marketDiff", "kline_1m", "ticker", "depthSnapshot"]

BITFINEX_CHANNELS = ["trades", "book"]

BITMEX_CHANNELS = [
    "trade",
    "orderBookL2",
    "liquidation",
    "connected",
    "announcement",
    "chat",
    "publicNotifications",
    "instrument",
    "settlement",
    "funding",
    "insurance",
    "orderBookL2_25",
    "quote",
    "quoteBin1m",
    "quoteBin5m",
    "quoteBin1h",
    "quoteBin1d",
    "tradeBin1m",
    "tradeBin5m",
    "tradeBin1h",
    "tradeBin1d",
]

BITSTAMP_CHANNELS = ["live_trades", "live_orders", "diff_order_book"]

COINBASE_CHANNELS = [
    "subscriptions",
    "received",
    "open",
    "done",
    "match",
    "change",
    "l2update",
    "ticker",
    "snapshot",
    "last_match",
    "full_snapshot",
]

DERIBIT_CHANNELS = [
    "book",
    "deribit_price_index",
    "deribit_price_ranking",
    "estimated_expiration_price",
    "markprice.options",
    "perpetual",
    "trades",
    "ticker",
    "quote",
]

KRAKEN_CHANNELS = ["ticker", "trade", "book", "spread"]

OKEX_CHANNELS = [
    "spot/ticker",
    "spot/trade",
    "spot/depth",
    "swap/ticker",
    "swap/trade",
    "swap/depth",
    "swap/funding_rate",
    "swap/price_range",
    "swap/mark_price",
    "futures/ticker",
    "futures/trade",
    "futures/depth",
    "futures/price_range",
    "futures/mark_price",
    "futures/estimated_price",
]

CRYPTOFACILITIES_CHANNELS = ["trade", "trade_snapshot", "book", "book_snapshot", "ticker", "heartbeat"]

FTX_CHANNELS = ["orderbook", "trades"]

GEMINI_CHANNELS = ["trade", "l2_updates", "auction_open", "auction_indicative", "auction_result"]

BITFLYER_CHANNELS = ["lightning_board_snapshot", "lightning_board", "lightning_ticker", "lightning_executions"]

BINANCE_FUTURES_CHANNELS = ["aggTrade", "ticker", "depth", "markPrice", "depthSnapshot"]

BITFINEX_DERIV_CHANNELS = ["trades", "book", "status"]

EXCHANGE_CHANNELS_INFO = {
    "bitmex": BITMEX_CHANNELS,
    "coinbase": COINBASE_CHANNELS,
    "deribit": DERIBIT_CHANNELS,
    "cryptofacilities": CRYPTOFACILITIES_CHANNELS,
    "bitstamp": BITSTAMP_CHANNELS,
    "kraken": KRAKEN_CHANNELS,
    "okex": OKEX_CHANNELS,
    "binance": BINANCE_CHANNELS,
    "binance-jersey": BINANCE_CHANNELS,
    "binance-dex": BINANCE_DEX_CHANNELS,
    "bitfinex": BITFINEX_CHANNELS,
    "ftx": FTX_CHANNELS,
    "gemini": GEMINI_CHANNELS,
    "bitflyer": BITFLYER_CHANNELS,
    "binance-futures": BINANCE_FUTURES_CHANNELS,
    "bitfinex-derivatives": BITFINEX_DERIV_CHANNELS,
}


ASK = "ask"
BID = "bid"
