from dataclasses import FrozenInstanceError

import pytest

from tardis_dev import Channel


def test_channel_symbols_default_to_none():
    channel = Channel("trade")

    assert channel.name == "trade"
    assert channel.symbols is None


def test_channel_symbols_are_normalized_and_frozen():
    channel = Channel("trade", ["ETHUSD", "BTCUSD"])

    assert channel.symbols == ("BTCUSD", "ETHUSD")

    with pytest.raises(FrozenInstanceError):
        channel.name = "book"
