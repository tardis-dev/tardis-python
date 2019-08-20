import asyncio
import gzip
import logging
import json
import os
import tempfile

from collections import namedtuple
from datetime import datetime, timedelta
from .consts import EXCHANGES, EXCHANGE_CHANNELS_INFO
from .handy import get_slice_cache_path

Response = namedtuple("Response", ["local_timestamp", "message"])
Channel = namedtuple("Channel", ["name", "symbols"])

DATE_MESSAGE_SPLIT_INDEX = 28


class TardisClient:
    def __init__(
        self,
        endpoint="https://tardis.dev/api",
        cache_dir=os.path.join(tempfile.gettempdir(), ".tardis-cache"),
        api_key="",
    ):
        self.logger = logging.getLogger(__name__)
        self.endpoint = endpoint
        self.cache_dir = cache_dir
        self.api_key = api_key

        # self.logger.debug("TODO")

    async def replay(self, exchange, from_date, to_date, filters=[], decode_response=True):
        # self.logger.debug("Initializing WebSocket.")

        self._validate_payload(exchange, from_date, to_date, filters)
        from_date = datetime.fromisoformat(from_date)
        to_date = datetime.fromisoformat(to_date)
        current_slice_date = from_date

        while current_slice_date < to_date:
            current_slice_path = None
            while current_slice_path is None:
                path_to_check = get_slice_cache_path(self.cache_dir, exchange, current_slice_date, filters)
                # print(path_to_check)

                if os.path.isfile(path_to_check):
                    current_slice_path = path_to_check
                else:
                    # todo check process erorors
                    await asyncio.sleep(0.3)

            with gzip.open(current_slice_path, "rb") as file:
                for line in file:
                    if len(line) == 0:
                        continue

                    if decode_response:
                        # TODO comment about parsing date
                        timestamp = datetime.strptime(
                            line[0 : DATE_MESSAGE_SPLIT_INDEX - 2].decode("utf-8"), "%Y-%m-%dT%H:%M:%S.%f"
                        )
                        yield Response(timestamp, json.loads(line[DATE_MESSAGE_SPLIT_INDEX + 1 :]))
                    else:
                        yield Response(line[0:DATE_MESSAGE_SPLIT_INDEX], line[DATE_MESSAGE_SPLIT_INDEX + 1 :])

            current_slice_date = current_slice_date + timedelta(seconds=60)

    def _validate_payload(self, exchange, from_date, to_date, filters):
        if exchange not in EXCHANGES:
            raise ValueError(
                f"Invalid 'exchange' argument: {exchange}. Please provide one of the following exchanges: {sEXCHANGES.join(', ')}."
            )

        if from_date is None or self._try_parse_as_iso_date(from_date) is False:
            raise ValueError(
                f"Invalid 'from_date' argument: {from_date}. Please provide valid ISO date string. https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat"
            )

        if to_date is None or self._try_parse_as_iso_date(to_date) is False:
            raise ValueError(
                f"Invalid 'to_date' argument: {to_date}. Please provide valid ISO date string. https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat"
            )

        if datetime.fromisoformat(from_date) >= datetime.fromisoformat(to_date):
            raise ValueError(
                "Invalid 'from_date' and 'to_date' arguments combination. Please provide 'to_date' date string that is later than 'from_date'."
            )

        if filters is None:
            return

        if isinstance(filters, list) is False:
            raise ValueError("Invalid 'filters' argument. Please provide valid filters Channel list")

        if len(filters) > 0:
            for filter in filters:
                if filter.name not in EXCHANGE_CHANNELS_INFO[exchange]:
                    valid_channels = ", ".join(EXCHANGE_CHANNELS_INFO[exchange])
                    raise ValueError(
                        f"Invalid 'name' argument: {filter.name}. Please provide one of the following channels: {valid_channels}."
                    )

                if filter.symbols is None:
                    continue

                if isinstance(filter.symbols, list) is False or any(
                    isinstance(symbol, str) == False for symbol in filter.symbols
                ):
                    raise ValueError(
                        f"Invalid 'symbols[]' argument: {filter.symbols}. Please provide list of symbol strings."
                    )

    def _try_parse_as_iso_date(self, date_string):
        try:
            datetime.fromisoformat(date_string)
            return True
        except ValueError:
            return False

