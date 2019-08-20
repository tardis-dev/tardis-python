import os
import json
import hashlib
from functools import reduce


def get_slice_cache_path(cache_dir, exchange, date, filters):
    return os.path.join(cache_dir, "feeds", exchange, get_filters_hash(filters), format_date_to_path(date)) + ".json.gz"


def get_filters_hash(filters):
    if filters is None:
        filters = []

    def deduplicate_filters(prev, current):
        match_existing = None if len(prev) == 0 else next(filter for filter in prev if filter.name == current.name)

        if match_existing is not None:
            if match_existing.symbols is not None and current.symbols is not None:
                match_existing = match_existing._replace(symbols=[*match_existing.symbols, *current.symbols])

            elif current.symbols is not None:
                match_existing = match_existing._replace(symbols=[*current.symbols])
        else:
            prev.append(current)
        return prev

    # deduplicate filters (if the channel was provided multiple times)
    # filters = reduce(deduplicate_filters, filters, [])
    # sort filters in place to improve local disk cache ratio (no matter filters order if the same filters are provided will hit the cache)

    filters.sort(key=lambda filter: filter.name)

    # sort and deduplicate filters symbols
    # for filter in filters:
    #     if filter.symbols is not None:
    #         filter.symbols = list(set(filter.symbols))
    #         filter.symbols.sort()

    filters_serialized = json.dumps(filters, separators=(",", ":"))
    # return sha 256 hash digest of serialized filters
    hashlib.sha256(filters_serialized.encode("utf-8")).hexdigest()
    # todo
    return "07fdc31e2de738243f3c4980dc9b67b11e746eacc5f3da5818cc08b5cbc80ec2"


def format_date_to_path(date):
    year = str(date.year)
    month = double_digit(date.month)
    day = double_digit(date.day)
    hour = double_digit(date.hour)
    minute = double_digit(date.minute)

    return os.path.join(year, month, day, hour, minute)


def double_digit(input):
    return f"0{input}" if input < 10 else f"{input}"
