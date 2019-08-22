import os
import json
import hashlib
from dataclasses import asdict
from functools import reduce


def get_slice_cache_path(cache_dir, exchange, date, filters):
    return os.path.join(cache_dir, "feeds", exchange, get_filters_hash(filters), format_date_to_path(date)) + ".json.gz"


def get_filters_hash(filters):
    # it not filters were provided or were empty return empty list hash
    if filters is None or len(filters) == 0:
        return hashlib.sha256(json.dumps([]).encode("utf-8")).hexdigest()

    # convert fitlers to dictionary so can be json serialized (use the same names as other clients - channel, symbols)
    filters_serializable = [{"channel": filter.name, "symbols": filter.symbols} for filter in filters]
    filters_serialized = json.dumps(filters_serializable, separators=(",", ":"))
    # return sha 256 hash digest of serialized filters
    return hashlib.sha256(filters_serialized.encode("utf-8")).hexdigest()


def format_date_to_path(date):
    year = str(date.year)
    month = double_digit(date.month)
    day = double_digit(date.day)
    hour = double_digit(date.hour)
    minute = double_digit(date.minute)

    return os.path.join(year, month, day, hour, minute)


def double_digit(input):
    return f"0{input}" if input < 10 else f"{input}"
