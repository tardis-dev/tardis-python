from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version("tardis-dev")
except PackageNotFoundError:
    __version__ = "0.0.0"

from tardis_dev._options import DEFAULT_CACHE_DIR, DEFAULT_DATASETS_ENDPOINT, DEFAULT_ENDPOINT
from tardis_dev.channel import Channel
from tardis_dev.clear_cache import clear_cache
from tardis_dev.download_datasets import default_file_name, download_datasets, download_datasets_async
from tardis_dev.get_exchange_details import get_exchange_details, get_exchange_details_async
from tardis_dev.replay import Response, replay


__all__ = [
    "__version__",
    "DEFAULT_ENDPOINT",
    "DEFAULT_DATASETS_ENDPOINT",
    "DEFAULT_CACHE_DIR",
    "Channel",
    "Response",
    "replay",
    "download_datasets",
    "download_datasets_async",
    "get_exchange_details",
    "get_exchange_details_async",
    "clear_cache",
    "default_file_name",
]
