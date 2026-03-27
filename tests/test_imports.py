from importlib.metadata import version

from tardis_dev import (
    DEFAULT_CACHE_DIR,
    DEFAULT_DATASETS_ENDPOINT,
    DEFAULT_ENDPOINT,
    Channel,
    Response,
    __version__,
    clear_cache,
    default_file_name,
    download_datasets,
    download_datasets_async,
    get_exchange_details,
    get_exchange_details_async,
    replay,
)


def test_public_imports_are_available():
    assert __version__ == version("tardis-dev")
    assert DEFAULT_ENDPOINT == "https://api.tardis.dev/v1"
    assert DEFAULT_DATASETS_ENDPOINT == "https://datasets.tardis.dev/v1"
    assert DEFAULT_CACHE_DIR
    assert Channel is not None
    assert Response is not None
    assert replay is not None
    assert download_datasets is not None
    assert download_datasets_async is not None
    assert get_exchange_details is not None
    assert get_exchange_details_async is not None
    assert clear_cache is not None
    assert default_file_name is not None
