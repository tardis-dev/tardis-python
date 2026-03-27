import shutil

from tardis_dev._options import DEFAULT_CACHE_DIR


def clear_cache(*, cache_dir: str = DEFAULT_CACHE_DIR) -> None:
    shutil.rmtree(cache_dir, ignore_errors=True)
