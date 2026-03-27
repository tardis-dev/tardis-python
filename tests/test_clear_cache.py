from pathlib import Path

from tardis_dev import clear_cache


def test_clear_cache_removes_configured_directory(tmp_path: Path):
    cache_dir = tmp_path / "cache"
    nested_file = cache_dir / "feeds" / "bitmex" / "slice.json.gz"
    nested_file.parent.mkdir(parents=True)
    nested_file.write_bytes(b"data")

    clear_cache(cache_dir=str(cache_dir))

    assert not cache_dir.exists()
