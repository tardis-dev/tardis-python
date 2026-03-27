import os
import tempfile


DEFAULT_ENDPOINT = "https://api.tardis.dev/v1"
DEFAULT_DATASETS_ENDPOINT = "https://datasets.tardis.dev/v1"
DEFAULT_CACHE_DIR = os.path.join(tempfile.gettempdir(), ".tardis-cache")
