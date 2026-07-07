import json
import logging
from pathlib import Path

from config import CACHE_DIR

logger = logging.getLogger(__name__)

_root = Path(CACHE_DIR)


def cache_path(source, key):
    return _root / source / f"{key}.json"


def get(source, key):
    p = cache_path(source, key)
    if not p.exists():
        return None
    with open(p) as f:
        return json.load(f)


def put(source, key, data):
    p = cache_path(source, key)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(data, f, indent=2)
    logger.debug("Cached %s/%s (%d bytes)", source, key, p.stat().st_size)


def exists(source, key):
    return cache_path(source, key).exists()


def is_empty():
    return not _root.exists() or not any(_root.iterdir())
