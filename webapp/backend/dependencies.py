import os
from pathlib import Path

from fastapi import Request

from core.database import Store
from core.utils import UrlNormaliser

_store_cache: dict[str, Store] = {}


def _default_db_path() -> str:
    data_dir = os.environ.get(
        "PEERTUBE2NOSTR_DATA_DIR",
        str(Path.home() / ".local" / "share" / "peertube2nostr"),
    )
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "peertube2nostr.db")


def get_db_path() -> str:
    return os.environ.get("PEERTUBE2NOSTR_DB_PATH") or _default_db_path()


def get_store(request: Request = None) -> Store:
    db_path = get_db_path()
    if db_path in _store_cache:
        return _store_cache[db_path]
    n = UrlNormaliser()
    store = Store(db_path, n)
    store.init_schema()
    store.seed_default_relays_if_empty()
    _store_cache[db_path] = store
    return store


def get_normaliser() -> UrlNormaliser:
    return UrlNormaliser()
