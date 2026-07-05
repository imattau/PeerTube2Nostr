import os
import threading
from functools import lru_cache
from pathlib import Path

from fastapi import Depends, Request

from core.database import Store, get_stored_nsec
from core.utils import UrlNormaliser


def _default_db_path() -> str:
    data_dir = os.environ.get(
        "PEERTUBE2NOSTR_DATA_DIR",
        str(Path.home() / ".local" / "share" / "peertube2nostr"),
    )
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "peertube2nostr.db")


_thread_local = threading.local()


def get_db_path() -> str:
    return os.environ.get("PEERTUBE2NOSTR_DB_PATH") or _default_db_path()


def get_store(request: Request = None) -> Store:
    db_path = get_db_path()
    cache = getattr(_thread_local, "_store_cache", None)
    if cache is not None and cache[0] == db_path:
        return cache[1]
    n = UrlNormaliser()
    store = Store(db_path, n)
    store.init_schema()
    store.seed_default_relays_if_empty()
    _thread_local._store_cache = (db_path, store)
    return store


def get_normaliser() -> UrlNormaliser:
    return UrlNormaliser()
