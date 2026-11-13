import os
import sys

import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)


@pytest.fixture()
def db_path(tmp_path, monkeypatch):
    path = str(tmp_path / "peertube2nostr.db")
    monkeypatch.setenv("PEERTUBE2NOSTR_DB_PATH", path)
    return path


@pytest.fixture()
def store(db_path):
    from core.database import Store
    from core.utils import UrlNormaliser

    s = Store(db_path, UrlNormaliser())
    s.init_schema()
    s.sync_admin_defaults()
    yield s
    s.close()
