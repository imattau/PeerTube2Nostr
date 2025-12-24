import pytest
import os
import sqlite3
from ..core.database import Store
from ..core.utils import UrlNormaliser

@pytest.fixture
def temp_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    return db_path

@pytest.fixture
def normaliser():
    return UrlNormaliser()

@pytest.fixture
def store(temp_db, normaliser):
    s = Store(temp_db, normaliser)
    s.init_schema()
    yield s
    s.close()
