import pytest
import os
import tempfile
from fastapi.testclient import TestClient
from ..main import app, manager
from ..core.utils import UrlNormaliser
from ..core.database import Store

@pytest.fixture
def client():
    # Use an isolated temp DB so tests don't share state
    old_db = manager.db_path
    old_store = manager.store
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(db_fd)
    manager.db_path = db_path
    manager.n = UrlNormaliser()
    manager.store = Store(db_path, manager.n)
    manager.store.init_schema()
    manager.store.set_setting("api_key", "test-secret-key")
    manager.store.set_setting("setup_complete", "1")
    with TestClient(app) as c:
        yield c
    os.unlink(db_path)
    manager.db_path = old_db
    manager.store = old_store

def test_unauthorized_access(client):
    response = client.get("/api/metrics")
    assert response.status_code == 401

def test_authorized_metrics(client):
    response = client.get("/api/metrics", headers={"X-API-Key": "test-secret-key"})
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "pending" in data

def test_add_source_api(client):
    payload = {"url": "https://peertube.social/c/test"}
    response = client.post(
        "/api/sources", 
        json=payload, 
        headers={"X-API-Key": "test-secret-key"}
    )
    assert response.status_code == 200
    assert "id" in response.json()

def test_setup_status(client):
    response = client.get("/api/setup/status")
    assert response.status_code == 200
    assert "is_complete" in response.json()
