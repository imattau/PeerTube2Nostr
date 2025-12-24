import pytest
from fastapi.testclient import TestClient
from ..main import app, manager
import os

@pytest.fixture
def client():
    # Force a known API Key for testing
    manager.api_key = "test-secret-key"
    with TestClient(app) as c:
        yield c

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
