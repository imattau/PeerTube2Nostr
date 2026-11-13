import pytest
from fastapi.testclient import TestClient
from pynostr.event import Event
from pynostr.key import PrivateKey


@pytest.fixture()
def client():
    import main

    return TestClient(main.app)


def _pending_video() -> int:
    from core.database import IngestedItem
    from dependencies import get_store

    store = get_store()
    source_id = store.add_rss_source("https://tube.example/feeds/videos.xml")
    store.insert_pending(
        IngestedItem(
            source_id=source_id,
            entry_key="api-video-1",
            watch_url="https://tube.example/w/1",
            title="API video",
            summary="",
            peertube_base=None,
            peertube_video_id=None,
            hls_url=None,
            mp4_url=None,
            peertube_instance=None,
            channel_name=None,
            channel_url=None,
            account_name=None,
            account_url=None,
            published_ts=None,
        )
    )
    return int(store.list_videos("pending", 10)[0]["id"])


def _signed_event() -> dict:
    event = Event(kind=1, content="hello", tags=[])
    event.sign(PrivateKey().hex())
    return event.to_dict()


def test_relays_endpoint_exposes_admin_defaults(db_path, client, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", "wss://admin.one, wss://admin.two")
    res = client.get("/api/relays")
    assert res.status_code == 200
    body = res.json()
    assert body["default_relays"] == ["wss://admin.one/", "wss://admin.two/"]
    assert body["relay_policy"] == "open"
    assert all(r["is_default"] for r in body["relays"])


def test_settings_endpoint_exposes_policy(db_path, client, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "locked")
    res = client.get("/api/settings")
    assert res.status_code == 200
    assert res.json()["relay_policy"] == "locked"


def test_update_settings_preserves_has_nsec(db_path, client):
    res = client.put("/api/settings?min_publish_interval_seconds=60")
    assert res.status_code == 200
    assert res.json()["has_nsec"] is False
    assert res.json()["min_publish_interval_seconds"] == 60


def test_locked_policy_blocks_relay_mutation(db_path, client, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "locked")
    res = client.post("/api/relays", json={"relay_url": "wss://user.example"})
    assert res.status_code == 403


def test_publish_signed_rejects_invalid_signature(db_path, client):
    video_id = _pending_video()
    event = _signed_event()
    event["content"] = "tampered"
    res = client.post(
        "/api/queue/publish-signed",
        json={"video_id": video_id, "event_json": event, "relays": ["wss://relay.example"]},
    )
    assert res.status_code == 400


def test_publish_signed_uses_selected_relays(db_path, client, monkeypatch):
    video_id = _pending_video()

    captured = {}

    class FakeRelayManager:
        def __init__(self, timeout=15):
            self.timeout = timeout
            self.relays = []
            self.published = []

        def add_relay(self, url):
            self.relays.append(url)

        def publish_event(self, event):
            self.published.append(event)

        def run_sync(self):
            pass

        def close_connections(self):
            pass

    monkeypatch.setattr("pynostr.relay_manager.RelayManager", FakeRelayManager)

    event = _signed_event()
    res = client.post(
        "/api/queue/publish-signed",
        json={
            "video_id": video_id,
            "event_json": event,
            "relays": ["wss://chosen.example"],
        },
    )

    assert res.status_code == 200, res.text
    assert res.json()["relays"] == ["wss://chosen.example/"]
    captured["event_id"] = res.json()["event_id"]

    from dependencies import get_store

    posted = get_store().list_videos("posted", 10)
    assert posted[0]["nostr_event_id"] == event["id"]


def test_publish_signed_respects_locked_policy(db_path, client, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", "wss://admin.one")
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "locked")
    from dependencies import get_store

    get_store().sync_admin_defaults()
    video_id = _pending_video()

    res = client.post(
        "/api/queue/publish-signed",
        json={
            "video_id": video_id,
            "event_json": _signed_event(),
            "relays": ["wss://not-allowed.example"],
        },
    )
    assert res.status_code == 400
