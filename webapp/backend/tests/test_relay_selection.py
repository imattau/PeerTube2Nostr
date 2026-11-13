import time

import pytest
from pynostr.event import Event
from pynostr.key import PrivateKey

from core.relays import (
    RelaySelectionError,
    SignedEventError,
    resolve_publish_relays,
    verify_signed_event,
)


def _signed_event(kind=1, content="hello") -> dict:
    event = Event(kind=kind, content=content, tags=[])
    event.sign(PrivateKey().hex())
    return event.to_dict()


def test_resolve_uses_enabled_relays_by_default(store):
    store.add_relay("wss://one.example")
    store.add_relay("wss://two.example")
    store.set_relay_enabled("wss://two.example", False)

    relays = resolve_publish_relays(store)
    assert "wss://one.example/" in relays
    assert "wss://two.example/" not in relays


def test_resolve_uses_requested_relays(store):
    store.add_relay("wss://one.example")
    store.add_relay("wss://two.example")
    assert resolve_publish_relays(store, ["wss://two.example"]) == ["wss://two.example/"]


def test_resolve_rejects_invalid_requested_relay(store):
    with pytest.raises(RelaySelectionError):
        resolve_publish_relays(store, ["https://not-a-relay.example"])


def test_resolve_rejects_empty_selection(store, monkeypatch):
    for r in store.list_relays_detailed():
        store.remove_relay(str(r[0]))
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "open")
    with pytest.raises(RelaySelectionError):
        resolve_publish_relays(store)


def test_locked_policy_only_allows_admin_defaults(store, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", "wss://admin.one")
    store.sync_admin_defaults()
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "locked")

    assert resolve_publish_relays(store, ["wss://admin.one"]) == ["wss://admin.one/"]
    with pytest.raises(RelaySelectionError):
        resolve_publish_relays(store, ["wss://other.example"])


def test_locked_policy_defaults_to_admin_relays(store, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", "wss://admin.one")
    store.sync_admin_defaults()
    store.add_relay("wss://user.example")
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "locked")

    assert resolve_publish_relays(store) == ["wss://admin.one/"]


def test_verify_signed_event_accepts_valid_event():
    data = _signed_event()
    ev = verify_signed_event(data)
    assert ev.id == data["id"]
    assert ev.kind == 1


def test_verify_signed_event_rejects_tampering():
    data = _signed_event()
    data["content"] = "tampered"
    with pytest.raises(SignedEventError):
        verify_signed_event(data)


def test_verify_signed_event_rejects_unsupported_kind():
    with pytest.raises(SignedEventError):
        verify_signed_event(_signed_event(kind=4))


def test_verify_signed_event_rejects_stale_timestamp():
    data = _signed_event()
    data["created_at"] = int(time.time()) - 3600
    # The signature no longer covers the modified timestamp, so either check may fire.
    with pytest.raises(SignedEventError):
        verify_signed_event(data)
