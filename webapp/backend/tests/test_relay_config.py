import os

from core.utils import DEFAULT_RELAYS, get_admin_default_relays, get_relay_policy


def test_default_relays_fall_back_to_builtin(monkeypatch):
    monkeypatch.delenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", raising=False)
    assert get_admin_default_relays() == DEFAULT_RELAYS


def test_default_relays_from_env_are_normalised_and_deduped(monkeypatch):
    monkeypatch.setenv(
        "PEERTUBE2NOSTR_DEFAULT_RELAYS",
        "wss://relay.one/, nonsense, wss://relay.one, ws://relay.two:80",
    )
    relays = get_admin_default_relays()
    assert relays == ["wss://relay.one/", "ws://relay.two/"]


def test_invalid_env_falls_back_to_builtin(monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", "not-a-relay")
    assert get_admin_default_relays() == DEFAULT_RELAYS


def test_relay_policy_defaults_to_open(monkeypatch):
    monkeypatch.delenv("PEERTUBE2NOSTR_RELAY_POLICY", raising=False)
    assert get_relay_policy() == "open"


def test_relay_policy_locked(monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "LOCKED")
    assert get_relay_policy() == "locked"


def test_unknown_relay_policy_is_open(monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_RELAY_POLICY", "whatever")
    assert get_relay_policy() == "open"
