from core.database import IngestedItem


def test_admin_defaults_are_marked_and_user_relays_preserved(store, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", "wss://admin.one, wss://admin.two")
    store.sync_admin_defaults()

    store.add_relay("wss://user.relay")
    store.set_relay_enabled("wss://user.relay", False)
    store.sync_admin_defaults()

    relays = {r[2]: r for r in store.list_relays_detailed()}
    assert relays["wss://admin.one/"][7] == 1
    assert relays["wss://admin.two/"][7] == 1
    assert "wss://user.relay" in relays
    assert relays["wss://user.relay"][7] == 0
    # A user-disabled relay stays disabled across admin syncs.
    assert relays["wss://user.relay"][1] == 0

    assert store.get_default_relays() == ["wss://admin.one/", "wss://admin.two/"]


def test_disabled_default_is_not_returned(store, monkeypatch):
    monkeypatch.setenv("PEERTUBE2NOSTR_DEFAULT_RELAYS", "wss://admin.one, wss://admin.two")
    store.sync_admin_defaults()
    store.set_relay_enabled("wss://admin.two", False)
    assert store.get_default_relays() == ["wss://admin.one/"]


def test_claim_pending_is_atomic(store):
    store.add_rss_source("https://tube.example/feeds/videos.xml")
    store.insert_pending(
        IngestedItem(
            source_id=1,
            entry_key="video-1",
            watch_url="https://tube.example/w/1",
            title="Video",
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
    video = store.list_videos("pending", 10)[0]

    assert store.claim_pending(video["id"]) is True
    assert store.claim_pending(video["id"]) is False
    assert store.count_pending() == 1  # publishing still counts as pending

    assert store.requeue_stale_publishing(0) == 1
    assert store.claim_pending(video["id"]) is True


def test_mark_posted_records_relays(store):
    store.add_rss_source("https://tube.example/feeds/videos.xml")
    store.insert_pending(
        IngestedItem(
            source_id=1,
            entry_key="video-2",
            watch_url="https://tube.example/w/2",
            title="Video",
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
    video = store.list_videos("pending", 10)[0]
    store.mark_posted(video["id"], "event-id", ["wss://relay.one/"])

    posted = store.list_videos("posted", 10)[0]
    assert posted["published_relays"] == '["wss://relay.one/"]'
