import pytest
from ..core.models import IngestedItem

def test_relay_management(store):
    rid = store.add_relay("wss://relay.test")
    assert rid > 0
    
    relays = store.list_relays()
    assert len(relays) == 1
    assert relays[0]['relay_url'] == "wss://relay.test"
    
    store.set_relay_enabled(rid, False)
    relays = store.list_relays()
    assert relays[0]['enabled'] == 0
    
    store.remove_relay(rid)
    assert len(store.list_relays()) == 0

def test_source_management(store):
    sid = store.add_rss_source("https://example.com/feed.xml")
    assert sid > 0
    
    sources = store.list_sources()
    assert len(sources) == 1
    assert sources[0]['rss_url'] == "https://example.com/feed.xml"
    
    store.remove_source(sid)
    assert len(store.list_sources()) == 0

def test_video_persistence(store):
    sid = store.add_rss_source("https://example.com/feed.xml")
    item = IngestedItem(
        source_id=sid,
        entry_key="test-key",
        watch_url="https://instance.com/w/123",
        title="Test Video",
        summary="Desc",
        peertube_base="https://instance.com",
        peertube_video_id="123",
        hls_url=None,
        mp4_url=None,
        peertube_instance="instance.com",
        channel_name="Chan",
        channel_url=None,
        account_name=None,
        account_url=None,
        published_ts=1700000000,
        thumbnail_url="https://thumb.jpg"
    )
    
    store.insert_pending(item)
    assert store.count_pending() == 1
    assert store.video_exists(sid, "test-key") is True
