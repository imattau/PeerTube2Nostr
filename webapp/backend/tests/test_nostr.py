import pytest
from unittest.mock import patch, MagicMock, ANY
from ..core import nostr as nostr_module
from ..core.nostr import NostrPublisher


class TestNostrPublisherBuildTags:
    def test_landscape_video(self):
        pending = {
            "title": "Test Video",
            "summary": "A test video",
            "channel_name": "TestChannel",
            "direct_url": "https://example.com/video.mp4",
            "width": 1920,
            "height": 1080,
            "watch_url": "https://example.com/w/abc123",
            "channel_url": "https://example.com/c/test",
            "published_ts": 1700000000,
        }
        kind, tags = NostrPublisher._build_tags(pending)
        assert kind == 21
        tag_types = [t[0] for t in tags]
        assert "title" in tag_types
        assert "imeta" in tag_types
        assert "t" in tag_types
        assert "r" in tag_types
        title_tag = next(t for t in tags if t[0] == "title")
        assert title_tag[1] == "Test Video"

    def test_portrait_video(self):
        pending = {
            "title": "Vertical Video",
            "channel_name": "Chan",
            "direct_url": "https://example.com/v.mp4",
            "width": 1080,
            "height": 1920,
            "watch_url": "https://example.com/w/def",
        }
        kind, tags = NostrPublisher._build_tags(pending)
        assert kind == 22

    def test_no_dimensions_defaults_kind21(self):
        pending = {
            "title": "No Dims",
            "direct_url": "https://example.com/v.mp4",
            "watch_url": "https://example.com/w/xyz",
        }
        kind, tags = NostrPublisher._build_tags(pending)
        assert kind == 21

    def test_imeta_mp4(self):
        pending = {
            "title": "MP4 Test",
            "channel_name": "Chan",
            "direct_url": "https://example.com/v.mp4",
            "width": 640,
            "height": 480,
            "thumbnail_url": "https://example.com/thumb.jpg",
            "duration": 120,
            "watch_url": "https://example.com/w/1",
        }
        kind, tags = NostrPublisher._build_tags(pending)
        imeta_tags = [t for t in tags if t[0] == "imeta"]
        assert len(imeta_tags) == 1
        assert any("url https://example.com/v.mp4" in t for t in imeta_tags)
        assert any("m video/mp4" in t for t in imeta_tags)

    def test_imeta_hls(self):
        pending = {
            "title": "HLS Test",
            "channel_name": "Chan",
            "hls_url": "https://example.com/stream.m3u8",
            "watch_url": "https://example.com/w/2",
        }
        kind, tags = NostrPublisher._build_tags(pending)
        imeta_tags = [t for t in tags if t[0] == "imeta"]
        assert len(imeta_tags) == 1
        assert any("m application/x-mpegURL" in t for t in imeta_tags)

    def test_tags_include_origin_and_peertube_instance(self):
        pending = {
            "title": "Origin Test",
            "channel_name": "Chan",
            "direct_url": "https://example.com/v.mp4",
            "watch_url": "https://example.com/w/3",
            "peertube_video_id": "vid123",
            "peertube_instance": "example.com",
        }
        kind, tags = NostrPublisher._build_tags(pending)
        tag_types = [t[0] for t in tags]
        assert "origin" in tag_types
        assert "peertube:instance" in tag_types


class TestNostrPublisherPublish:
    def test_pynostr_version_compat_public_key_ctor(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent, \
             patch.object(nostr_module, "RelayManager") as MockRelayManager:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc123"
            mock_priv.hex.return_value = "privhex"
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            mock_ev.id = "eventid123"
            MockEvent.side_effect = [mock_ev]

            mock_rm = MagicMock()
            MockRelayManager.return_value = mock_rm

            result = NostrPublisher.publish(
                nsec="nsec1test",
                relays=["wss://relay.example.com"],
                content="hello",
                kind=1,
                tags=[],
            )
            assert result == "eventid123"
            mock_rm.publish_event.assert_called_once_with(mock_ev)
            mock_rm.run_sync.assert_called_once()

    def test_closes_connections_after_publish(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent, \
             patch.object(nostr_module, "RelayManager") as MockRelayManager:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc"
            mock_priv.hex.return_value = "privhex"
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            mock_ev.id = "eid"
            MockEvent.return_value = mock_ev

            mock_rm = MagicMock()
            MockRelayManager.return_value = mock_rm

            NostrPublisher.publish("nsec1x", ["wss://relay.example.com"], "c", 1, [])
            mock_rm.close_connections.assert_called_once()

    def test_pynostr_version_fallback_pubkey_ctor(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent, \
             patch.object(nostr_module, "RelayManager") as MockRelayManager:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc123"
            mock_priv.hex.return_value = "privhex"
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            mock_ev.id = "eid2"
            MockEvent.side_effect = [TypeError("no public_key"), mock_ev]

            mock_rm = MagicMock()
            MockRelayManager.return_value = mock_rm

            result = NostrPublisher.publish("nsec1x", ["wss://relay.example.com"], "c", 1, [])
            assert result == "eid2"
            calls = MockEvent.call_args_list
            assert len(calls) == 2
            assert "public_key" in calls[0][1]
            assert "pubkey" in calls[1][1]

    def test_pynostr_priv_sign_event(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent, \
             patch.object(nostr_module, "RelayManager") as MockRelayManager:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc"
            mock_priv.hex.return_value = "privhex"
            mock_priv.sign_event = MagicMock()
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            mock_ev.id = "eid3"
            MockEvent.return_value = mock_ev

            mock_rm = MagicMock()
            MockRelayManager.return_value = mock_rm
            mock_rm.add_relay = MagicMock()

            NostrPublisher.publish("nsec1x", ["wss://relay.example.com"], "c", 1, [])
            mock_priv.sign_event.assert_called_once_with(mock_ev)

    def test_pynostr_ev_sign_with_priv(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent, \
             patch.object(nostr_module, "RelayManager") as MockRelayManager:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc"
            mock_priv.hex.return_value = "privhex"
            del mock_priv.sign_event
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            mock_ev.id = "eid4"
            MockEvent.return_value = mock_ev

            mock_rm = MagicMock()
            MockRelayManager.return_value = mock_rm

            NostrPublisher.publish("nsec1x", ["wss://relay.example.com"], "c", 1, [])
            mock_ev.sign.assert_called_once_with(mock_priv)

    def test_pynostr_ev_sign_with_hex_fallback(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent, \
             patch.object(nostr_module, "RelayManager") as MockRelayManager:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc"
            mock_priv.hex.return_value = "privhex"
            del mock_priv.sign_event
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            mock_ev.id = "eid5"
            mock_ev.sign.side_effect = [TypeError("wrong type"), None]
            MockEvent.return_value = mock_ev

            mock_rm = MagicMock()
            MockRelayManager.return_value = mock_rm

            NostrPublisher.publish("nsec1x", ["wss://relay.example.com"], "c", 1, [])
            calls = mock_ev.sign.call_args_list
            assert len(calls) == 2
            assert calls[0][0][0] == mock_priv
            assert calls[1][0][0] == "privhex"

    def test_pynostr_close_connections_handles_exception(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent, \
             patch.object(nostr_module, "RelayManager") as MockRelayManager:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc"
            mock_priv.hex.return_value = "privhex"
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            mock_ev.id = "eid6"
            MockEvent.return_value = mock_ev

            mock_rm = MagicMock()
            mock_rm.close_connections.side_effect = Exception("conn error")
            MockRelayManager.return_value = mock_rm

            result = NostrPublisher.publish("nsec1x", ["wss://relay.example.com"], "c", 1, [])
            assert result == "eid6"

    def test_raises_on_no_sign_method(self):
        with patch.object(nostr_module, "PrivateKey") as MockPrivateKey, \
             patch.object(nostr_module, "Event") as MockEvent:

            mock_priv = MagicMock()
            mock_priv.public_key.hex.return_value = "abc"
            del mock_priv.sign_event
            MockPrivateKey.from_nsec.return_value = mock_priv

            mock_ev = MagicMock()
            del mock_ev.sign
            MockEvent.return_value = mock_ev

            with pytest.raises(RuntimeError, match="Unable to sign event"):
                NostrPublisher.publish("nsec1x", ["wss://relay.example.com"], "c", 1, [])
