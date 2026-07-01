import pytest
from unittest.mock import patch, MagicMock, PropertyMock


class TestPeerTubeClient:
    def test_parse_rss_with_timeout_success(self):
        from ..core.peertube import PeerTubeClient
        from ..core.utils import UrlNormaliser

        client = PeerTubeClient(UrlNormaliser())
        sample_xml = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>yt:video:abc123</id>
    <link href="https://example.com/w/abc123"/>
    <title>Test Video</title>
    <summary>Description</summary>
    <published>2024-01-01T00:00:00Z</published>
  </entry>
</feed>"""

        with patch.object(client.session, "get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.text = sample_xml
            mock_get.return_value = mock_resp

            results = client.parse_rss("https://example.com/feed.xml")
            assert len(results) == 1
            assert results[0]["title"] == "Test Video"

    def test_parse_rss_with_timeout_exception(self):
        from ..core.peertube import PeerTubeClient
        from ..core.utils import UrlNormaliser

        client = PeerTubeClient(UrlNormaliser())

        with patch.object(client.session, "get") as mock_get:
            mock_get.side_effect = Exception("timeout")

            results = client.parse_rss("https://slow.example.com/feed.xml")
            assert results == []

    def test_enrich_video_api_failure_returns_none_fields(self):
        from ..core.peertube import PeerTubeClient
        from ..core.utils import UrlNormaliser

        client = PeerTubeClient(UrlNormaliser())
        with patch.object(client, "_get_json") as mock_get_json:
            mock_get_json.return_value = None

            result = client.enrich_video("https://example.com/w/abc123")
            assert result[0] is not None
            assert result[2:] == (None,) * 13

    def test_parse_rss_uses_session_timeout(self):
        from ..core.peertube import PeerTubeClient
        from ..core.utils import UrlNormaliser

        client = PeerTubeClient(UrlNormaliser())
        with patch.object(client.session, "get") as mock_get:
            mock_get.side_effect = TimeoutError("timed out")
            results = client.parse_rss("https://example.com/feed.xml")
            assert results == []
            mock_get.assert_called_once_with("https://example.com/feed.xml", timeout=15)


class TestPeerTubeClientVideoListing:
    def test_list_channel_videos_api_success(self):
        from ..core.peertube import PeerTubeClient

        normaliser = MagicMock()
        normaliser.normalise_http_url.return_value = "https://peertube.example.com"
        client = PeerTubeClient(normaliser)

        with patch.object(client, "_get_json") as mock_get_json:
            mock_get_json.return_value = {"data": [{"uuid": "v1", "name": "Video 1"}]}

            result = client.list_channel_videos("https://peertube.example.com", "mychannel")
            assert result is not None
            assert len(result) == 1
            assert result[0]["name"] == "Video 1"

    def test_list_channel_videos_api_returns_none_on_failure(self):
        from ..core.peertube import PeerTubeClient

        normaliser = MagicMock()
        normaliser.normalise_http_url.return_value = "https://peertube.example.com"
        client = PeerTubeClient(normaliser)

        with patch.object(client, "_get_json") as mock_get_json:
            mock_get_json.return_value = None

            result = client.list_channel_videos("https://peertube.example.com", "mychannel")
            assert result is None


class TestPeerTubeClientEnrich:
    def test_enrich_video_picks_mp4_and_hls(self):
        from ..core.peertube import PeerTubeClient

        normaliser = MagicMock()
        normaliser.extract_watch_id.return_value = ("https://peertube.example.com", "vid123")
        normaliser.normalise_watch_url.return_value = "https://peertube.example.com/w/vid123"
        client = PeerTubeClient(normaliser)

        video_data = {
            "name": "My Video",
            "description": "A cool video",
            "duration": 300,
            "thumbnailPath": "/static/thumb.jpg",
            "files": [
                {
                    "fileUrl": "https://peertube.example.com/static/video.mp4",
                    "mimeType": "video/mp4",
                    "resolution": {"width": 1920, "height": 1080},
                    "size": 1000000,
                }
            ],
            "streamingPlaylists": [
                {
                    "playlistUrl": "https://peertube.example.com/static/stream.m3u8",
                }
            ],
            "channel": {"displayName": "Test Channel", "url": "https://peertube.example.com/c/test"},
            "account": {"displayName": "Test Account", "url": "https://peertube.example.com/a/test"},
        }

        with patch.object(client, "_get_json") as mock_get_json:
            mock_get_json.return_value = video_data

            result = client.enrich_video("https://peertube.example.com/w/vid123")
            assert result[0] == "https://peertube.example.com"
            assert result[1] == "vid123"
            assert "video.mp4" in (result[2] or "")
            assert "stream.m3u8" in (result[3] or "")
            assert result[4] == 300
            assert result[5] is not None
            assert result[6] is not None
            assert result[7] == "https://peertube.example.com"
            assert result[8] == "Test Channel"
            assert result[12] == "My Video"
            assert result[13] == "A cool video"

    def test_enrich_video_missing_watch_id(self):
        from ..core.peertube import PeerTubeClient

        normaliser = MagicMock()
        normaliser.extract_watch_id.return_value = None
        client = PeerTubeClient(normaliser)

        result = client.enrich_video("https://example.com/invalid")
        assert result == (None,) * 15


class TestIngestPipeline:
    def test_ingest_entries_inserts_new_video(self):
        from ..core.peertube import IngestPipeline

        mock_store = MagicMock()
        mock_store.video_exists.return_value = False

        mock_pt = MagicMock()
        mock_pt.enrich_video.return_value = (
            "https://base.com", "vid1", "https://mp4.url", "https://hls.url",
            120, 1920, 1080, "instance.com", "Channel", "https://channel.url",
            "Account", "https://account.url", "Title", "Summary", "https://thumb.url",
        )

        pipeline = IngestPipeline(mock_store, mock_pt, print)

        entries = [{"id": "e1", "link": "https://example.com/w/v1", "title": "V1", "summary": "S1"}]
        inserted, skipped = pipeline.ingest_entries(
            source_id=1,
            entries=entries,
            entry_key_fn=lambda e: e["id"],
            watch_url_fn=lambda e: e["link"],
            title_fn=lambda e: e["title"],
            summary_fn=lambda e: e["summary"],
            published_ts_fn=lambda e: 1700000000,
            cutoff_ts=None,
            channel_url_fallback=None,
        )

        assert inserted == 1
        assert skipped == 0
        mock_store.insert_pending.assert_called_once()

    def test_ingest_entries_skips_existing(self):
        from ..core.peertube import IngestPipeline

        mock_store = MagicMock()
        mock_store.video_exists.return_value = True

        mock_pt = MagicMock()
        pipeline = IngestPipeline(mock_store, mock_pt, print)

        entries = [{"id": "e1", "link": "https://example.com/w/v1"}]
        inserted, skipped = pipeline.ingest_entries(
            source_id=1, entries=entries,
            entry_key_fn=lambda e: e["id"], watch_url_fn=lambda e: e["link"],
            title_fn=lambda e: "", summary_fn=lambda e: "",
            published_ts_fn=lambda e: None, cutoff_ts=None, channel_url_fallback=None,
        )

        assert inserted == 0
        assert skipped == 0
        assert not mock_store.insert_pending.called

    def test_ingest_entries_skips_old_entries(self):
        from ..core.peertube import IngestPipeline

        mock_store = MagicMock()
        mock_store.video_exists.return_value = False

        mock_pt = MagicMock()
        pipeline = IngestPipeline(mock_store, mock_pt, print)

        entries = [{"id": "e1", "link": "https://example.com/w/v1"}]
        inserted, skipped = pipeline.ingest_entries(
            source_id=1, entries=entries,
            entry_key_fn=lambda e: e["id"], watch_url_fn=lambda e: e["link"],
            title_fn=lambda e: "", summary_fn=lambda e: "",
            published_ts_fn=lambda e: 100, cutoff_ts=200, channel_url_fallback=None,
        )

        assert inserted == 0
        assert skipped == 1

    def test_ingest_entries_skips_missing_key_or_watch(self):
        from ..core.peertube import IngestPipeline

        mock_store = MagicMock()
        mock_pt = MagicMock()
        pipeline = IngestPipeline(mock_store, mock_pt, print)

        entries = [{"id": "", "link": ""}]
        inserted, skipped = pipeline.ingest_entries(
            source_id=1, entries=entries,
            entry_key_fn=lambda e: e.get("id"), watch_url_fn=lambda e: e.get("link"),
            title_fn=lambda e: "", summary_fn=lambda e: "",
            published_ts_fn=lambda e: None, cutoff_ts=None, channel_url_fallback=None,
        )

        assert inserted == 0
        assert skipped == 0
