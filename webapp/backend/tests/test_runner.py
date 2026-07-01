import pytest
from unittest.mock import patch, MagicMock, PropertyMock
import time


class TestRateLimiter:
    def test_init_gets_limits(self):
        mock_store = MagicMock()
        mock_store.get_publish_limits.return_value = (600, 5)
        mock_store.get_daily_source_limit.return_value = 2

        from ..core.runner import RateLimiter
        rl = RateLimiter(mock_store, 1000000)
        assert rl.min_interval == 600
        assert rl.max_per_hour == 5
        assert rl.max_per_day_per_source == 2

    def test_wait_interval_zero_when_no_last_post(self):
        mock_store = MagicMock()
        mock_store.last_posted_ts.return_value = None
        mock_store.get_publish_limits.return_value = (600, 5)
        mock_store.get_daily_source_limit.return_value = 2

        from ..core.runner import RateLimiter
        rl = RateLimiter(mock_store, 1000000)
        assert rl.wait_interval() == 0

    def test_wait_interval_positive(self):
        mock_store = MagicMock()
        mock_store.last_posted_ts.return_value = 999900
        mock_store.get_publish_limits.return_value = (200, 5)
        mock_store.get_daily_source_limit.return_value = 2

        from ..core.runner import RateLimiter
        rl = RateLimiter(mock_store, 1000000)
        wait = rl.wait_interval()
        assert wait > 0
        assert wait == 100  # 200 - (1000000 - 999900) = 100

    def test_wait_hourly_under_cap(self):
        mock_store = MagicMock()
        mock_store.count_posted_since.return_value = 3
        mock_store.get_publish_limits.return_value = (600, 5)
        mock_store.get_daily_source_limit.return_value = 2

        from ..core.runner import RateLimiter
        rl = RateLimiter(mock_store, 1000000)
        assert rl.wait_hourly() == 0

    def test_wait_hourly_at_cap(self):
        mock_store = MagicMock()
        mock_store.count_posted_since.return_value = 5
        mock_store.oldest_posted_since.return_value = 996500
        mock_store.get_publish_limits.return_value = (600, 5)
        mock_store.get_daily_source_limit.return_value = 2

        from ..core.runner import RateLimiter
        rl = RateLimiter(mock_store, 1000000)
        assert rl.wait_hourly() > 0


class TestRunnerRelayHealth:
    def test_health_check_connected_relay(self):
        from ..core.runner import Runner

        mock_store = MagicMock()
        mock_store.get_enabled_relays.return_value = ["wss://relay.example.com"]

        with patch("pynostr.relay_manager.RelayManager") as MockRM:
            mock_rm = MagicMock()
            MockRM.return_value = mock_rm

            mock_mp = MagicMock()
            mock_mp.has_eose_notices.return_value = True
            mock_mp.has_events.return_value = False
            mock_mp.has_notices.return_value = False
            mock_rm.message_pool = mock_mp

            runner = Runner(
                store=mock_store,
                pt=MagicMock(),
                pub=MagicMock(),
                n=MagicMock(),
            )
            runner.check_relays_health()

            mock_rm.add_relay.assert_called_once_with("wss://relay.example.com")
            mock_rm.add_subscription_on_all_relays.assert_called_once()
            mock_rm.run_sync.assert_called_once()
            mock_rm.close_connections.assert_called_once()
            mock_store.update_relay_latency.assert_called_once()
            assert not mock_store.mark_relay_used.called

    def test_health_check_creates_subscription(self):
        from ..core.runner import Runner

        mock_store = MagicMock()
        mock_store.get_enabled_relays.return_value = ["wss://relay.example.com"]

        with patch("pynostr.relay_manager.RelayManager") as MockRM:
            mock_rm = MagicMock()
            MockRM.return_value = mock_rm

            mock_mp = MagicMock()
            mock_mp.has_eose_notices.return_value = True
            mock_rm.message_pool = mock_mp

            runner = Runner(
                store=mock_store,
                pt=MagicMock(),
                pub=MagicMock(),
                n=MagicMock(),
            )
            runner.check_relays_health()

            args, kwargs = mock_rm.add_subscription_on_all_relays.call_args
            sub_id = args[0]
            assert sub_id.startswith("health-")
            filters = args[1]
            assert len(filters) == 1
            assert filters[0].limit == 0

    def test_health_check_no_response_relay(self):
        from ..core.runner import Runner

        mock_store = MagicMock()
        mock_store.get_enabled_relays.return_value = ["wss://silent-relay.example.com"]

        with patch("pynostr.relay_manager.RelayManager") as MockRM:
            mock_rm = MagicMock()
            MockRM.return_value = mock_rm

            mock_mp = MagicMock()
            mock_mp.has_eose_notices.return_value = False
            mock_mp.has_events.return_value = False
            mock_mp.has_notices.return_value = False
            mock_rm.message_pool = mock_mp

            runner = Runner(
                store=mock_store,
                pt=MagicMock(),
                pub=MagicMock(),
                n=MagicMock(),
            )
            runner.check_relays_health()

            mock_store.mark_relay_used.assert_called_once()
            args, _ = mock_store.mark_relay_used.call_args
            assert "No response from relay" in args[1]

    def test_health_check_exception(self):
        from ..core.runner import Runner

        mock_store = MagicMock()
        mock_store.get_enabled_relays.return_value = ["wss://bad-relay.example.com"]

        with patch("pynostr.relay_manager.RelayManager") as MockRM:
            MockRM.side_effect = Exception("connection refused")

            runner = Runner(
                store=mock_store,
                pt=MagicMock(),
                pub=MagicMock(),
                n=MagicMock(),
            )
            runner.check_relays_health()

            mock_store.mark_relay_used.assert_called_once()
            args, _ = mock_store.mark_relay_used.call_args
            assert "connection refused" in args[1]

    def test_health_check_multiple_relays(self):
        from ..core.runner import Runner

        mock_store = MagicMock()
        mock_store.get_enabled_relays.return_value = [
            "wss://relay1.example.com",
            "wss://relay2.example.com",
        ]

        with patch("pynostr.relay_manager.RelayManager") as MockRM:
            mock_rm = MagicMock()
            MockRM.return_value = mock_rm
            mock_mp = MagicMock()
            mock_mp.has_eose_notices.return_value = True
            mock_mp.has_events.return_value = False
            mock_mp.has_notices.return_value = False
            mock_rm.message_pool = mock_mp

            runner = Runner(
                store=mock_store,
                pt=MagicMock(),
                pub=MagicMock(),
                n=MagicMock(),
            )
            runner.check_relays_health()

            assert mock_rm.add_relay.call_count == 2
            assert mock_store.update_relay_latency.call_count == 2


class TestRunnerIngest:
    def test_ingest_sources_once_calls_ingest_for_each_enabled_source(self):
        from ..core.runner import Runner

        mock_store = MagicMock()
        mock_store.get_enabled_sources.return_value = [
            {"id": 1, "api_base": "https://a.com", "api_channel": "chan1"},
            {"id": 2, "api_base": "https://b.com", "api_channel": "chan2"},
        ]

        mock_pt = MagicMock()
        mock_pt.list_channel_videos.return_value = None

        runner = Runner(
            store=mock_store,
            pt=mock_pt,
            pub=MagicMock(),
            n=MagicMock(),
        )

        with patch.object(runner, "_ingest_source") as mock_ingest:
            runner.ingest_sources_once(api_limit=50, lookback_days=30)
            assert mock_ingest.call_count == 2
            mock_ingest.assert_any_call({"id": 1, "api_base": "https://a.com", "api_channel": "chan1"}, 50, 30)
            mock_ingest.assert_any_call({"id": 2, "api_base": "https://b.com", "api_channel": "chan2"}, 50, 30)


class TestRunnerPublish:
    def test_publish_one_pending_no_pending(self):
        from ..core.runner import Runner

        mock_store = MagicMock()
        mock_store.next_pending_eligible.return_value = None

        runner = Runner(
            store=mock_store,
            pt=MagicMock(),
            pub=MagicMock(),
            n=MagicMock(),
        )
        result = runner.publish_one_pending(nsec="nsec1x", relays=["wss://relay.example.com"])
        assert result is False

    def test_publish_one_pending_success(self):
        from ..core.runner import Runner

        pending = {"id": 1, "source_id": 1, "title": "Test", "watch_url": "https://example.com/w/1"}
        mock_store = MagicMock()
        mock_store.next_pending_eligible.return_value = pending
        mock_store.get_daily_source_limit.return_value = 3

        mock_pub = MagicMock()
        mock_pub._build_tags.return_value = (21, [["title", "Test"]])
        mock_pub._build_content.return_value = "Test content"
        mock_pub.publish.return_value = "event_id_123"

        runner = Runner(
            store=mock_store,
            pt=MagicMock(),
            pub=mock_pub,
            n=MagicMock(),
        )
        result = runner.publish_one_pending(nsec="nsec1x", relays=["wss://relay.example.com"])
        assert result is True
        mock_store.mark_posted.assert_called_once_with(1, "event_id_123")
        mock_pub.publish.assert_called_once()

    def test_publish_one_pending_failure(self):
        from ..core.runner import Runner

        pending = {"id": 1, "source_id": 1, "title": "Test", "watch_url": "https://example.com/w/1"}
        mock_store = MagicMock()
        mock_store.next_pending_eligible.return_value = pending
        mock_store.get_daily_source_limit.return_value = 3

        mock_pub = MagicMock()
        mock_pub._build_tags.return_value = (21, [])
        mock_pub._build_content.return_value = "content"
        mock_pub.publish.side_effect = Exception("publish failed")

        runner = Runner(
            store=mock_store,
            pt=MagicMock(),
            pub=mock_pub,
            n=MagicMock(),
        )
        result = runner.publish_one_pending(nsec="nsec1x", relays=["wss://relay.example.com"])
        assert result is False
        mock_store.mark_failed.assert_called_once_with(1, "publish failed")

    def test_publish_with_pending_arg(self):
        from ..core.runner import Runner

        pending = {"id": 2, "source_id": 1, "title": "Specific", "watch_url": "https://example.com/w/2"}
        mock_store = MagicMock()

        mock_pub = MagicMock()
        mock_pub._build_tags.return_value = (21, [])
        mock_pub._build_content.return_value = "content"
        mock_pub.publish.return_value = "eid"

        runner = Runner(
            store=mock_store,
            pt=MagicMock(),
            pub=mock_pub,
            n=MagicMock(),
        )
        result = runner.publish_one_pending(nsec="nsec1x", relays=["wss://r.com"], pending=pending)
        assert result is True
        assert not mock_store.next_pending_eligible.called
