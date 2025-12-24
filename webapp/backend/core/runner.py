import time
import threading
import calendar
from typing import Optional, List, Dict, Callable
from .database import Store, get_stored_nsec
from .peertube import PeerTubeClient, IngestPipeline
from .nostr import NostrPublisher
from .utils import UrlNormaliser, parse_any_timestamp

class RateLimiter:
    def __init__(self, store: Store, now_ts: int) -> None:
        self.store = store
        self.now_ts = now_ts
        self.min_interval, self.max_per_hour = store.get_publish_limits()
        self.max_per_day_per_source = store.get_daily_source_limit()

    def wait_interval(self) -> int:
        last = self.store.last_posted_ts() or 0
        return max(0, self.min_interval - (self.now_ts - last)) if last else 0

    def wait_hourly(self) -> int:
        if self.store.count_posted_since(self.now_ts - 3600) >= self.max_per_hour:
            oldest = self.store.oldest_posted_since(self.now_ts - 3600)
            if oldest: return max(0, 3600 - (self.now_ts - oldest))
        return 0

    def wait_daily_for_source(self, source_id: Optional[int]) -> int:
        if source_id is None or self.max_per_day_per_source <= 0: return 0
        if self.store.count_posted_since_for_source(source_id, self.now_ts - 86400) >= self.max_per_day_per_source:
            oldest = self.store.oldest_posted_since_for_source(source_id, self.now_ts - 86400)
            if oldest: return max(0, 86400 - (self.now_ts - oldest))
        return 0

    def next_wait(self, source_id: Optional[int]) -> int:
        return max(self.wait_interval(), self.wait_hourly(), self.wait_daily_for_source(source_id))

class Runner:
    def __init__(self, store: Store, pt: PeerTubeClient, pub: NostrPublisher, n: UrlNormaliser, 
                 log_fn: Optional[Callable] = None, status_fn: Optional[Callable] = None) -> None:
        self.store = store
        self.pt = pt
        self.pub = pub
        self.n = n
        self.log_fn = log_fn or print
        self.status_fn = status_fn or (lambda x: None)
        self.ingest = IngestPipeline(store, pt, self.log_fn)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def ingest_sources_once(self, api_limit: int = 50, lookback_days: int = 30) -> None:
        for s in self.store.get_enabled_sources():
            self._ingest_source(s, api_limit, lookback_days)

    def check_relays_health(self) -> None:
        relays = self.store.get_enabled_relays()
        for r in relays:
            start = time.time()
            try:
                # Basic connection test using RelayManager
                from pynostr.relay_manager import RelayManager
                rm = RelayManager(timeout=5)
                rm.add_relay(r)
                # pynostr doesn't always expose a simple ping, but adding and closing tests basic reachability
                latency = int((time.time() - start) * 1000)
                self.store.update_relay_latency(r, latency)
            except Exception as e:
                self.store.mark_relay_used(r, str(e))

    def _ingest_source(self, s: dict, api_limit: int, lookback_days: int) -> None:
        sid = s["id"]
        api_base, api_chan, rss_url = s.get("api_base"), s.get("api_channel"), s.get("rss_url")
        last_polled = s.get("last_polled_ts")
        cutoff = None
        if not last_polled:
            lb = s.get("lookback_days") if s.get("lookback_days") is not None else lookback_days
            if lb > 0: cutoff = self.n.now_ts() - (lb * 86400)

        err = None
        if api_base and api_chan:
            vids = self.pt.list_channel_videos(api_base, api_chan, limit=api_limit)
            if vids is not None:
                ins, skp = self.ingest.ingest_entries(sid, list(reversed(vids)), 
                    lambda v: v.get("uuid") or v.get("id"), lambda v: v.get("url"),
                    lambda v: v.get("name", ""), lambda v: v.get("description", ""),
                    lambda v: parse_any_timestamp(v.get("publishedAt")), cutoff, s.get("api_channel_url"))
                self.store.mark_source_polled(sid, None)
                if ins: self.log_fn(f"[source {sid}] API new items: {ins}")
                return
            err = "API failed; trying RSS"

        if rss_url:
            try:
                entries = self.pt.parse_rss(rss_url)
                ins, skp = self.ingest.ingest_entries(sid, entries,
                    lambda e: e.get("id") or e.get("link"), lambda e: e.get("link"),
                    lambda e: e.get("title", ""), lambda e: e.get("summary", ""),
                    lambda e: int(calendar.timegm(e.published_parsed)) if hasattr(e, "published_parsed") else None,
                    cutoff, None)
                self.store.mark_source_polled(sid, err)
                if ins: self.log_fn(f"[source {sid}] RSS new items: {ins}")
            except Exception as e:
                self.store.mark_source_polled(sid, f"{err or ''} RSS failed: {e}")
        else:
            self.store.mark_source_polled(sid, err or "No RSS fallback and API failed")

    def publish_one_pending(self, nsec: str, relays: List[str]) -> bool:
        pending = self.store.next_pending_eligible(self.n.now_ts(), self.store.get_daily_source_limit())
        if not pending: return False
        try:
            eid = self.pub.publish(nsec, relays, self.pub._build_content(pending), self.pub._build_tags(pending))
            self.store.mark_posted(pending["id"], eid)
            for r in relays: self.store.mark_relay_used(r, None)
            self.log_fn(f"Published {eid} | {pending.get('title') or pending.get('watch_url')}")
            return True
        except Exception as e:
            self.store.mark_failed(pending["id"], str(e))
            for r in relays: self.store.mark_relay_used(r, str(e))
            self.log_fn(f"Publish failed: {e}")
            return False

    def run_loop(self, poll_seconds: int = 300, publish_interval: int = 60):
        self.log_fn("Starting background loop...")
        last_relay_check = 0
        while not self._stop_event.is_set():
            try:
                now = self.n.now_ts()
                if now - last_relay_check > 600:  # Every 10 minutes
                    self.status_fn("checking-relays")
                    self.check_relays_health()
                    last_relay_check = now

                self.status_fn("polling")
                self.ingest_sources_once()
                
                nsec = get_stored_nsec(self.store.db_path)
                relays = self.store.get_enabled_relays()
                
                if nsec and relays:
                    now = self.n.now_ts()
                    rate = RateLimiter(self.store, now)
                    pending = self.store.next_pending_eligible(now, self.store.get_daily_source_limit())
                    if pending:
                        wait = rate.next_wait(pending["source_id"])
                        if wait <= 0:
                            self.status_fn("publishing")
                            self.publish_one_pending(nsec, relays)
                        else:
                            self.status_fn("rate-limited")
                    else:
                        self.status_fn("idle")
                else:
                    self.status_fn("waiting-config")

                # Sleep in small chunks to be responsive to stop event
                for _ in range(poll_seconds * 5):
                    if self._stop_event.is_set(): break
                    time.sleep(0.2)
            except Exception as e:
                self.log_fn(f"Loop error: {e}")
                time.sleep(10)
