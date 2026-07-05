import os
import threading
import time
from typing import Optional, List

from core.database import Store, get_stored_nsec
from core.peertube import PeerTubeClient, IngestPipeline
from core.nostr import NostrPublisher
from core.utils import UrlNormaliser, DEFAULT_RELAYS, _sleep_interruptible

_RUNTIME_STATUS = ""


def _set_runtime_status(value: str) -> None:
    global _RUNTIME_STATUS
    _RUNTIME_STATUS = value


def _get_runtime_status() -> str:
    return _RUNTIME_STATUS


class PendingSelector:
    def __init__(self, store: Store) -> None:
        self.store = store

    def has_pending(self) -> bool:
        return self.store.count_pending() > 0

    def list_pending(self, limit: int = 200) -> list[tuple]:
        return self.store.list_pending(limit=limit)

    def next_eligible(self, now_ts: int) -> Optional[dict]:
        max_per_day_per_source = self.store.get_daily_source_limit()
        return self.store.next_pending_eligible(now_ts, max_per_day_per_source)

    def daily_counts(self, now_ts: int) -> dict[int, int]:
        return self.store.count_posted_by_source_since(now_ts - 86400)


class RateLimiter:
    def __init__(self, store: Store, now_ts: int) -> None:
        self.store = store
        self.now_ts = now_ts
        self.min_interval, self.max_per_hour = store.get_publish_limits()
        self.max_per_day_per_source = store.get_daily_source_limit()

    def wait_interval(self) -> int:
        last_posted = self.store.last_posted_ts() or 0
        if not last_posted:
            return 0
        return max(0, self.min_interval - (self.now_ts - last_posted))

    def wait_hourly(self) -> int:
        posted_last_hour = self.store.count_posted_since(self.now_ts - 3600)
        if posted_last_hour >= self.max_per_hour:
            oldest = self.store.oldest_posted_since(self.now_ts - 3600)
            if oldest:
                return max(0, 3600 - (self.now_ts - oldest))
        return 0

    def wait_daily_for_source(self, source_id: Optional[int]) -> int:
        if source_id is None or self.max_per_day_per_source <= 0:
            return 0
        posted_last_day = self.store.count_posted_since_for_source(source_id, self.now_ts - 86400)
        if posted_last_day >= self.max_per_day_per_source:
            oldest = self.store.oldest_posted_since_for_source(source_id, self.now_ts - 86400)
            if oldest:
                return max(0, 86400 - (self.now_ts - oldest))
        return 0

    def wait_daily_for_any(self, source_ids: list[int]) -> int:
        if self.max_per_day_per_source <= 0:
            return 0
        if not source_ids:
            return 0
        counts = self.store.count_posted_by_source_since(self.now_ts - 86400)
        waits = []
        for sid in source_ids:
            if int(counts.get(int(sid), 0)) >= self.max_per_day_per_source:
                oldest = self.store.oldest_posted_since_for_source(int(sid), self.now_ts - 86400)
                if oldest:
                    waits.append(max(0, 86400 - (self.now_ts - oldest)))
        return min(waits) if waits else 0

    def next_wait(self, pending_source_id: Optional[int]) -> int:
        return max(self.wait_interval(), self.wait_hourly(), self.wait_daily_for_source(pending_source_id))


class Runner:
    def __init__(
        self,
        store: Store,
        pt: PeerTubeClient,
        pub: NostrPublisher,
        n: UrlNormaliser,
        log_fn: Optional[callable] = None,
        status_fn: Optional[callable] = None,
        dry_run: bool = False,
    ) -> None:
        self.store = store
        self.pt = pt
        self.pub = pub
        self.n = n
        self.log_fn = log_fn
        self.status_fn = status_fn
        self.dry_run = dry_run
        self.ingest = IngestPipeline(store, pt, n, self._log)

    def _log(self, msg: str) -> None:
        if self.log_fn:
            self.log_fn(msg)
        else:
            print(msg)

    def _status(self, msg: str) -> None:
        if self.status_fn:
            self.status_fn(msg)

    def ingest_sources_once(self, api_limit: int = 50, lookback_days: int = 30) -> None:
        sources = self.store.get_enabled_sources()
        for s in sources:
            self._ingest_source(s, api_limit, lookback_days)

    def ingest_source_once(self, source_id: int, api_limit: int = 50, lookback_days: int = 30) -> None:
        s = self.store.get_source_by_id(source_id)
        if not s:
            self._log(f"Source id {source_id} not found.")
            return
        if s.get("enabled") != 1:
            self._log(f"Source id {source_id} is disabled.")
            return
        self._ingest_source(s, api_limit, lookback_days)

    def _ingest_source(self, s: dict, api_limit_per_source: int, new_source_lookback_days: int) -> None:
        sid = s["id"]
        api_base = s.get("api_base")
        api_channel = s.get("api_channel")
        rss_url = s.get("rss_url")
        last_polled_ts = s.get("last_polled_ts")
        lookback_days = s.get("lookback_days")
        cutoff_ts = None
        if not last_polled_ts:
            effective_lookback = lookback_days if lookback_days is not None else new_source_lookback_days
            if effective_lookback and effective_lookback > 0:
                cutoff_ts = self.n.now_ts() - (effective_lookback * 86400)

        inserted = 0
        skipped = 0
        err: Optional[str] = None

        if api_base and api_channel:
            vids = self.pt.list_channel_videos(api_base=api_base, channel=api_channel, limit=api_limit_per_source)
            if vids is not None:
                entries = list(reversed(vids))

                def api_entry_key(v: dict) -> Optional[str]:
                    return v.get("uuid") or v.get("shortUUID") or v.get("id") or v.get("url")

                def api_watch_url(v: dict) -> str:
                    watch_url = v.get("url")
                    if isinstance(watch_url, str) and watch_url.startswith("http"):
                        return watch_url
                    vid_id = v.get("uuid") or v.get("shortUUID") or v.get("id")
                    if isinstance(vid_id, (str, int)) and api_base:
                        return f"{self.n.normalise_http_url(api_base)}/w/{vid_id}"
                    return ""

                inserted, skipped = self.ingest.ingest_entries(
                    source_id=sid,
                    entries=entries,
                    entry_key_fn=api_entry_key,
                    watch_url_fn=api_watch_url,
                    title_fn=lambda v: (v.get("name") or v.get("title") or "").strip(),
                    summary_fn=lambda v: (v.get("description") or "").strip(),
                    published_ts_fn=self._api_entry_ts,
                    cutoff_ts=cutoff_ts,
                    channel_url_fallback=s.get("api_channel_url"),
                )

                self.store.mark_source_polled(sid, None)
                if inserted:
                    self._log(f"[source {sid}] API new items: {inserted}")
                if skipped:
                    self._log(f"[source {sid}] API skipped old items: {skipped}")
                return

            err = "API listing failed; trying RSS fallback"

        if rss_url:
            try:
                entries = self.pt.parse_rss(rss_url)
                inserted, skipped = self.ingest.ingest_entries(
                    source_id=sid,
                    entries=entries,
                    entry_key_fn=self._rss_entry_key,
                    watch_url_fn=lambda e: (e.get("link") or "").strip(),
                    title_fn=lambda e: (e.get("title") or "").strip(),
                    summary_fn=lambda e: (e.get("summary") or "").strip(),
                    published_ts_fn=self._rss_entry_ts,
                    cutoff_ts=cutoff_ts,
                    channel_url_fallback=None,
                )

                self.store.mark_source_polled(sid, None if not err else err)
                if inserted:
                    self._log(f"[source {sid}] RSS new items: {inserted}")
                if skipped:
                    self._log(f"[source {sid}] RSS skipped old items: {skipped}")
            except Exception as ex:
                self.store.mark_source_polled(sid, f"{err + '; ' if err else ''}RSS failed: {ex}")
                self._log(f"[source {sid}] RSS error: {ex}")
        else:
            self.store.mark_source_polled(sid, err or "No RSS fallback configured and API listing failed/unconfigured")

    @staticmethod
    def _rss_entry_key(e: dict) -> str:
        for k in ("id", "guid", "link"):
            v = e.get(k)
            if v:
                return str(v)
        return str(hash(repr(sorted(e.items()))))

    @staticmethod
    def _api_entry_ts(v: dict) -> Optional[int]:
        from core.utils import _parse_any_timestamp
        val = v.get("publishedAt") or v.get("createdAt")
        return _parse_any_timestamp(val)

    @staticmethod
    def _rss_entry_ts(e: dict) -> Optional[int]:
        import calendar
        from core.utils import _parse_any_timestamp
        for k in ("published_parsed", "updated_parsed"):
            val = e.get(k)
            if val:
                try:
                    return int(calendar.timegm(val))
                except Exception:
                    continue
        for k in ("published", "updated"):
            val = e.get(k)
            ts = _parse_any_timestamp(val)
            if ts:
                return ts
        return None

    def publish_one_pending(self, nsec: str, relays: list[str], pending: Optional[dict] = None) -> None:
        if pending is None:
            pending = self.store.next_pending()
        if not pending:
            return

        content = self.pub._build_content(pending)
        kind, tags = self.pub._build_tags(pending)

        if self.dry_run:
            self._log(f"[DRY-RUN] Would publish: {pending.get('title') or pending.get('watch_url')}")
            self._log(f"[DRY-RUN] Content:\n{content}")
            self._log(f"[DRY-RUN] Tags: {tags}")
            self._log(f"[DRY-RUN] Relays: {relays}")
            return

        try:
            eid = self.pub.publish(nsec=nsec, relays=relays, content=content, kind=kind, tags=tags)
            self.store.mark_posted(pending["id"], eid)
            for r in relays:
                self.store.mark_relay_used(r, None)
            self._log(f"Published {eid} | {pending.get('title') or pending.get('watch_url')}")
        except Exception as ex:
            self.store.mark_failed(pending["id"], str(ex))
            for r in relays:
                self.store.mark_relay_used(r, str(ex))
            self._log(f"Publish failed: {ex}")

    def run(
        self,
        nsec: Optional[str],
        relays: Optional[list[str]],
        poll_seconds: int,
        publish_interval_seconds: int,
        retry_failed_after_seconds: Optional[int],
        api_limit_per_source: int,
        new_source_lookback_days: int,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        dynamic_nsec = nsec is None
        dynamic_relays = relays is None
        last_relays: Optional[list[str]] = None
        last_nsec_set: Optional[bool] = None

        self._log(f"Poll: {poll_seconds}s | Publish spacing: {publish_interval_seconds}s | API limit/source: {api_limit_per_source}")

        last_retry_check = 0
        selector = PendingSelector(self.store)

        while True:
            try:
                if stop_event and stop_event.is_set():
                    self._log("Stopped.")
                    return

                self._status("Fetching feeds")
                if dynamic_relays:
                    relays = self.store.get_enabled_relays() or DEFAULT_RELAYS
                if relays != last_relays:
                    self._log(f"Relays: {', '.join(relays or [])}")
                    last_relays = list(relays or [])

                if dynamic_nsec:
                    nsec = get_stored_nsec(self.store.db_path)
                nsec_set = bool(nsec)
                if last_nsec_set is None or nsec_set != last_nsec_set:
                    if nsec_set:
                        self._log("Nsec available for publishing.")
                    else:
                        self._log("No nsec set; publishing paused.")
                    last_nsec_set = nsec_set

                now = self.n.now_ts()
                if retry_failed_after_seconds is not None:
                    if last_retry_check == 0 or (now - last_retry_check) >= 60:
                        n = self.store.retry_failed(retry_failed_after_seconds)
                        if n:
                            self._log(f"Re-queued failed items for retry: {n}")
                        last_retry_check = now

                self.ingest_sources_once(
                    api_limit=api_limit_per_source,
                    lookback_days=new_source_lookback_days,
                )

                if nsec:
                    now_ts = self.n.now_ts()
                    rate = RateLimiter(self.store, now_ts)
                    pending = selector.next_eligible(now_ts)
                    if pending is not None:
                        wait = rate.next_wait(int(pending["source_id"]))
                        if wait > 0:
                            self._status("Rate limited")
                        else:
                            self._status("Publishing")
                            self.publish_one_pending(nsec=nsec, relays=relays or [], pending=pending)
                            self._status("Idle")
                    else:
                        if selector.has_pending():
                            rows = selector.list_pending(limit=200)
                            source_ids = sorted({int(r[1]) for r in rows})
                            wait = max(rate.wait_interval(), rate.wait_hourly(), rate.wait_daily_for_any(source_ids))
                            if wait > 0:
                                self._status("Rate limited")
                            else:
                                self._status("Idle")
                        else:
                            self._status("Idle")
                else:
                    self._status("Idle")

                if not _sleep_interruptible(publish_interval_seconds, stop_event):
                    self._log("Stopped.")
                    return
                self._status("Sleeping")
                if not _sleep_interruptible(poll_seconds, stop_event):
                    self._log("Stopped.")
                    return
                self._status("Idle")
            except KeyboardInterrupt:
                self._log("\nStopped.")
                return
            except Exception as ex:
                self._log(f"Loop error: {ex}")
                _sleep_interruptible(poll_seconds, stop_event)
