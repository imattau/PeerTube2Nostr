import time
from dataclasses import dataclass

from core.database import Store, get_stored_nsec
from core.runner import _get_runtime_status
from core.runner import PendingSelector, RateLimiter


@dataclass
class DashboardMetrics:
    relays: int
    sources: int
    pending: int
    posted: int
    failed: int
    last_poll_ts: int
    last_posted_ts: int
    min_interval: int
    max_per_hour: int
    max_per_day_per_source: int
    has_nsec: bool
    status: str
    now_ts: int
    next_post: str

    @classmethod
    def from_store(cls, store: Store, db_path: str) -> "DashboardMetrics":
        now_ts = int(time.time())
        min_interval, max_per_hour = store.get_publish_limits()
        max_per_day_per_source = store.get_daily_source_limit()
        return cls(
            relays=store.count_relays(),
            sources=store.count_sources(),
            pending=store.count_pending(),
            posted=store.count_posted(),
            failed=store.count_failed(),
            last_poll_ts=store.last_polled_ts() or 0,
            last_posted_ts=store.last_posted_ts() or 0,
            min_interval=min_interval,
            max_per_hour=max_per_hour,
            max_per_day_per_source=max_per_day_per_source,
            has_nsec=bool(get_stored_nsec(db_path)),
            status=_get_runtime_status() or "idle",
            now_ts=now_ts,
            next_post=_estimate_next_post(store, db_path),
        )

    def poll_age(self) -> str:
        return f"{self.now_ts - self.last_poll_ts}s ago" if self.last_poll_ts else "never"

    def post_age(self) -> str:
        return f"{self.now_ts - self.last_posted_ts}s ago" if self.last_posted_ts else "never"

    def status_toolbar(self) -> str:
        nsec_txt = "nsec:yes" if self.has_nsec else "nsec:no"
        status_txt = f" status:{self.status}" if self.status else ""
        return (
            f" relays:{self.relays} sources:{self.sources} pending:{self.pending} "
            f"posted:{self.posted} failed:{self.failed} {nsec_txt}{status_txt} "
        )

    def dashboard_lines(self) -> list[str]:
        return [
            "Dashboard:",
            f"  Relays: {self.relays}",
            f"  Sources: {self.sources}",
            f"  Pending: {self.pending}",
            f"  Posted: {self.posted}",
            f"  Failed: {self.failed}",
            f"  Last poll: {self.poll_age()}",
            f"  Last post: {self.post_age()}",
            f"  Rate: min_interval={self.min_interval}s, max_per_hour={self.max_per_hour}",
            f"  Nsec set: {'yes' if self.has_nsec else 'no'}",
            f"  Status: {self.status}",
            "  Hint: type '/' to open the command palette",
        ]

    def counts_block(self) -> str:
        return "\n".join(
            [
                "Counts",
                f"Relays:   {self.relays}",
                f"Sources:  {self.sources}",
                f"Pending:  {self.pending}",
                f"Posted:   {self.posted}",
                f"Failed:   {self.failed}",
            ]
        )

    def activity_block(self) -> str:
        return "\n".join(
            [
                "Activity",
                f"Last poll: {self.poll_age()}",
                f"Last post: {self.post_age()}",
                f"Status:    {self.status}",
                f"Next post: {self.next_post}",
                f"Nsec set:  {'yes' if self.has_nsec else 'no'}",
            ]
        )

    def rate_block(self) -> str:
        return "\n".join(
            [
                "Rate Limits",
                f"Min interval: {self.min_interval}s",
                f"Max/hour:     {self.max_per_hour}",
                f"Max/day/src:  {self.max_per_day_per_source}",
            ]
        )


def _estimate_next_post(store: Store, db_path: str) -> str:
    if store.count_pending() == 0:
        return "none"
    if not get_stored_nsec(db_path):
        return "nsec missing"
    now_ts = int(time.time())
    selector = PendingSelector(store)
    rate = RateLimiter(store, now_ts)
    pending = selector.next_eligible(now_ts)
    wait = rate.next_wait(int(pending["source_id"])) if pending else 0
    if pending is None and selector.has_pending():
        rows = selector.list_pending(limit=200)
        source_ids = sorted({int(r[1]) for r in rows})
        wait = max(wait, rate.wait_interval(), rate.wait_hourly(), rate.wait_daily_for_any(source_ids))
    if wait == 0:
        return "now"
    return f"in {wait}s"
