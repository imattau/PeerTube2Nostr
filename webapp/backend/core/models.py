from dataclasses import dataclass
from typing import Optional

@dataclass
class IngestedItem:
    source_id: int
    entry_key: str
    watch_url: str
    title: str
    summary: str
    peertube_base: Optional[str]
    peertube_video_id: Optional[str]
    hls_url: Optional[str]
    mp4_url: Optional[str]
    peertube_instance: Optional[str]
    channel_name: Optional[str]
    channel_url: Optional[str]
    account_name: Optional[str]
    account_url: Optional[str]
    published_ts: Optional[int]
    thumbnail_url: Optional[str]

@dataclass
class DashboardMetrics:
    relays: int
    sources: int
    pending: int
    posted: int
    failed: int
    last_poll_ts: Optional[int]
    last_posted_ts: Optional[int]
    min_interval: int
    max_per_hour: int
    max_per_day_per_source: int
    has_nsec: bool
    status: str
    now_ts: int
    next_post: str
