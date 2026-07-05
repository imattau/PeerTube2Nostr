use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub id: i64,
    pub enabled: bool,
    pub api_base: Option<String>,
    pub api_channel: Option<String>,
    pub api_channel_url: Option<String>,
    pub rss_url: Option<String>,
    pub lookback_days: Option<i64>,
    pub last_polled_ts: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relay {
    pub id: i64,
    pub enabled: bool,
    pub relay_url: String,
    pub relay_url_norm: Option<String>,
    pub last_used_ts: Option<i64>,
    pub last_error: Option<String>,
    pub latency_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Video {
    pub id: i64,
    pub source_id: i64,
    pub watch_url: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub hls_url: Option<String>,
    pub direct_url: Option<String>,
    pub thumbnail_url: Option<String>,
    pub duration: Option<i64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub peertube_instance: Option<String>,
    pub channel_name: Option<String>,
    pub channel_url: Option<String>,
    pub account_name: Option<String>,
    pub account_url: Option<String>,
    pub status: String,
    pub nostr_event_id: Option<String>,
    pub error: Option<String>,
    pub first_seen_ts: i64,
    pub last_attempt_ts: Option<i64>,
    pub posted_ts: Option<i64>,
    pub published_ts: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub min_publish_interval_seconds: i64,
    pub max_posts_per_hour: i64,
    pub max_posts_per_day_per_source: i64,
    pub has_nsec: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    pub relays: i64,
    pub sources: i64,
    pub pending: i64,
    pub posted: i64,
    pub failed: i64,
    pub has_nsec: bool,
    pub status: String,
    pub next_post: String,
    pub poll_age: String,
    pub post_age: String,
    pub last_poll_ts: Option<i64>,
    pub last_posted_ts: Option<i64>,
    pub min_interval: i64,
    pub max_per_hour: i64,
    pub max_per_day_per_source: i64,
    pub now_ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetrics {
    pub pending: i64,
    pub posted_today: i64,
    pub failed: i64,
    pub active_sources: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddResult {
    pub id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OkResult {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountResult {
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsecStatus {
    pub configured: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsecSetResult {
    pub stored_in: String,
}

