use tauri::State;

use crate::db::Database;
use crate::models::*;
use crate::nsec;

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tauri::command]
pub fn get_metrics(db: State<'_, Database>) -> Metrics {
    let now = now_ts();
    let day_start = now - 86400;
    let pending = db.count_pending();
    let _posted = db.count_posted();
    let failed = db.count_failed();
    let posted_today = db.count_posted_since(day_start);
    let (min_interval, max_per_hour) = db.get_publish_limits();
    let max_per_day = db.get_daily_source_limit();

    let poll_age = format_poll_age(db.last_polled_ts(), now);
    let post_age = format_poll_age(db.last_posted_ts(), now);

    Metrics {
        relays: db.count_relays(),
        sources: db.count_sources(),
        pending,
        posted: posted_today,
        failed,
        has_nsec: nsec::has_nsec(&db.db_path),
        status: "idle".into(),
        next_post: if pending > 0 { "now".into() } else { "none".into() },
        poll_age,
        post_age,
        last_poll_ts: db.last_polled_ts(),
        last_posted_ts: db.last_posted_ts(),
        min_interval,
        max_per_hour,
        max_per_day_per_source: max_per_day,
        now_ts: now,
    }
}

fn format_poll_age(ts: Option<i64>, now: i64) -> String {
    match ts {
        Some(t) => format!("{}s ago", now - t),
        None => "never".into(),
    }
}
