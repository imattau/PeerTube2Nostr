use crate::db::Database;
use crate::models::*;
use crate::nsec;
use tauri::State;

#[tauri::command]
pub fn get_settings(db: State<'_, Database>) -> Settings {
    let (min_interval, max_per_hour) = db.get_publish_limits();
    let max_per_day = db.get_daily_source_limit();
    Settings {
        min_publish_interval_seconds: min_interval,
        max_posts_per_hour: max_per_hour,
        max_posts_per_day_per_source: max_per_day,
        has_nsec: nsec::has_nsec(&db.db_path),
    }
}

#[tauri::command]
pub fn update_settings(
    min_publish_interval_seconds: Option<i64>,
    max_posts_per_hour: Option<i64>,
    max_posts_per_day_per_source: Option<i64>,
    db: State<'_, Database>,
) -> Settings {
    if let Some(v) = min_publish_interval_seconds {
        db.set_setting("min_publish_interval_seconds", &v.to_string());
    }
    if let Some(v) = max_posts_per_hour {
        db.set_setting("max_posts_per_hour", &v.to_string());
    }
    if let Some(v) = max_posts_per_day_per_source {
        db.set_setting("max_posts_per_day_per_source", &v.to_string());
    }
    let (min_interval, max_per_hour) = db.get_publish_limits();
    let max_per_day = db.get_daily_source_limit();
    Settings {
        min_publish_interval_seconds: min_interval,
        max_posts_per_hour: max_per_hour,
        max_posts_per_day_per_source: max_per_day,
        has_nsec: nsec::has_nsec(&db.db_path),
    }
}
