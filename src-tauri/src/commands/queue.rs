use tauri::State;

use crate::db::Database;
use crate::models::*;

#[tauri::command]
pub fn list_videos(status: String, limit: Option<i64>, db: State<'_, Database>) -> Vec<Video> {
    db.list_videos(&status, limit.unwrap_or(200))
}

#[tauri::command]
pub fn retry_failed(db: State<'_, Database>) -> CountResult {
    let count = db.retry_failed();
    CountResult { count }
}

#[tauri::command]
pub fn get_queue_counts(db: State<'_, Database>) -> QueueCounts {
    QueueCounts {
        pending: db.count_pending(),
        failed: db.count_failed(),
        posted: db.count_posted(),
    }
}
