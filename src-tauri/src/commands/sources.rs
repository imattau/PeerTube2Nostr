use tauri::State;

use crate::db::Database;
use crate::models::*;

#[tauri::command]
pub fn list_sources(db: State<'_, Database>) -> Vec<Source> {
    db.list_sources()
}

#[tauri::command]
pub fn add_source(url: String, db: State<'_, Database>) -> Result<AddResult, String> {
    let url = url.trim();
    let parsed = url::Url::parse(url).map_err(|_| "Invalid URL".to_string())?;
    let host = parsed.host_str().unwrap_or("");
    let path = parsed.path().trim_end_matches('/');

    let parts: Vec<&str> = path.split("/c/").collect();
    if parts.len() == 2 {
        let channel = parts[1].trim_end_matches("/videos");
        let base = format!("{}://{}", parsed.scheme(), host);
        let id = db.add_channel_source(url, &base, channel)?;
        return Ok(AddResult { id });
    }

    if path.contains("/feeds/") || url.contains("videos.xml") {
        let id = db.add_relay(url, url).map_err(|e| format!("Failed to add RSS source: {}", e))?;
        return Ok(AddResult { id });
    }

    Err("URL does not look like a PeerTube channel or RSS feed".into())
}

#[tauri::command]
pub fn remove_source(id: i64, db: State<'_, Database>) -> OkResult {
    db.remove_source(id);
    OkResult { ok: true }
}

#[tauri::command]
pub fn enable_source(id: i64, db: State<'_, Database>) -> OkResult {
    db.set_source_enabled(id, true);
    OkResult { ok: true }
}

#[tauri::command]
pub fn disable_source(id: i64, db: State<'_, Database>) -> OkResult {
    db.set_source_enabled(id, false);
    OkResult { ok: true }
}
