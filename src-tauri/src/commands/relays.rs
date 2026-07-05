use tauri::State;

use crate::db::Database;
use crate::models::*;

#[tauri::command]
pub fn list_relays(db: State<'_, Database>) -> Vec<Relay> {
    db.list_relays()
}

#[tauri::command]
pub fn add_relay(relay_url: String, db: State<'_, Database>) -> Result<AddResult, String> {
    let norm = normalise_relay_url(&relay_url)?;
    let id = db.add_relay(&relay_url, &norm)?;
    Ok(AddResult { id })
}

#[tauri::command]
pub fn remove_relay(id: i64, db: State<'_, Database>) -> OkResult {
    db.remove_relay(id);
    OkResult { ok: true }
}

#[tauri::command]
pub fn enable_relay(id: i64, db: State<'_, Database>) -> OkResult {
    db.set_relay_enabled(id, true);
    OkResult { ok: true }
}

#[tauri::command]
pub fn disable_relay(id: i64, db: State<'_, Database>) -> OkResult {
    db.set_relay_enabled(id, false);
    OkResult { ok: true }
}

fn normalise_relay_url(url: &str) -> Result<String, String> {
    let raw = url.trim();
    let parsed = url::Url::parse(raw).map_err(|_| format!("Invalid URL: {}", raw))?;
    let scheme = parsed.scheme();
    if scheme != "wss" && scheme != "ws" {
        return Err("Relay URL must be ws or wss".into());
    }
    let host = parsed.host_str().unwrap_or("").to_lowercase();
    if host.is_empty() {
        return Err("Invalid URL host".into());
    }
    let port = parsed.port();
    let netloc = match port {
        Some(p) if !((scheme == "wss" && p == 443) || (scheme == "ws" && p == 80)) => {
            format!("{}:{}", host, p)
        }
        _ => host,
    };
    let path = parsed.path().trim_end_matches('/');
    let path = if path.is_empty() { "/" } else { path };
    Ok(format!("{}://{}{}", scheme, netloc, path))
}
