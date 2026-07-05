use crate::db::Database;
use crate::nsec;
use crate::models::*;
use tauri::State;

#[tauri::command]
pub fn get_nsec_status(db: State<'_, Database>) -> NsecStatus {
    NsecStatus {
        configured: nsec::has_nsec(&db.db_path),
    }
}

#[tauri::command]
pub fn set_nsec(nsec_key: String, db: State<'_, Database>) -> Result<NsecStatus, String> {
    if nsec_key.trim().is_empty() {
        return Err("NSEC cannot be empty".into());
    }
    nsec::store_nsec(&db.db_path, nsec_key.trim())?;
    Ok(NsecStatus { configured: true })
}

#[tauri::command]
pub fn delete_nsec(db: State<'_, Database>) -> NsecStatus {
    nsec::delete_nsec(&db.db_path);
    db.set_setting("setup_complete", "");
    NsecStatus { configured: false }
}
