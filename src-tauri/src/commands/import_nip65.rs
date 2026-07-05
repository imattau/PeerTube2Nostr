use std::process::Command;
use tauri::State;

use crate::db::Database;
use crate::nsec;

#[tauri::command]
pub fn import_nip65_relays(
    db: State<'_, Database>,
) -> Result<ImportedResult, String> {
    let nsec_key = nsec::get_nsec(&db.db_path).ok_or("No NSEC configured")?;
    let bootstrap_urls = db.get_enabled_relays();
    if bootstrap_urls.is_empty() {
        return Err("No bootstrap relays configured".into());
    }

    let script = format!(
        r#"
import sys
sys.path.insert(0, 'webapp/backend')
sys.path.insert(0, '.')
from core.database import Store, get_stored_nsec
from core.sync import import_nip65_relays
from core.utils import UrlNormaliser

n = UrlNormaliser()
store = Store({db_path:?}, n)
store.init_schema()
nsec = get_stored_nsec({db_path:?})
count = import_nip65_relays(nsec, store, n, {relays:?}, log_fn=print)
print(count)
store.close()
"#,
        db_path = db.db_path,
        relays = bootstrap_urls,
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&script)
        .output()
        .map_err(|e| format!("Failed to run Python importer: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Python import failed: {}", stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let count: usize = stdout.trim().parse().unwrap_or(0);

    Ok(ImportedResult { imported: count })
}

#[derive(serde::Serialize)]
pub struct ImportedResult {
    pub imported: usize,
}
