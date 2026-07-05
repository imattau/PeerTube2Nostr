use keyring::Entry;
use std::path::PathBuf;

const KEYRING_SERVICE: &str = "peertube2nostr";

fn keyring_user(db_path: &str) -> String {
    std::fs::canonicalize(db_path)
        .unwrap_or_else(|_| PathBuf::from(db_path))
        .to_string_lossy()
        .to_string()
}

pub fn store_nsec(db_path: &str, nsec: &str) -> Result<String, String> {
    let user = keyring_user(db_path);
    let entry = Entry::new(KEYRING_SERVICE, &user).map_err(|e| format!("Failed to create keyring entry: {}", e))?;
    entry
        .set_password(nsec)
        .map_err(|e| format!("Failed to store nsec in keyring: {}", e))?;

    let secret_path = format!("{}.nsec", db_path);
    if let Err(e) = std::fs::write(&secret_path, nsec.trim()) {
        return Err(format!("Failed to write fallback nsec file: {}", e));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(&secret_path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o600);
            let _ = std::fs::set_permissions(&secret_path, perms);
        }
    }
    Ok("keyring".into())
}

pub fn get_nsec(db_path: &str) -> Option<String> {
    let user = keyring_user(db_path);
    if let Ok(entry) = Entry::new(KEYRING_SERVICE, &user) {
        if let Ok(password) = entry.get_password() {
            if !password.is_empty() {
                return Some(password);
            }
        }
    }
    let secret_path = format!("{}.nsec", db_path);
    std::fs::read_to_string(&secret_path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn has_nsec(db_path: &str) -> bool {
    get_nsec(db_path).is_some()
}

pub fn delete_nsec(db_path: &str) -> bool {
    let mut removed = false;
    let user = keyring_user(db_path);
    if let Ok(entry) = Entry::new(KEYRING_SERVICE, &user) {
        if entry.delete_credential().is_ok() {
            removed = true;
        }
    }
    let secret_path = format!("{}.nsec", db_path);
    if std::fs::remove_file(&secret_path).is_ok() {
        removed = true;
    }
    removed
}
