use rusqlite::{Connection, params};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::models::*;

pub struct Database {
    pub conn: Mutex<Connection>,
    pub db_path: String,
}

impl Database {
    pub fn new() -> Result<Self, String> {
        let db_path = Self::get_db_path();
        let conn = Connection::open(&db_path).map_err(|e| format!("Failed to open DB: {}", e))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| format!("Failed to set pragmas: {}", e))?;
        let db = Database { conn: Mutex::new(conn), db_path: db_path.to_string_lossy().to_string() };
        db.init_schema()?;
        Ok(db)
    }

    fn get_db_path() -> PathBuf {
        std::env::var("PEERTUBE2NOSTR_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let data_dir = directories::ProjectDirs::from("com", "peertube2nostr", "peertube2nostr")
                    .map(|d| d.data_dir().to_path_buf())
                    .unwrap_or_else(|| PathBuf::from("."));
                std::fs::create_dir_all(&data_dir).ok();
                data_dir.join("peertube2nostr.db")
            })
    }

    fn init_schema(&self) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_ts INTEGER NOT NULL,
                api_base TEXT, api_base_norm TEXT,
                api_channel TEXT, api_channel_url TEXT, api_channel_url_norm TEXT,
                rss_url TEXT, rss_url_norm TEXT,
                last_polled_ts INTEGER, last_error TEXT, lookback_days INTEGER
            );
            CREATE TABLE IF NOT EXISTS relays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relay_url TEXT NOT NULL UNIQUE, relay_url_norm TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_ts INTEGER NOT NULL,
                last_used_ts INTEGER, last_error TEXT, latency_ms INTEGER
            );
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL, entry_key TEXT NOT NULL,
                watch_url TEXT NOT NULL, watch_url_norm TEXT NOT NULL,
                peertube_base TEXT, peertube_video_id TEXT,
                peertube_instance TEXT, channel_name TEXT, channel_url TEXT,
                account_name TEXT, account_url TEXT,
                title TEXT, summary TEXT, hls_url TEXT, direct_url TEXT,
                published_ts INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                nostr_event_id TEXT, error TEXT,
                first_seen_ts INTEGER NOT NULL,
                last_attempt_ts INTEGER, posted_ts INTEGER,
                thumbnail_url TEXT, duration INTEGER, width INTEGER, height INTEGER,
                FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE,
                UNIQUE(source_id, entry_key)
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value TEXT
            );
        ").map_err(|e| format!("Schema creation failed: {}", e))?;

        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES ('min_publish_interval_seconds', '1200')",
            [],
        ).ok();
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES ('max_posts_per_hour', '3')",
            [],
        ).ok();
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES ('max_posts_per_day_per_source', '1')",
            [],
        ).ok();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM relays", [], |r| r.get(0))
            .unwrap_or(0);
        if count == 0 {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            for relay in ["wss://relay.damus.io", "wss://nos.lol"] {
                conn.execute(
                    "INSERT OR IGNORE INTO relays(relay_url, relay_url_norm, enabled, created_ts) VALUES (?1, ?1, 1, ?2)",
                    params![relay, ts],
                ).ok();
            }
        }

        Ok(())
    }

    // --- Settings ---

    pub fn get_setting(&self, key: &str, default: &str) -> String {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM settings WHERE key=?1",
            params![key],
            |r| r.get::<_, String>(0),
        ).unwrap_or(default.to_string())
    }

    pub fn set_setting(&self, key: &str, value: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        ).ok();
    }

    pub fn get_publish_limits(&self) -> (i64, i64) {
        let min_int = self.get_setting("min_publish_interval_seconds", "1200").parse().unwrap_or(1200);
        let max_hour = self.get_setting("max_posts_per_hour", "3").parse().unwrap_or(3);
        (min_int, max_hour)
    }

    pub fn get_daily_source_limit(&self) -> i64 {
        self.get_setting("max_posts_per_day_per_source", "1").parse().unwrap_or(1)
    }

    // --- Sources ---

    pub fn list_sources(&self) -> Vec<Source> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, enabled, api_base, api_channel, api_channel_url, rss_url, lookback_days, last_polled_ts, last_error FROM sources ORDER BY id ASC"
        ).unwrap();
        stmt.query_map([], |row| {
            Ok(Source {
                id: row.get(0)?,
                enabled: row.get::<_, i64>(1)? != 0,
                api_base: row.get(2)?,
                api_channel: row.get(3)?,
                api_channel_url: row.get(4)?,
                rss_url: row.get(5)?,
                lookback_days: row.get(6)?,
                last_polled_ts: row.get(7)?,
                last_error: row.get(8)?,
            })
        }).unwrap().filter_map(|r| r.ok()).collect()
    }

    pub fn add_channel_source(&self, channel_url: &str, api_base: &str, channel: &str) -> Result<i64, String> {
        let conn = self.conn.lock().unwrap();
        let ts = now_ts();
        conn.execute(
            "INSERT OR IGNORE INTO sources (enabled, created_ts, api_base, api_base_norm, api_channel, api_channel_url, api_channel_url_norm) VALUES (1, ?1, ?2, ?2, ?3, ?4, ?4)",
            params![ts, api_base, channel, channel_url],
        ).map_err(|e| format!("Failed to add source: {}", e))?;
        let id = conn.last_insert_rowid();
        Ok(id)
    }

    pub fn set_source_enabled(&self, id: i64, enabled: bool) -> bool {
        let conn = self.conn.lock().unwrap();
        let val: i64 = if enabled { 1 } else { 0 };
        conn.execute("UPDATE sources SET enabled=?1 WHERE id=?2", params![val, id]).ok();
        true
    }

    pub fn remove_source(&self, id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM sources WHERE id=?1", params![id]).ok();
        true
    }

    // --- Relays ---

    pub fn list_relays(&self) -> Vec<Relay> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, enabled, relay_url, relay_url_norm, last_used_ts, last_error, latency_ms FROM relays ORDER BY id ASC"
        ).unwrap();
        stmt.query_map([], |row| {
            Ok(Relay {
                id: row.get(0)?,
                enabled: row.get::<_, i64>(1)? != 0,
                relay_url: row.get(2)?,
                relay_url_norm: row.get(3)?,
                last_used_ts: row.get(4)?,
                last_error: row.get(5)?,
                latency_ms: row.get(6)?,
            })
        }).unwrap().filter_map(|r| r.ok()).collect()
    }

    pub fn add_relay(&self, url: &str, norm: &str) -> Result<i64, String> {
        let conn = self.conn.lock().unwrap();
        let ts = now_ts();
        conn.execute(
            "INSERT OR IGNORE INTO relays(relay_url, relay_url_norm, enabled, created_ts) VALUES (?1, ?2, 1, ?3)",
            params![url, norm, ts],
        ).map_err(|e| format!("Failed to add relay: {}", e))?;
        let id = conn.last_insert_rowid();
        if id == 0 {
            let existing: i64 = conn.query_row(
                "SELECT id FROM relays WHERE relay_url_norm=?1", params![norm], |r| r.get(0),
            ).unwrap_or(0);
            if existing > 0 { return Ok(existing); }
            return Err("Failed to add relay".into());
        }
        Ok(id)
    }

    pub fn remove_relay(&self, id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM relays WHERE id=?1", params![id]).ok();
        true
    }

    pub fn set_relay_enabled(&self, id: i64, enabled: bool) -> bool {
        let conn = self.conn.lock().unwrap();
        let val: i64 = if enabled { 1 } else { 0 };
        conn.execute("UPDATE relays SET enabled=?1 WHERE id=?2", params![val, id]).ok();
        true
    }

    // --- Videos / Queue ---

    pub fn list_videos(&self, status: &str, limit: i64) -> Vec<Video> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_id, watch_url, title, summary, hls_url, direct_url, thumbnail_url, duration, width, height, peertube_instance, channel_name, channel_url, account_name, account_url, status, nostr_event_id, error, first_seen_ts, last_attempt_ts, posted_ts, published_ts FROM videos WHERE status=?1 ORDER BY first_seen_ts DESC LIMIT ?2"
        ).unwrap();
        stmt.query_map(params![status, limit], |row| {
            Ok(Video {
                id: row.get(0)?, source_id: row.get(1)?,
                watch_url: row.get(2)?, title: row.get(3)?,
                summary: row.get(4)?, hls_url: row.get(5)?,
                direct_url: row.get(6)?, thumbnail_url: row.get(7)?,
                duration: row.get(8)?, width: row.get(9)?, height: row.get(10)?,
                peertube_instance: row.get(11)?, channel_name: row.get(12)?,
                channel_url: row.get(13)?, account_name: row.get(14)?,
                account_url: row.get(15)?, status: row.get(16)?,
                nostr_event_id: row.get(17)?, error: row.get(18)?,
                first_seen_ts: row.get(19)?, last_attempt_ts: row.get(20)?,
                posted_ts: row.get(21)?, published_ts: row.get(22)?,
            })
        }).unwrap().filter_map(|r| r.ok()).collect()
    }

    pub fn count_pending(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM videos WHERE status='pending'", [], |r| r.get(0)).unwrap_or(0)
    }

    pub fn count_posted(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM videos WHERE status='posted'", [], |r| r.get(0)).unwrap_or(0)
    }

    pub fn count_failed(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM videos WHERE status='failed'", [], |r| r.get(0)).unwrap_or(0)
    }

    pub fn count_posted_since(&self, since_ts: i64) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM videos WHERE status='posted' AND posted_ts >= ?1",
            params![since_ts],
            |r| r.get(0),
        ).unwrap_or(0)
    }

    pub fn count_sources(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM sources", [], |r| r.get(0)).unwrap_or(0)
    }

    pub fn count_relays(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM relays", [], |r| r.get(0)).unwrap_or(0)
    }

    pub fn last_polled_ts(&self) -> Option<i64> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT MAX(last_polled_ts) FROM sources", [], |r| r.get(0)).ok()
    }

    pub fn last_posted_ts(&self) -> Option<i64> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT MAX(posted_ts) FROM videos WHERE status='posted'", [], |r| r.get(0)).ok()
    }

    pub fn retry_failed(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        let _ts = now_ts();
        conn.execute(
            "UPDATE videos SET status='pending' WHERE status='failed'",
            [],
        ).unwrap_or(0) as i64
    }
}

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
