import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

try:
    import keyring
    import keyring.errors
except Exception:
    keyring = None

from core.utils import UrlNormaliser, DEFAULT_RELAYS


KEYRING_SERVICE = "peertube_nostr"


def _keyring_available() -> bool:
    return keyring is not None


def _keyring_user(db_path: str) -> str:
    return os.path.abspath(db_path)


def _nsec_file_path(db_path: str) -> str:
    return os.environ.get("NSEC_FILE") or (os.path.abspath(db_path) + ".nsec")


def _read_secret_file(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def _write_secret_file(path: str, value: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(value.strip() + "\n")


def get_stored_nsec(db_path: str) -> Optional[str]:
    if _keyring_available():
        try:
            nsec = keyring.get_password(KEYRING_SERVICE, _keyring_user(db_path))
            if nsec:
                return nsec
        except keyring.errors.KeyringError:
            pass
    return _read_secret_file(_nsec_file_path(db_path))


def set_stored_nsec(db_path: str, nsec: str) -> tuple:
    if _keyring_available():
        try:
            keyring.set_password(KEYRING_SERVICE, _keyring_user(db_path), nsec)
            return "keyring", None
        except keyring.errors.KeyringError:
            pass
    path = _nsec_file_path(db_path)
    _write_secret_file(path, nsec)
    return "file", path


def clear_stored_nsec(db_path: str) -> bool:
    removed = False
    if _keyring_available():
        try:
            keyring.delete_password(KEYRING_SERVICE, _keyring_user(db_path))
            removed = True
        except keyring.errors.PasswordDeleteError:
            pass
    path = _nsec_file_path(db_path)
    try:
        os.remove(path)
        removed = True
    except FileNotFoundError:
        pass
    return removed


@dataclass
class IngestedItem:
    source_id: int
    entry_key: str
    watch_url: str
    title: str
    summary: str
    peertube_base: Optional[str]
    peertube_video_id: Optional[str]
    hls_url: Optional[str]
    mp4_url: Optional[str]
    peertube_instance: Optional[str]
    channel_name: Optional[str]
    channel_url: Optional[str]
    account_name: Optional[str]
    account_url: Optional[str]
    published_ts: Optional[int]
    thumbnail_url: Optional[str] = None
    duration: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None


class Store:
    def __init__(self, db_path: str, n: UrlNormaliser) -> None:
        self.db_path = db_path
        self.n = n
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")

    def close(self) -> None:
        self.conn.close()

    def _has_column(self, table: str, col: str) -> bool:
        cur = self.conn.execute(f"PRAGMA table_info({table})")
        return any(r[1] == col for r in cur.fetchall())

    def _add_column(self, table: str, col: str, coltype: str) -> None:
        if not self._has_column(table, col):
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")

    def init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                enabled INTEGER NOT NULL DEFAULT 1,
                created_ts INTEGER NOT NULL,

                api_base TEXT,
                api_base_norm TEXT,
                api_channel TEXT,
                api_channel_url TEXT,
                api_channel_url_norm TEXT,

                rss_url TEXT,
                rss_url_norm TEXT,

                last_polled_ts INTEGER,
                last_error TEXT,
                lookback_days INTEGER
            );
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sources_enabled ON sources(enabled);")
        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_sources_api ON sources(api_base_norm, api_channel);")
        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_sources_rss ON sources(rss_url_norm);")

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS relays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relay_url TEXT NOT NULL UNIQUE,
                relay_url_norm TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_ts INTEGER NOT NULL,
                last_used_ts INTEGER,
                last_error TEXT,
                latency_ms INTEGER
            );
            """
        )
        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_relays_relay_url_norm ON relays(relay_url_norm);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_relays_enabled ON relays(enabled);")

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                entry_key TEXT NOT NULL,

                watch_url TEXT NOT NULL,
                watch_url_norm TEXT NOT NULL,

                peertube_base TEXT,
                peertube_video_id TEXT,

                peertube_instance TEXT,
                channel_name TEXT,
                channel_url TEXT,
                account_name TEXT,
                account_url TEXT,

                title TEXT,
                summary TEXT,
                hls_url TEXT,
                direct_url TEXT,
                published_ts INTEGER,

                status TEXT NOT NULL DEFAULT 'pending',
                nostr_event_id TEXT,
                error TEXT,

                first_seen_ts INTEGER NOT NULL,
                last_attempt_ts INTEGER,
                posted_ts INTEGER,

                FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE,
                UNIQUE(source_id, entry_key)
            );
            """
        )

        self._add_column("sources", "api_base_norm", "TEXT")
        self._add_column("sources", "api_channel_url_norm", "TEXT")
        self._add_column("sources", "rss_url_norm", "TEXT")
        self._add_column("sources", "lookback_days", "INTEGER")

        self._add_column("videos", "peertube_instance", "TEXT")
        self._add_column("videos", "channel_name", "TEXT")
        self._add_column("videos", "channel_url", "TEXT")
        self._add_column("videos", "account_name", "TEXT")
        self._add_column("videos", "account_url", "TEXT")
        self._add_column("videos", "published_ts", "INTEGER")
        self._add_column("relays", "latency_ms", "INTEGER")

        self._add_column("videos", "thumbnail_url", "TEXT")
        self._add_column("videos", "duration", "INTEGER")
        self._add_column("videos", "width", "INTEGER")
        self._add_column("videos", "height", "INTEGER")

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_watch_norm ON videos(watch_url_norm);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_first_seen ON videos(first_seen_ts);")

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )

        self._ensure_setting("min_publish_interval_seconds", "1200")
        self._ensure_setting("max_posts_per_hour", "3")
        self._ensure_setting("max_posts_per_day_per_source", "1")

        self.conn.commit()
        self._migrate_legacy_feeds_to_sources()

    def _migrate_legacy_feeds_to_sources(self) -> None:
        try:
            cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='feeds'")
            if not cur.fetchone():
                return
            cur2 = self.conn.execute("SELECT COUNT(*) FROM sources")
            if int(cur2.fetchone()[0]) > 0:
                return

            rows = self.conn.execute("SELECT feed_url, enabled, created_ts, last_polled_ts, last_error FROM feeds").fetchall()
            for feed_url, enabled, created_ts, last_polled_ts, last_error in rows:
                try:
                    rss_norm = self.n.normalise_feed_url(feed_url)
                except Exception:
                    rss_norm = None
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO sources
                    (enabled, created_ts, rss_url, rss_url_norm, last_polled_ts, last_error)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (enabled, created_ts or self.n.now_ts(), feed_url, rss_norm, last_polled_ts, last_error),
                )
            self.conn.commit()
        except Exception:
            pass

    # Settings
    def _ensure_setting(self, key: str, value: str) -> None:
        cur = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        if cur.fetchone() is None:
            self.conn.execute("INSERT INTO settings(key, value) VALUES (?, ?)", (key, value))

    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        return str(row[0]) if row[0] is not None else default

    def set_setting(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_publish_limits(self) -> tuple[int, int]:
        min_interval = int(self.get_setting("min_publish_interval_seconds", "1200") or "1200")
        max_per_hour = int(self.get_setting("max_posts_per_hour", "3") or "3")
        return min_interval, max_per_hour

    def get_daily_source_limit(self) -> int:
        return int(self.get_setting("max_posts_per_day_per_source", "1") or "1")

    # Relays
    def seed_default_relays_if_empty(self) -> None:
        cur = self.conn.execute("SELECT COUNT(*) FROM relays")
        if int(cur.fetchone()[0]) > 0:
            return
        ts = self.n.now_ts()
        for r in DEFAULT_RELAYS:
            norm = self.n.normalise_relay_url(r)
            self.conn.execute(
                "INSERT OR IGNORE INTO relays(relay_url, relay_url_norm, enabled, created_ts) VALUES (?, ?, 1, ?)",
                (r, norm, ts),
            )
        self.conn.commit()

    def add_relay(self, relay_url: str) -> int:
        return self.add_relay_with_enabled(relay_url, enabled=True)

    def add_relay_with_enabled(self, relay_url: str, enabled: bool) -> int:
        raw = (relay_url or "").strip()
        norm = self.n.normalise_relay_url(raw)
        ts = self.n.now_ts()
        enabled_val = 1 if enabled else 0
        self.conn.execute(
            "INSERT OR IGNORE INTO relays(relay_url, relay_url_norm, enabled, created_ts) VALUES (?, ?, ?, ?)",
            (raw, norm, enabled_val, ts),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT id FROM relays WHERE relay_url_norm=?", (norm,)).fetchone()
        if not row:
            raise RuntimeError("Failed to add relay")
        return int(row[0])

    def remove_relay(self, relay_id_or_url: str) -> int:
        s = (relay_id_or_url or "").strip()
        try:
            rid = int(s)
            cur = self.conn.execute("DELETE FROM relays WHERE id=?", (rid,))
        except ValueError:
            norm = self.n.normalise_relay_url(s)
            cur = self.conn.execute("DELETE FROM relays WHERE relay_url_norm=?", (norm,))
        self.conn.commit()
        return cur.rowcount

    def remove_source(self, source_id: int) -> int:
        cur = self.conn.execute("DELETE FROM sources WHERE id=?", (source_id,))
        self.conn.commit()
        return cur.rowcount

    def update_relay_url(self, relay_id_or_url: str, new_relay_url: str) -> int:
        s = (relay_id_or_url or "").strip()
        new_raw = (new_relay_url or "").strip()
        new_norm = self.n.normalise_relay_url(new_raw)
        try:
            rid = int(s)
            cur = self.conn.execute(
                "UPDATE relays SET relay_url=?, relay_url_norm=? WHERE id=?",
                (new_raw, new_norm, rid),
            )
        except ValueError:
            old_norm = self.n.normalise_relay_url(s)
            cur = self.conn.execute(
                "UPDATE relays SET relay_url=?, relay_url_norm=? WHERE relay_url_norm=?",
                (new_raw, new_norm, old_norm),
            )
        self.conn.commit()
        return cur.rowcount

    def set_relay_enabled(self, relay_id_or_url: str, enabled: bool) -> int:
        s = (relay_id_or_url or "").strip()
        val = 1 if enabled else 0
        try:
            rid = int(s)
            cur = self.conn.execute("UPDATE relays SET enabled=? WHERE id=?", (val, rid))
        except ValueError:
            norm = self.n.normalise_relay_url(s)
            cur = self.conn.execute("UPDATE relays SET enabled=? WHERE relay_url_norm=?", (val, norm))
        self.conn.commit()
        return cur.rowcount

    def list_relays(self) -> list[tuple]:
        return self.conn.execute(
            "SELECT id, enabled, relay_url, relay_url_norm, last_used_ts, last_error, latency_ms FROM relays ORDER BY id ASC"
        ).fetchall()

    def get_enabled_relays(self) -> list[str]:
        cur = self.conn.execute("SELECT relay_url FROM relays WHERE enabled=1 ORDER BY id ASC")
        out: list[str] = []
        for (u,) in cur.fetchall():
            try:
                out.append(self.n.normalise_relay_url(u))
            except Exception:
                out.append(str(u))
        return out

    def mark_relay_used(self, relay_url: str, error: Optional[str]) -> None:
        ts = self.n.now_ts()
        try:
            norm = self.n.normalise_relay_url(relay_url)
        except Exception:
            norm = None
        if error:
            self.conn.execute(
                "UPDATE relays SET last_used_ts=?, last_error=?, latency_ms=NULL WHERE relay_url_norm=? OR relay_url=?",
                (ts, error[:1000], norm, relay_url),
            )
        else:
            self.conn.execute(
                "UPDATE relays SET last_used_ts=?, last_error=NULL WHERE relay_url_norm=? OR relay_url=?",
                (ts, norm, relay_url),
            )
        self.conn.commit()

    def update_relay_latency(self, relay_url: str, latency_ms: int) -> None:
        ts = self.n.now_ts()
        try:
            norm = self.n.normalise_relay_url(relay_url)
        except Exception:
            norm = None
        self.conn.execute(
            "UPDATE relays SET last_used_ts=?, last_error=NULL, latency_ms=? WHERE relay_url_norm=? OR relay_url=?",
            (ts, latency_ms, norm, relay_url),
        )
        self.conn.commit()

    # Sources
    def add_channel_source(self, channel_url: str) -> int:
        base, channel = self.n.extract_channel_ref(channel_url)
        base_norm = self.n.normalise_http_url(base)
        chan_url_norm = self.n.normalise_http_url(channel_url)

        ts = self.n.now_ts()
        self.conn.execute(
            """
            INSERT OR IGNORE INTO sources
            (enabled, created_ts, api_base, api_base_norm, api_channel, api_channel_url, api_channel_url_norm)
            VALUES (1, ?, ?, ?, ?, ?, ?)
            """,
            (ts, base, base_norm, channel, channel_url, chan_url_norm),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id FROM sources WHERE api_base_norm=? AND api_channel=?",
            (base_norm, channel),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to add channel source")
        return int(row[0])

    def add_rss_source(self, rss_url: str) -> int:
        raw = (rss_url or "").strip()
        rss_norm = self.n.normalise_feed_url(raw)
        ts = self.n.now_ts()
        self.conn.execute(
            """
            INSERT OR IGNORE INTO sources
            (enabled, created_ts, rss_url, rss_url_norm)
            VALUES (1, ?, ?, ?)
            """,
            (ts, raw, rss_norm),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT id FROM sources WHERE rss_url_norm=?", (rss_norm,)).fetchone()
        if not row:
            raise RuntimeError("Failed to add RSS source")
        return int(row[0])

    def set_source_rss(self, source_id: int, rss_url: str) -> int:
        raw = (rss_url or "").strip()
        rss_norm = self.n.normalise_feed_url(raw)
        cur = self.conn.execute(
            "UPDATE sources SET rss_url=?, rss_url_norm=? WHERE id=?",
            (raw, rss_norm, source_id),
        )
        self.conn.commit()
        return cur.rowcount

    def clear_source_rss(self, source_id: int) -> int:
        cur = self.conn.execute(
            "UPDATE sources SET rss_url=NULL, rss_url_norm=NULL WHERE id=?",
            (source_id,),
        )
        self.conn.commit()
        return cur.rowcount

    def set_source_channel(self, source_id: int, channel_url: str) -> int:
        base, channel = self.n.extract_channel_ref(channel_url)
        base_norm = self.n.normalise_http_url(base)
        chan_url_norm = self.n.normalise_http_url(channel_url)
        cur = self.conn.execute(
            """
            UPDATE sources
            SET api_base=?, api_base_norm=?, api_channel=?, api_channel_url=?, api_channel_url_norm=?
            WHERE id=?
            """,
            (base, base_norm, channel, channel_url, chan_url_norm, source_id),
        )
        self.conn.commit()
        return cur.rowcount

    def clear_source_channel(self, source_id: int) -> int:
        cur = self.conn.execute(
            """
            UPDATE sources
            SET api_base=NULL, api_base_norm=NULL, api_channel=NULL, api_channel_url=NULL, api_channel_url_norm=NULL
            WHERE id=?
            """,
            (source_id,),
        )
        self.conn.commit()
        return cur.rowcount

    def set_source_enabled(self, source_id: int, enabled: bool) -> int:
        val = 1 if enabled else 0
        cur = self.conn.execute("UPDATE sources SET enabled=? WHERE id=?", (val, source_id))
        self.conn.commit()
        return cur.rowcount

    def list_sources(self) -> list[tuple]:
        return self.conn.execute(
            """
            SELECT id, enabled,
                   api_base, api_channel, api_channel_url,
                   rss_url, lookback_days,
                   last_polled_ts, last_error
            FROM sources
            ORDER BY id ASC
            """
        ).fetchall()

    def get_source_by_id(self, source_id: int) -> Optional[dict]:
        row = self.conn.execute(
            """
            SELECT id, enabled,
                   api_base, api_channel, api_channel_url,
                   rss_url, lookback_days,
                   last_polled_ts
            FROM sources
            WHERE id=?
            """,
            (source_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "enabled": int(row[1]),
            "api_base": row[2],
            "api_channel": row[3],
            "api_channel_url": row[4],
            "rss_url": row[5],
            "lookback_days": row[6],
            "last_polled_ts": row[7],
        }

    def get_enabled_sources(self) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT id, api_base, api_channel, api_channel_url, rss_url, last_polled_ts, lookback_days
            FROM sources
            WHERE enabled=1
            ORDER BY id ASC
            """
        ).fetchall()
        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "api_base": r[1],
                    "api_channel": r[2],
                    "api_channel_url": r[3],
                    "rss_url": r[4],
                    "last_polled_ts": r[5],
                    "lookback_days": r[6],
                }
            )
        return out

    def set_source_lookback(self, source_id: int, lookback_days: Optional[int]) -> int:
        cur = self.conn.execute(
            "UPDATE sources SET lookback_days=? WHERE id=?",
            (lookback_days, source_id),
        )
        self.conn.commit()
        return cur.rowcount

    def mark_source_polled(self, source_id: int, error: Optional[str]) -> None:
        ts = self.n.now_ts()
        self.conn.execute(
            "UPDATE sources SET last_polled_ts=?, last_error=? WHERE id=?",
            (ts, (error[:1000] if error else None), source_id),
        )
        self.conn.commit()

    # Videos
    def get_metrics(self) -> dict:
        now_ts = int(time.time())
        day_start = now_ts - 86400
        return {
            "pending": self.count_pending(),
            "posted_today": self.count_posted_since(day_start),
            "failed": self.count_failed(),
            "active_sources": self.count_sources(),
        }

    def count_pending(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM videos WHERE status='pending'").fetchone()
        return int(row[0]) if row else 0

    def count_posted(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM videos WHERE status='posted'").fetchone()
        return int(row[0]) if row else 0

    def count_failed(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM videos WHERE status='failed'").fetchone()
        return int(row[0]) if row else 0

    def count_posted_since(self, since_ts: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM videos WHERE status='posted' AND posted_ts >= ?",
            (since_ts,),
        ).fetchone()
        return int(row[0]) if row else 0

    def count_posted_by_source_since(self, since_ts: int) -> dict[int, int]:
        rows = self.conn.execute(
            "SELECT source_id, COUNT(*) FROM videos WHERE status='posted' AND posted_ts >= ? GROUP BY source_id",
            (since_ts,),
        ).fetchall()
        return {int(r[0]): int(r[1]) for r in rows}

    def count_posted_since_for_source(self, source_id: int, since_ts: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM videos WHERE status='posted' AND source_id=? AND posted_ts >= ?",
            (source_id, since_ts),
        ).fetchone()
        return int(row[0]) if row else 0

    def list_pending(self, limit: int = 10) -> list[tuple]:
        return self.conn.execute(
            """
            SELECT v.id, v.source_id, v.title, v.watch_url, v.first_seen_ts, v.published_ts,
                   s.api_base, s.api_channel, s.rss_url
            FROM videos v
            JOIN sources s ON s.id = v.source_id
            WHERE v.status='pending' AND s.enabled=1
            ORDER BY (v.published_ts IS NULL) ASC, v.published_ts ASC, v.first_seen_ts ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def list_videos(self, status: str = 'pending', limit: int = 200) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT v.id, v.source_id, v.watch_url, v.title, v.summary,
                   v.hls_url, v.direct_url, v.thumbnail_url, v.duration, v.width, v.height,
                   v.peertube_instance, v.channel_name, v.channel_url,
                   v.account_name, v.account_url,
                   v.status, v.nostr_event_id, v.error,
                   v.first_seen_ts, v.last_attempt_ts, v.posted_ts, v.published_ts
            FROM videos v
            WHERE v.status=?
            ORDER BY v.first_seen_ts DESC
            LIMIT ?
            """,
            (status, limit),
        ).fetchall()
        keys = [
            "id", "source_id", "watch_url", "title", "summary",
            "hls_url", "direct_url", "thumbnail_url", "duration", "width", "height",
            "peertube_instance", "channel_name", "channel_url",
            "account_name", "account_url",
            "status", "nostr_event_id", "error",
            "first_seen_ts", "last_attempt_ts", "posted_ts", "published_ts",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def oldest_posted_since(self, since_ts: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MIN(posted_ts) FROM videos WHERE status='posted' AND posted_ts >= ?",
            (since_ts,),
        ).fetchone()
        if not row:
            return None
        val = row[0]
        return int(val) if val else None

    def oldest_posted_since_for_source(self, source_id: int, since_ts: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MIN(posted_ts) FROM videos WHERE status='posted' AND source_id=? AND posted_ts >= ?",
            (source_id, since_ts),
        ).fetchone()
        if not row:
            return None
        val = row[0]
        return int(val) if val else None

    def last_polled_ts(self) -> Optional[int]:
        row = self.conn.execute("SELECT MAX(last_polled_ts) FROM sources").fetchone()
        if not row:
            return None
        val = row[0]
        return int(val) if val else None

    def last_posted_ts(self) -> Optional[int]:
        row = self.conn.execute("SELECT MAX(posted_ts) FROM videos WHERE status='posted'").fetchone()
        if not row:
            return None
        val = row[0]
        return int(val) if val else None

    def count_sources(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM sources").fetchone()
        return int(row[0]) if row else 0

    def count_relays(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM relays").fetchone()
        return int(row[0]) if row else 0

    def video_exists(self, source_id: int, entry_key: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM videos WHERE source_id=? AND entry_key=? LIMIT 1",
            (source_id, entry_key),
        ).fetchone()
        return row is not None

    def insert_pending(self, item: IngestedItem) -> None:
        ts = self.n.now_ts()
        self.conn.execute(
            """
            INSERT OR IGNORE INTO videos
            (source_id, entry_key, watch_url, watch_url_norm,
             peertube_base, peertube_video_id,
             peertube_instance, channel_name, channel_url, account_name, account_url,
             title, summary, hls_url, direct_url,
             published_ts, thumbnail_url, duration, width, height,
             status, first_seen_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (
                item.source_id,
                item.entry_key,
                item.watch_url,
                self.n.normalise_watch_url(item.watch_url),
                item.peertube_base,
                item.peertube_video_id,
                item.peertube_instance,
                item.channel_name,
                item.channel_url,
                item.account_name,
                item.account_url,
                item.title,
                item.summary,
                item.hls_url,
                item.mp4_url,
                item.published_ts,
                item.thumbnail_url,
                item.duration,
                item.width,
                item.height,
                ts,
            ),
        )
        self.conn.commit()

    def update_published_ts_if_null(self, source_id: int, entry_key: str, published_ts: int) -> None:
        self.conn.execute(
            """
            UPDATE videos
            SET published_ts=?
            WHERE source_id=? AND entry_key=? AND published_ts IS NULL
            """,
            (published_ts, source_id, entry_key),
        )
        self.conn.commit()

    def next_pending(self) -> Optional[dict]:
        row = self.conn.execute(
            """
            SELECT v.id, v.source_id, v.watch_url, v.title, v.summary, v.hls_url, v.direct_url,
                   v.peertube_instance, v.channel_name, v.channel_url, v.account_name, v.account_url,
                   v.thumbnail_url, v.duration, v.width, v.height
            FROM videos v
            JOIN sources s ON s.id = v.source_id
            WHERE v.status='pending' AND s.enabled=1
            ORDER BY (v.published_ts IS NULL) ASC, v.published_ts ASC, v.first_seen_ts ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        keys = [
            "id", "source_id", "watch_url", "title", "summary", "hls_url", "direct_url",
            "peertube_instance", "channel_name", "channel_url", "account_name", "account_url",
            "thumbnail_url", "duration", "width", "height",
        ]
        return dict(zip(keys, row))

    def next_pending_eligible(self, now_ts: int, max_per_day_per_source: int, limit: int = 200) -> Optional[dict]:
        if max_per_day_per_source <= 0:
            return self.next_pending()
        rows = self.conn.execute(
            """
            SELECT v.id, v.source_id, v.watch_url, v.title, v.summary, v.hls_url, v.direct_url,
                   v.peertube_instance, v.channel_name, v.channel_url, v.account_name, v.account_url,
                   v.thumbnail_url, v.duration, v.width, v.height
            FROM videos v
            JOIN sources s ON s.id = v.source_id
            WHERE v.status='pending' AND s.enabled=1
            ORDER BY (v.published_ts IS NULL) ASC, v.published_ts ASC, v.first_seen_ts ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        if not rows:
            return None
        counts = self.count_posted_by_source_since(now_ts - 86400)
        keys = [
            "id", "source_id", "watch_url", "title", "summary", "hls_url", "direct_url",
            "peertube_instance", "channel_name", "channel_url", "account_name", "account_url",
            "thumbnail_url", "duration", "width", "height",
        ]
        for row in rows:
            sid = int(row[1])
            if int(counts.get(sid, 0)) < max_per_day_per_source:
                return dict(zip(keys, row))
        return None

    def mark_posted(self, video_row_id: int, event_id: str) -> None:
        ts = self.n.now_ts()
        self.conn.execute(
            """
            UPDATE videos
            SET status='posted', nostr_event_id=?, posted_ts=?, last_attempt_ts=?, error=NULL
            WHERE id=?
            """,
            (event_id, ts, ts, video_row_id),
        )
        self.conn.commit()

    def mark_failed(self, video_row_id: int, err: str) -> None:
        ts = self.n.now_ts()
        self.conn.execute(
            "UPDATE videos SET status='failed', error=?, last_attempt_ts=? WHERE id=?",
            (err[:2000], ts, video_row_id),
        )
        self.conn.commit()

    def clear_pending_for_source(self, source_id: int) -> int:
        ts = self.n.now_ts()
        cur = self.conn.execute(
            """
            UPDATE videos
            SET status='cancelled', error='cleared by resync', last_attempt_ts=?
            WHERE source_id=? AND status='pending'
            """,
            (ts, source_id),
        )
        self.conn.commit()
        return cur.rowcount

    def retry_failed(self, older_than_seconds: int) -> int:
        ts = self.n.now_ts()
        cutoff = ts - older_than_seconds
        cur = self.conn.execute(
            """
            UPDATE videos
            SET status='pending'
            WHERE status='failed' AND (last_attempt_ts IS NULL OR last_attempt_ts < ?)
            """,
            (cutoff,),
        )
        self.conn.commit()
        return cur.rowcount

    def retry_failed_for_source(self, source_id: int, older_than_seconds: int) -> int:
        ts = self.n.now_ts()
        cutoff = ts - older_than_seconds
        cur = self.conn.execute(
            """
            UPDATE videos
            SET status='pending'
            WHERE status='failed'
              AND source_id=?
              AND (last_attempt_ts IS NULL OR last_attempt_ts < ?)
            """,
            (source_id, cutoff),
        )
        self.conn.commit()
        return cur.rowcount

    def reset_all(self) -> None:
        self.conn.execute("DELETE FROM videos")
        self.conn.execute("DELETE FROM sources")
        self.conn.execute("DELETE FROM relays")
        self.conn.execute("DELETE FROM settings")
        self._ensure_setting("min_publish_interval_seconds", "1200")
        self._ensure_setting("max_posts_per_hour", "3")
        self._ensure_setting("max_posts_per_day_per_source", "1")
        self.conn.commit()
        self.seed_default_relays_if_empty()
