#!/usr/bin/env python3
"""
PeerTube -> Nostr publisher (single file, classes, SQLite)
Primary ingest = PeerTube API (channel videos)
Fallback ingest = RSS/Atom feed (if API fails or not configured)

Key points
- Store "sources" in DB (a source can be API-configured channel, and/or RSS feed)
- Ingest flow per source:
  1) Try API: /api/v1/video-channels/<channel>/videos (primary)
  2) If API not configured or errors, try RSS feed URL (fallback)
- MP4-first for Nostr embed, HLS fallback, watch URL always included
- Feeds/relays validated, canonicalised, de-duped using *_url_norm unique indexes

Dependencies:
  pip install requests feedparser pynostr

Examples
  python peertube_nostr.py init --db peertube.db
  python peertube_nostr.py add-relay wss://relay.damus.io --db peertube.db

  # Preferred: add channel by URL (API primary), optionally add RSS fallback too
  python peertube_nostr.py add-channel "https://example.tube/c/mychannel" --db peertube.db
  python peertube_nostr.py set-rss 1 "https://example.tube/feeds/videos.xml?channelId=123" --db peertube.db

  # Or add RSS-only source (will rely on RSS listing)
  python peertube_nostr.py add-rss "https://example.tube/feeds/videos.xml?channelId=123" --db peertube.db

  NOSTR_NSEC="nsec1..." python peertube_nostr.py run --db peertube.db
"""

import argparse
import calendar
import getpass
import json
import os
import re
import sqlite3
import sys
import threading
import time
import shlex
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from queue import Queue, Empty
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import urlparse, urlunparse

import feedparser
import requests
from pynostr.event import Event
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager
try:
    import keyring
    import keyring.errors
except Exception:  # pragma: no cover - optional dependency
    keyring = None
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.styles import Style
except Exception:  # pragma: no cover - optional dependency
    PromptSession = None
try:
    from textual.app import App, ComposeResult
    from textual.containers import Vertical, Horizontal
    from textual.widgets import Header, Footer, Input, Static, RichLog, ListView, ListItem, Label
except Exception:  # pragma: no cover - optional dependency
    App = None


DEFAULT_RELAYS = ["wss://relay.damus.io", "wss://nos.lol"]
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


def set_stored_nsec(db_path: str, nsec: str) -> Tuple[str, Optional[str]]:
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


class UrlNormaliser:
    ALLOWED_RELAY_SCHEMES = {"wss", "ws"}
    ALLOWED_HTTP_SCHEMES = {"https", "http"}

    def __init__(self) -> None:
        self._watch_patterns = [
            re.compile(r"/videos/watch/([A-Za-z0-9_-]+)"),
            re.compile(r"/w/([A-Za-z0-9_-]+)"),
        ]
        # PeerTube channel URL patterns seen in the wild
        self._channel_patterns = [
            re.compile(r"^/c/([^/]+)$"),
            re.compile(r"^/c/([^/]+)/videos$"),
            re.compile(r"^/video-channels/([^/]+)$"),
            re.compile(r"^/video-channels/([^/]+)/videos$"),
            re.compile(r"^/accounts/([^/]+)$"),  # sometimes used, not always a channel
        ]

    @staticmethod
    def now_ts() -> int:
        return int(time.time())

    def _normalise_url(self, url: str) -> str:
        raw = (url or "").strip()
        if not raw:
            raise ValueError("URL is empty")

        p = urlparse(raw)
        if not p.scheme or not p.netloc:
            raise ValueError(f"Invalid URL: {raw}")

        scheme = p.scheme.lower()
        host = p.hostname.lower() if p.hostname else ""
        if not host:
            raise ValueError(f"Invalid URL host: {raw}")

        port = p.port
        if (scheme == "https" and port == 443) or (scheme == "http" and port == 80) or (scheme == "wss" and port == 443) or (scheme == "ws" and port == 80):
            port = None

        netloc = host if port is None else f"{host}:{port}"

        path = p.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        return urlunparse((scheme, netloc, path, "", p.query or "", ""))

    def normalise_http_url(self, url: str) -> str:
        u = self._normalise_url(url)
        p = urlparse(u)
        if p.scheme not in self.ALLOWED_HTTP_SCHEMES:
            raise ValueError("URL must be http or https")
        return u

    def normalise_feed_url(self, url: str) -> str:
        return self.normalise_http_url(url)

    def normalise_relay_url(self, url: str) -> str:
        u = self._normalise_url(url)
        p = urlparse(u)
        if p.scheme not in self.ALLOWED_RELAY_SCHEMES:
            raise ValueError("Relay URL must be ws or wss")
        return u

    @staticmethod
    def looks_like_peertube_feed(url: str) -> bool:
        u = (url or "").lower()
        return "/feeds/" in u or "feeds/videos" in u or "videos.xml" in u

    @staticmethod
    def normalise_watch_url(url: str) -> str:
        u = urlparse((url or "").strip())
        return f"{u.scheme}://{u.netloc}{u.path}" + (f"?{u.query}" if u.query else "")

    @staticmethod
    def normalise_base(url: str) -> str:
        u = urlparse(url)
        return f"{u.scheme}://{u.netloc}"

    def extract_watch_id(self, watch_url: str) -> Optional[Tuple[str, str]]:
        base = self.normalise_base(watch_url)
        path = urlparse(watch_url).path
        for pat in self._watch_patterns:
            m = pat.search(path)
            if m:
                return base, m.group(1)
        return None

    def extract_channel_ref(self, channel_url: str) -> Tuple[str, str]:
        """
        Takes a channel URL like:
          https://instance.tld/c/mychannel
          https://instance.tld/video-channels/mychannel
        Returns:
          (base, channel_handle) where channel_handle is the last segment.
        """
        u = self.normalise_http_url(channel_url)
        p = urlparse(u)
        path = p.path.rstrip("/")
        if path.endswith("/videos"):
            path = path[: -len("/videos")]
        for pat in self._channel_patterns:
            m = pat.match(path)
            if m:
                return f"{p.scheme}://{p.netloc}", m.group(1)
        # Best-effort: take last segment if it looks plausible
        seg = path.strip("/").split("/")[-1] if path.strip("/") else ""
        if not seg:
            raise ValueError("Could not extract channel handle from URL")
        return f"{p.scheme}://{p.netloc}", seg


class Store:
    def __init__(self, db_path: str, n: UrlNormaliser) -> None:
        self.db_path = db_path
        self.n = n
        self.conn = sqlite3.connect(db_path)
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
        # "sources" replaces the old "feeds" concept:
        # - api_base + api_channel is the primary ingest config
        # - rss_url is optional fallback ingest
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                enabled INTEGER NOT NULL DEFAULT 1,
                created_ts INTEGER NOT NULL,

                api_base TEXT,
                api_base_norm TEXT,
                api_channel TEXT,              -- channel handle/name
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
                last_error TEXT
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
                direct_url TEXT,  -- MP4 preferred
                published_ts INTEGER,

                status TEXT NOT NULL DEFAULT 'pending', -- pending|posted|failed|cancelled
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

        # Migrations for older DBs (safe no-ops for new DBs)
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

        # Best-effort migration from legacy "feeds" table if it exists:
        # If a user had an older DB, keep it simple: treat feed_url as rss_url source.
        self._migrate_legacy_feeds_to_sources()

    def _migrate_legacy_feeds_to_sources(self) -> None:
        try:
            cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='feeds'")
            if not cur.fetchone():
                return
            # If sources already has rows, do not migrate again
            cur2 = self.conn.execute("SELECT COUNT(*) FROM sources")
            if int(cur2.fetchone()[0]) > 0:
                return

            # Move enabled feed_url into sources.rss_url
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
            # ignore migration failures
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
            "SELECT id, enabled, relay_url, relay_url_norm, last_used_ts, last_error FROM relays ORDER BY id ASC"
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
        self.conn.execute(
            "UPDATE relays SET last_used_ts=?, last_error=? WHERE relay_url_norm=? OR relay_url=?",
            (ts, (error[:1000] if error else None), norm, relay_url),
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

    def set_source_rss(self, source_id: int, rss_url: str) -> None:
        raw = (rss_url or "").strip()
        rss_norm = self.n.normalise_feed_url(raw)
        self.conn.execute(
            "UPDATE sources SET rss_url=?, rss_url_norm=? WHERE id=?",
            (raw, rss_norm, source_id),
        )
        self.conn.commit()

    def clear_source_rss(self, source_id: int) -> None:
        self.conn.execute(
            "UPDATE sources SET rss_url=NULL, rss_url_norm=NULL WHERE id=?",
            (source_id,),
        )
        self.conn.commit()

    def set_source_channel(self, source_id: int, channel_url: str) -> None:
        base, channel = self.n.extract_channel_ref(channel_url)
        base_norm = self.n.normalise_http_url(base)
        chan_url_norm = self.n.normalise_http_url(channel_url)
        self.conn.execute(
            """
            UPDATE sources
            SET api_base=?, api_base_norm=?, api_channel=?, api_channel_url=?, api_channel_url_norm=?
            WHERE id=?
            """,
            (base, base_norm, channel, channel_url, chan_url_norm, source_id),
        )
        self.conn.commit()

    def clear_source_channel(self, source_id: int) -> None:
        self.conn.execute(
            """
            UPDATE sources
            SET api_base=NULL, api_base_norm=NULL, api_channel=NULL, api_channel_url=NULL, api_channel_url_norm=NULL
            WHERE id=?
            """,
            (source_id,),
        )
        self.conn.commit()

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

    def set_source_lookback(self, source_id: int, lookback_days: Optional[int]) -> None:
        self.conn.execute(
            "UPDATE sources SET lookback_days=? WHERE id=?",
            (lookback_days, source_id),
        )
        self.conn.commit()

    def mark_source_polled(self, source_id: int, error: Optional[str]) -> None:
        ts = self.n.now_ts()
        self.conn.execute(
            "UPDATE sources SET last_polled_ts=?, last_error=? WHERE id=?",
            (ts, (error[:1000] if error else None), source_id),
        )
        self.conn.commit()

    # Videos
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
             published_ts,
             status, first_seen_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
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
                   v.peertube_instance, v.channel_name, v.channel_url, v.account_name, v.account_url
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
        ]
        return dict(zip(keys, row))

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


class PeerTubeClient:
    def __init__(self, n: UrlNormaliser) -> None:
        self.n = n
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "peertube-nostr-publisher/0.1"})

    def _get_json(self, url: str, params: Optional[dict] = None, timeout: int = 15) -> Optional[dict]:
        try:
            r = self.session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def list_channel_videos(self, api_base: str, channel: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """
        Primary listing: API channel videos.
        Endpoint: /api/v1/video-channels/{channel}/videos
        Returns list of video objects (dicts) or None on failure.
        """
        base = self.n.normalise_http_url(api_base)
        url = f"{base}/api/v1/video-channels/{channel}/videos"

        # per PeerTube API: common params include start/count/sort
        # Best-effort across instances:
        params_variants = [
            {"start": 0, "count": min(limit, 100), "sort": "-publishedAt"},
            {"start": 0, "count": min(limit, 100), "sort": "-createdAt"},
            {"start": 0, "count": min(limit, 100)},
        ]
        for params in params_variants:
            data = self._get_json(url, params=params)
            if isinstance(data, dict) and isinstance(data.get("data"), list):
                return data["data"]
        return None

    def parse_rss(self, rss_url: str) -> List[dict]:
        d = feedparser.parse(rss_url)
        entries = d.entries or []
        # oldest first for stable inserts
        return list(reversed(entries))

    def enrich_video(self, watch_url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]:
        ]:
        """
        Given a watch URL, call /api/v1/videos/{id} and extract:
          base, video_id, mp4_url, hls_url, instance, channel_name, channel_url, account_name, account_url, api_title, api_desc
        """
        x = self.n.extract_watch_id(watch_url)
        if not x:
            return None, None, None, None, None, None, None, None, None, None, None

        base, vid = x
        v = self._get_json(f"{base}/api/v1/videos/{vid}")
        if not isinstance(v, dict):
            return base, vid, None, None, None, None, None, None, None, None, None

        hls = self._pick_hls_url(v)
        mp4 = self._pick_best_mp4_url(v)
        instance, channel_name, channel_url, account_name, account_url = self._extract_attribution(base, v)
        api_title = (v.get("name") or "").strip() or None
        api_desc = (v.get("description") or "").strip() or None
        return base, vid, mp4, hls, instance, channel_name, channel_url, account_name, account_url, api_title, api_desc

    @staticmethod
    def _pick_hls_url(v: dict) -> Optional[str]:
        sp = v.get("streamingPlaylists") or []
        for playlist in sp:
            for key in ("playlistUrl", "hlsUrl", "url"):
                val = playlist.get(key)
                if isinstance(val, str) and val.startswith("http") and val.endswith(".m3u8"):
                    return val

            files = playlist.get("files") or []
            for f in files:
                fu = f.get("fileUrl") or f.get("url")
                if isinstance(fu, str) and fu.startswith("http"):
                    if fu.endswith(".m3u8"):
                        return fu
                    if fu.endswith("-fragmented.mp4"):
                        return fu.replace("-fragmented.mp4", ".m3u8")
        return None

    @staticmethod
    def _pick_best_mp4_url(v: dict) -> Optional[str]:
        candidates = []

        def consider_file(f: dict) -> None:
            fu = f.get("fileUrl") or f.get("url")
            if not (isinstance(fu, str) and fu.startswith("http")):
                return
            mt = (f.get("mimeType") or "").lower()
            if "mp4" not in mt and not fu.lower().endswith(".mp4"):
                return
            size = int(f.get("size") or 0)
            res = f.get("resolution") or {}
            height = int(res.get("height") or 0)
            width = int(res.get("width") or 0)
            score = (height * width, size)
            candidates.append((height, score, fu))

        for f in (v.get("files") or []):
            consider_file(f)
        for pl in (v.get("streamingPlaylists") or []):
            for f in (pl.get("files") or []):
                consider_file(f)

        if not candidates:
            return None
        # Prefer a "sensible middle" size: highest <= 720p if available,
        # otherwise the smallest above 720p, else the largest available.
        by_height = []
        for (height, score, url) in candidates:
            by_height.append((height, score[1], url))
        with_height = [c for c in by_height if c[0] > 0]
        if with_height:
            under = [c for c in with_height if c[0] <= 720]
            if under:
                under.sort(reverse=True, key=lambda x: (x[0], x[1]))
                return under[0][2]
            over = sorted(with_height, key=lambda x: (x[0], x[1]))
            return over[0][2]
        candidates.sort(reverse=True, key=lambda x: x[1])
        return candidates[0][2]

    @staticmethod
    def _extract_attribution(base: str, v: dict) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        instance = base
        ch = v.get("channel") or {}
        acc = v.get("account") or {}

        channel_name = ch.get("displayName") or ch.get("name") or ch.get("preferredUsername") or None
        channel_url = ch.get("url") or ch.get("href") or None
        if not channel_url and ch.get("name"):
            channel_url = f"{base}/c/{ch.get('name')}"

        account_name = acc.get("displayName") or acc.get("name") or acc.get("preferredUsername") or None
        account_url = acc.get("url") or acc.get("href") or None
        if not account_url and acc.get("name"):
            account_url = f"{base}/a/{acc.get('name')}"

        return instance, channel_name, channel_url, account_name, account_url


class NostrPublisher:
    @staticmethod
    def _build_content(p: dict) -> str:
        title = (p.get("title") or "").strip()
        summary = (p.get("summary") or "").strip()
        watch = (p.get("watch_url") or "").strip()
        mp4 = p.get("direct_url")
        hls = p.get("hls_url")

        channel_name = p.get("channel_name") or p.get("account_name")
        channel_url = p.get("channel_url") or p.get("account_url")

        lines = []
        if title:
            lines.append(title)
        if channel_name:
            lines.append(f"By: {str(channel_name).strip()}")
        if channel_url:
            lines.append(f"Channel: {str(channel_url).strip()}")

        lines.append("")

        # MP4 first
        if mp4:
            lines.append(str(mp4).strip())
        if hls and hls != mp4:
            lines.append(str(hls).strip())
        if watch:
            lines.append(watch)

        if summary:
            lines.append("")
            lines.append(summary)

        return "\n".join(lines).strip()

    @staticmethod
    def _build_tags(p: dict) -> list[list[str]]:
        tags: list[list[str]] = [["t", "video"], ["t", "peertube"]]

        watch_url = p.get("watch_url")
        channel_url = p.get("channel_url")
        title = (p.get("title") or "").strip()
        author = (p.get("channel_name") or p.get("account_name") or "unknown").strip()

        mp4 = p.get("direct_url")
        hls = p.get("hls_url")

        if mp4:
            tags.append(["url", str(mp4)])
            tags.append(["m", "video/mp4"])
        elif hls:
            tags.append(["url", str(hls)])
            tags.append(["m", "application/x-mpegURL"])

        if watch_url:
            tags.append(["r", str(watch_url)])
        if channel_url:
            tags.append(["r", str(channel_url)])

        if title:
            tags.append(["alt", f"PeerTube video: {title} by {author}"])

        if p.get("peertube_instance"):
            tags.append(["peertube:instance", str(p["peertube_instance"])])
        if p.get("channel_name"):
            tags.append(["peertube:author", str(p["channel_name"])])
        if p.get("channel_url"):
            tags.append(["peertube:channel", str(p["channel_url"])])

        return tags

    @staticmethod
    def publish(nsec: str, relays: list[str], content: str, tags: list[list[str]]) -> str:
        priv = PrivateKey.from_nsec(nsec)
        pub_hex = priv.public_key.hex()
        try:
            ev = Event(kind=1, public_key=pub_hex, content=content, tags=tags)
        except TypeError:
            try:
                ev = Event(kind=1, pubkey=pub_hex, content=content, tags=tags)
            except TypeError:
                ev = Event(content=content, kind=1, tags=tags)
                if hasattr(ev, "pub_key"):
                    setattr(ev, "pub_key", pub_hex)
                elif hasattr(ev, "public_key"):
                    setattr(ev, "public_key", pub_hex)
        if hasattr(priv, "sign_event"):
            priv.sign_event(ev)
        elif hasattr(ev, "sign"):
            try:
                ev.sign(priv)
            except TypeError:
                priv_hex = _privkey_to_hex(priv)
                if not priv_hex:
                    raise
                ev.sign(priv_hex)
        else:
            raise RuntimeError("Unable to sign event with current pynostr version.")

        rm = RelayManager(timeout=6)
        for r in relays:
            rm.add_relay(r)
        rm.publish_event(ev)
        rm.run_sync()
        return ev.id


class Runner:
    def __init__(
        self,
        store: Store,
        pt: PeerTubeClient,
        pub: NostrPublisher,
        n: UrlNormaliser,
        log_fn: Optional[callable] = None,
        status_fn: Optional[callable] = None,
    ) -> None:
        self.store = store
        self.pt = pt
        self.pub = pub
        self.n = n
        self.log_fn = log_fn
        self.status_fn = status_fn

    def _log(self, msg: str) -> None:
        if self.log_fn:
            self.log_fn(msg)
        else:
            print(msg)

    def _status(self, msg: str) -> None:
        if self.status_fn:
            self.status_fn(msg)

    def ingest_sources_once(self, api_limit_per_source: int, new_source_lookback_days: int) -> None:
        sources = self.store.get_enabled_sources()
        for s in sources:
            self._ingest_source(s, api_limit_per_source, new_source_lookback_days)

    def ingest_source_once(self, source_id: int, api_limit_per_source: int, new_source_lookback_days: int) -> None:
        s = self.store.get_source_by_id(source_id)
        if not s:
            self._log(f"Source id {source_id} not found.")
            return
        if s.get("enabled") != 1:
            self._log(f"Source id {source_id} is disabled.")
            return
        self._ingest_source(s, api_limit_per_source, new_source_lookback_days)

    def _ingest_source(self, s: dict, api_limit_per_source: int, new_source_lookback_days: int) -> None:
        sid = s["id"]
        api_base = s.get("api_base")
        api_channel = s.get("api_channel")
        rss_url = s.get("rss_url")
        last_polled_ts = s.get("last_polled_ts")
        lookback_days = s.get("lookback_days")
        cutoff_ts = None
        if not last_polled_ts:
            effective_lookback = lookback_days if lookback_days is not None else new_source_lookback_days
            if effective_lookback and effective_lookback > 0:
                cutoff_ts = self.n.now_ts() - (effective_lookback * 86400)

        inserted = 0
        skipped = 0
        err: Optional[str] = None

        # 1) API primary
        if api_base and api_channel:
            vids = self.pt.list_channel_videos(api_base=api_base, channel=api_channel, limit=api_limit_per_source)
            if vids is not None:
                # PeerTube API is usually newest first; we insert oldest-first for stability
                for v in list(reversed(vids)):
                    # best-effort IDs
                    # "uuid" is common; sometimes "shortUUID"; sometimes "id" (numeric)
                    vid_id = v.get("uuid") or v.get("shortUUID") or v.get("id")
                    watch_url = v.get("url")
                    if not (isinstance(watch_url, str) and watch_url.startswith("http")):
                        # construct watch URL if possible
                        # if only base available and vid_id is a shortUUID/uuid, PeerTube watch path is usually /w/<id>
                        if isinstance(vid_id, (str, int)) and api_base:
                            watch_url = f"{self.n.normalise_http_url(api_base)}/w/{vid_id}"
                        else:
                            continue

                    entry_key = str(vid_id or watch_url)
                    if self.store.video_exists(sid, entry_key):
                        published_ts = self._api_entry_ts(v)
                        if published_ts:
                            self.store.update_published_ts_if_null(sid, entry_key, published_ts)
                        continue
                    if cutoff_ts:
                        published_ts = self._api_entry_ts(v)
                        if published_ts and published_ts < cutoff_ts:
                            skipped += 1
                            continue
                    published_ts = self._api_entry_ts(v)

                    title = (v.get("name") or v.get("title") or "").strip()
                    summary = (v.get("description") or "").strip()

                    base, v_api_id, mp4, hls, instance, ch_name, ch_url, acc_name, acc_url, api_title, api_desc = self.pt.enrich_video(watch_url)
                    if api_title:
                        title = api_title
                    if api_desc:
                        summary = api_desc

                    item = IngestedItem(
                        source_id=sid,
                        entry_key=entry_key,
                        watch_url=watch_url,
                        title=title,
                        summary=summary,
                        peertube_base=base,
                        peertube_video_id=str(v_api_id) if v_api_id else None,
                        hls_url=hls,
                        mp4_url=mp4,
                        peertube_instance=instance,
                        channel_name=ch_name,
                        channel_url=ch_url or s.get("api_channel_url"),
                        account_name=acc_name,
                        account_url=acc_url,
                        published_ts=published_ts,
                    )
                    self.store.insert_pending(item)
                    inserted += 1

                self.store.mark_source_polled(sid, None)
                if inserted:
                    self._log(f"[source {sid}] API new items: {inserted}")
                if skipped:
                    self._log(f"[source {sid}] API skipped old items: {skipped}")
                return  # API succeeded, no need RSS fallback

            # if API configured but failed, capture error and fall through to RSS
            err = "API listing failed; trying RSS fallback"

        # 2) RSS fallback
        if rss_url:
            try:
                entries = self.pt.parse_rss(rss_url)
                for e in entries:
                    # entry key
                    entry_key = self._rss_entry_key(e)
                    if self.store.video_exists(sid, entry_key):
                        published_ts = self._rss_entry_ts(e)
                        if published_ts:
                            self.store.update_published_ts_if_null(sid, entry_key, published_ts)
                        continue
                    if cutoff_ts:
                        published_ts = self._rss_entry_ts(e)
                        if published_ts and published_ts < cutoff_ts:
                            skipped += 1
                            continue
                    published_ts = self._rss_entry_ts(e)

                    title = (e.get("title") or "").strip()
                    watch_url = (e.get("link") or "").strip()
                    summary = (e.get("summary") or "").strip()

                    base, v_api_id, mp4, hls, instance, ch_name, ch_url, acc_name, acc_url, api_title, api_desc = self.pt.enrich_video(watch_url)
                    if api_title:
                        title = api_title
                    if api_desc:
                        summary = api_desc

                    item = IngestedItem(
                        source_id=sid,
                        entry_key=entry_key,
                        watch_url=watch_url,
                        title=title,
                        summary=summary,
                        peertube_base=base,
                        peertube_video_id=str(v_api_id) if v_api_id else None,
                        hls_url=hls,
                        mp4_url=mp4,
                        peertube_instance=instance,
                        channel_name=ch_name,
                        channel_url=ch_url,
                        account_name=acc_name,
                        account_url=acc_url,
                        published_ts=published_ts,
                    )
                    self.store.insert_pending(item)
                    inserted += 1

                self.store.mark_source_polled(sid, None if not err else err)
                if inserted:
                    self._log(f"[source {sid}] RSS new items: {inserted}")
                if skipped:
                    self._log(f"[source {sid}] RSS skipped old items: {skipped}")
            except Exception as ex:
                self.store.mark_source_polled(sid, f"{err + '; ' if err else ''}RSS failed: {ex}")
                self._log(f"[source {sid}] RSS error: {ex}")
        else:
            self.store.mark_source_polled(sid, err or "No RSS fallback configured and API listing failed/unconfigured")

    @staticmethod
    def _rss_entry_key(e: dict) -> str:
        for k in ("id", "guid", "link"):
            v = e.get(k)
            if v:
                return str(v)
        return str(hash(repr(sorted(e.items()))))

    @staticmethod
    def _api_entry_ts(v: dict) -> Optional[int]:
        val = v.get("publishedAt") or v.get("createdAt")
        return _parse_any_timestamp(val)

    @staticmethod
    def _rss_entry_ts(e: dict) -> Optional[int]:
        for k in ("published_parsed", "updated_parsed"):
            val = e.get(k)
            if val:
                try:
                    return int(calendar.timegm(val))
                except Exception:
                    continue
        for k in ("published", "updated"):
            val = e.get(k)
            ts = _parse_any_timestamp(val)
            if ts:
                return ts
        return None

    def publish_one_pending(self, nsec: str, relays: list[str]) -> None:
        pending = self.store.next_pending()
        if not pending:
            return

        content = self.pub._build_content(pending)
        tags = self.pub._build_tags(pending)

        try:
            eid = self.pub.publish(nsec=nsec, relays=relays, content=content, tags=tags)
            self.store.mark_posted(pending["id"], eid)
            for r in relays:
                self.store.mark_relay_used(r, None)
            self._log(f"Published {eid} | {pending.get('title') or pending.get('watch_url')}")
        except Exception as ex:
            self.store.mark_failed(pending["id"], str(ex))
            for r in relays:
                self.store.mark_relay_used(r, str(ex))
            self._log(f"Publish failed: {ex}")

    def run(
        self,
        nsec: Optional[str],
        relays: Optional[list[str]],
        poll_seconds: int,
        publish_interval_seconds: int,
        retry_failed_after_seconds: Optional[int],
        api_limit_per_source: int,
        new_source_lookback_days: int,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        dynamic_nsec = nsec is None
        dynamic_relays = relays is None
        last_relays: Optional[list[str]] = None
        last_nsec_set: Optional[bool] = None

        self._log(f"Poll: {poll_seconds}s | Publish spacing: {publish_interval_seconds}s | API limit/source: {api_limit_per_source}")

        last_retry_check = 0

        while True:
            try:
                if stop_event and stop_event.is_set():
                    self._log("Stopped.")
                    return

                self._status("Fetching feeds")
                if dynamic_relays:
                    relays = self.store.get_enabled_relays() or DEFAULT_RELAYS
                if relays != last_relays:
                    self._log(f"Relays: {', '.join(relays or [])}")
                    last_relays = list(relays or [])

                if dynamic_nsec:
                    nsec = get_stored_nsec(self.store.db_path)
                nsec_set = bool(nsec)
                if last_nsec_set is None or nsec_set != last_nsec_set:
                    if nsec_set:
                        self._log("Nsec available for publishing.")
                    else:
                        self._log("No nsec set; publishing paused.")
                    last_nsec_set = nsec_set

                now = self.n.now_ts()
                if retry_failed_after_seconds is not None:
                    if last_retry_check == 0 or (now - last_retry_check) >= 60:
                        n = self.store.retry_failed(retry_failed_after_seconds)
                        if n:
                            self._log(f"Re-queued failed items for retry: {n}")
                        last_retry_check = now

                self.ingest_sources_once(
                    api_limit_per_source=api_limit_per_source,
                    new_source_lookback_days=new_source_lookback_days,
                )

                # publish at most one per loop iteration
                if nsec:
                    min_interval, max_per_hour = self.store.get_publish_limits()
                    max_per_day_per_source = self.store.get_daily_source_limit()
                    now_ts = self.n.now_ts()
                    last_posted = self.store.last_posted_ts() or 0
                    pending = self.store.next_pending()
                    pending_source = int(pending["source_id"]) if pending else None
                    if last_posted and (now_ts - last_posted) < min_interval:
                        self._status("Rate limited")
                    else:
                        posted_last_hour = self.store.count_posted_since(now_ts - 3600)
                        if posted_last_hour >= max_per_hour:
                            self._status("Rate limited")
                        else:
                            if pending_source is not None:
                                posted_last_day = self.store.count_posted_since_for_source(pending_source, now_ts - 86400)
                                if posted_last_day >= max_per_day_per_source:
                                    self._status("Rate limited")
                                else:
                                    self._status("Publishing")
                                    self.publish_one_pending(nsec=nsec, relays=relays or [])
                                    self._status("Idle")
                            else:
                                self._status("Idle")
                else:
                    self._status("Idle")

                if not _sleep_interruptible(publish_interval_seconds, stop_event):
                    self._log("Stopped.")
                    return
                self._status("Sleeping")
                if not _sleep_interruptible(poll_seconds, stop_event):
                    self._log("Stopped.")
                    return
                self._status("Idle")
            except KeyboardInterrupt:
                self._log("\nStopped.")
                return
            except Exception as ex:
                self._log(f"Loop error: {ex}")
                _sleep_interruptible(poll_seconds, stop_event)


def parse_cli() -> argparse.Namespace:
    argv = sys.argv[1:]
    if "--db" in argv:
        idx = argv.index("--db")
        if idx < len(argv) - 1:
            db_args = argv[idx:idx + 2]
            del argv[idx:idx + 2]
            argv = db_args + argv
    else:
        for i, arg in enumerate(list(argv)):
            if arg.startswith("--db="):
                db_arg = argv.pop(i)
                argv = [db_arg] + argv
                break

    p = argparse.ArgumentParser(description="PeerTube channel videos -> Nostr (API primary, RSS fallback)")
    p.add_argument("--db", default=os.environ.get("DB_PATH", "peertube_to_nostr.db"))

    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Initialise DB schema").set_defaults(cmd="init")

    s = sub.add_parser("add-channel", help="Add a channel source (API primary) using channel URL")
    s.add_argument("channel_url")
    s.set_defaults(cmd="add-channel")

    s = sub.add_parser("add-source", help="Add a source by URL (channel or RSS)")
    s.add_argument("url")
    s.set_defaults(cmd="add-source")

    s = sub.add_parser("add-rss", help="Add an RSS-only source (fallback ingest only)")
    s.add_argument("rss_url")
    s.set_defaults(cmd="add-rss")

    s = sub.add_parser("set-rss", help="Set/replace RSS fallback URL for an existing source id")
    s.add_argument("source_id", type=int)
    s.add_argument("rss_url")
    s.set_defaults(cmd="set-rss")

    s = sub.add_parser("set-channel", help="Set/replace channel URL (API primary) for an existing source id")
    s.add_argument("source_id", type=int)
    s.add_argument("channel_url")
    s.set_defaults(cmd="set-channel")

    s = sub.add_parser("edit-source", help="Edit source URLs (channel and/or RSS)")
    s.add_argument("source_id", type=int)
    s.add_argument("--channel-url", dest="channel_url", default=None)
    s.add_argument("--rss-url", dest="rss_url", default=None)
    s.set_defaults(cmd="edit-source")

    s = sub.add_parser("set-source-lookback", help="Set lookback days for a source (first poll only)")
    s.add_argument("source_id", type=int)
    s.add_argument("lookback_days")
    s.set_defaults(cmd="set-source-lookback")

    s = sub.add_parser("enable-source", help="Enable a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="enable-source")

    s = sub.add_parser("disable-source", help="Disable a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="disable-source")

    s = sub.add_parser("remove-source", help="Remove a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="remove-source")

    sub.add_parser("list-sources", help="List sources").set_defaults(cmd="list-sources")

    s = sub.add_parser("add-relay", help="Add a relay (validated and de-duped)")
    s.add_argument("relay_url")
    s.set_defaults(cmd="add-relay")

    s = sub.add_parser("remove-relay", help="Remove a relay by id or URL")
    s.add_argument("relay_id_or_url")
    s.set_defaults(cmd="remove-relay")

    s = sub.add_parser("edit-relay", help="Edit a relay URL by id or URL")
    s.add_argument("relay_id_or_url")
    s.add_argument("new_relay_url")
    s.set_defaults(cmd="edit-relay")

    s = sub.add_parser("enable-relay", help="Enable a relay by id or URL")
    s.add_argument("relay_id_or_url")
    s.set_defaults(cmd="enable-relay")

    s = sub.add_parser("disable-relay", help="Disable a relay by id or URL")
    s.add_argument("relay_id_or_url")
    s.set_defaults(cmd="disable-relay")

    sub.add_parser("list-relays", help="List relays").set_defaults(cmd="list-relays")

    s = sub.add_parser("run", help="Run polling and publishing loop")
    s.add_argument("--nsec", default=None, help="nsec signing key (or set NOSTR_NSEC)")
    s.add_argument("--relays", default=None, help="Comma-separated relay URLs (overrides DB if provided)")
    s.add_argument("--poll-seconds", type=int, default=int(os.environ.get("POLL_SECONDS", "300")))
    s.add_argument("--publish-interval-seconds", type=int, default=int(os.environ.get("PUBLISH_INTERVAL_SECONDS", "10")))
    s.add_argument("--retry-failed-after-seconds", type=int, default=int(os.environ.get("RETRY_FAILED_AFTER_SECONDS", "3600")))
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.add_argument("--new-source-lookback-days", type=int, default=int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30")))
    s.set_defaults(cmd="run")

    s = sub.add_parser("interactive", help="Run with an interactive CLI to manage sources/relays/nsec")
    s.add_argument("--nsec", default=None, help="nsec signing key (or set NOSTR_NSEC)")
    s.add_argument("--relays", default=None, help="Comma-separated relay URLs (overrides DB if provided)")
    s.add_argument("--poll-seconds", type=int, default=int(os.environ.get("POLL_SECONDS", "300")))
    s.add_argument("--publish-interval-seconds", type=int, default=int(os.environ.get("PUBLISH_INTERVAL_SECONDS", "10")))
    s.add_argument("--retry-failed-after-seconds", type=int, default=int(os.environ.get("RETRY_FAILED_AFTER_SECONDS", "3600")))
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.add_argument("--new-source-lookback-days", type=int, default=int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30")))
    s.set_defaults(cmd="interactive")

    s = sub.add_parser("sync-profile", help="Sync profile metadata + NIP-65 relay list from relays")
    s.add_argument("--nsec", default=None, help="nsec signing key (or set NOSTR_NSEC)")
    s.add_argument("--relays", default=None, help="Comma-separated relay URLs (overrides DB if provided)")
    s.add_argument("--import-relays", action="store_true", help="Import NIP-65 relays into DB")
    s.add_argument("--enable-imported", action="store_true", help="Enable imported relays (default: disabled)")
    s.add_argument("--disable-missing", action="store_true", help="Disable DB relays not present in NIP-65 list")
    s.add_argument("--timeout-seconds", type=int, default=8)
    s.set_defaults(cmd="sync-profile")

    s = sub.add_parser("refresh", help="Ingest sources once (manual refresh)")
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.add_argument("--new-source-lookback-days", type=int, default=int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30")))
    s.set_defaults(cmd="refresh")

    s = sub.add_parser("repair-db", help="Repair/normalise DB fields after updates")
    s.set_defaults(cmd="repair-db")

    s = sub.add_parser("resync-source", help="Clear pending and re-ingest a single source")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="resync-source")

    s = sub.add_parser("retry-failed", help="Requeue failed items")
    s.add_argument("source_id", nargs="?", default=None)
    s.set_defaults(cmd="retry-failed")

    s = sub.add_parser("set-rate", help="Set publish rate limits")
    s.add_argument("--min-interval-seconds", type=int, default=None)
    s.add_argument("--max-posts-per-hour", type=int, default=None)
    s.add_argument("--max-posts-per-day-per-source", type=int, default=None)
    s.set_defaults(cmd="set-rate")

    sub.add_parser("show-rate", help="Show publish rate limits").set_defaults(cmd="show-rate")

    s = sub.add_parser("set-nsec", help="Store nsec securely in OS keyring for this DB path")
    s.add_argument("--nsec", default=None, help="nsec signing key (prompted if omitted)")
    s.set_defaults(cmd="set-nsec")

    sub.add_parser("clear-nsec", help="Remove stored nsec from OS keyring for this DB path").set_defaults(cmd="clear-nsec")

    return p.parse_args(argv)


def main() -> None:
    args = parse_cli()
    n = UrlNormaliser()
    store = Store(args.db, n)
    store.init_schema()

    try:
        if args.cmd == "init":
            store.seed_default_relays_if_empty()
            print(f"Initialised DB: {args.db}")
            return

        if args.cmd == "add-channel":
            sid = store.add_channel_source(args.channel_url)
            print(f"Added channel source id={sid}")
            return

        if args.cmd == "add-source":
            if not _maybe_add_url_as_source(store, n, args.url, print):
                raise SystemExit("URL did not look like a PeerTube channel or RSS feed.")
            return

        if args.cmd == "add-rss":
            rss_norm = n.normalise_feed_url(args.rss_url)
            if not n.looks_like_peertube_feed(rss_norm):
                print("Warning: RSS URL does not look like a typical PeerTube feed (still adding).")
            sid = store.add_rss_source(args.rss_url)
            print(f"Added RSS source id={sid} (canonical: {rss_norm})")
            return

        if args.cmd == "set-rss":
            rss_norm = n.normalise_feed_url(args.rss_url)
            if not n.looks_like_peertube_feed(rss_norm):
                print("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
            store.set_source_rss(args.source_id, args.rss_url)
            print(f"Set RSS fallback for source {args.source_id} (canonical: {rss_norm})")
            return

        if args.cmd == "set-channel":
            store.set_source_channel(args.source_id, args.channel_url)
            print(f"Set channel URL for source {args.source_id}")
            return

        if args.cmd == "enable-source":
            c = store.set_source_enabled(args.source_id, True)
            print(f"Enabled: {c}")
            return

        if args.cmd == "disable-source":
            c = store.set_source_enabled(args.source_id, False)
            print(f"Disabled: {c}")
            return

        if args.cmd == "remove-source":
            c = store.remove_source(args.source_id)
            print(f"Removed: {c}")
            return

        if args.cmd == "list-sources":
            rows = store.list_sources()
            if not rows:
                print("No sources.")
                return
            print("id\tenabled\tapi_base\tapi_channel\trss_url\tlookback\tlast_status\tlast_polled\tlast_error")
            for (sid, enabled, api_base, api_channel, api_channel_url, rss_url, lookback_days, last_polled_ts, last_error) in rows:
                lp = str(last_polled_ts) if last_polled_ts else "-"
                lb = str(lookback_days) if lookback_days is not None else "-"
                if last_polled_ts:
                    status = "ERR" if last_error else "OK"
                else:
                    status = "NEVER"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                api = f"{api_base or ''} {api_channel or ''}".strip()
                print(f"{sid}\t{enabled}\t{api}\t{rss_url or ''}\t{lb}\t{status}\t{lp}\t{le}")
            return

        if args.cmd == "add-relay":
            rid = store.add_relay(args.relay_url)
            print(f"Added relay id={rid}")
            return

        if args.cmd == "remove-relay":
            c = store.remove_relay(args.relay_id_or_url)
            print(f"Removed: {c}")
            return

        if args.cmd == "edit-relay":
            c = store.update_relay_url(args.relay_id_or_url, args.new_relay_url)
            print(f"Updated: {c}")
            return

        if args.cmd == "edit-source":
            _apply_edit_source(store, n, str(args.source_id), args.channel_url, args.rss_url, print)
            return

        if args.cmd == "set-source-lookback":
            val = str(args.lookback_days).strip().lower()
            if val in ("none", "null", "off"):
                store.set_source_lookback(args.source_id, None)
                print(f"Cleared lookback for source {args.source_id}")
                return
            try:
                days = int(val)
            except ValueError:
                raise SystemExit("lookback_days must be an integer or 'none'")
            store.set_source_lookback(args.source_id, days)
            print(f"Set lookback_days={days} for source {args.source_id}")
            return

        if args.cmd == "enable-relay":
            c = store.set_relay_enabled(args.relay_id_or_url, True)
            print(f"Enabled: {c}")
            return

        if args.cmd == "disable-relay":
            c = store.set_relay_enabled(args.relay_id_or_url, False)
            print(f"Disabled: {c}")
            return

        if args.cmd == "list-relays":
            rows = store.list_relays()
            if not rows:
                print("No relays.")
                return
            print("id\tenabled\trelay_url\tcanonical\tlast_used\tlast_error")
            for (rid, enabled, url, url_norm, last_used_ts, last_error) in rows:
                lu = str(last_used_ts) if last_used_ts else "-"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                print(f"{rid}\t{enabled}\t{url}\t{url_norm}\t{lu}\t{le}")
            return

        if args.cmd in ("run", "interactive"):
            store.seed_default_relays_if_empty()

            nsec_env = os.environ.get("NOSTR_NSEC") or args.nsec
            nsec = nsec_env or get_stored_nsec(args.db)
            if args.cmd == "run" and not nsec:
                raise SystemExit("Provide nsec via --nsec or NOSTR_NSEC, or run set-nsec to store it.")

            relays_env = os.environ.get("NOSTR_RELAYS")
            relays_cli = args.relays

            if relays_env and relays_env.strip():
                relays = [n.normalise_relay_url(x.strip()) for x in relays_env.split(",") if x.strip()]
            elif relays_cli and relays_cli.strip():
                relays = [n.normalise_relay_url(x.strip()) for x in relays_cli.split(",") if x.strip()]
            else:
                relays = None

            retry = args.retry_failed_after_seconds
            if retry == 0:
                retry = None

            if args.cmd == "interactive":
                _run_interactive(
                    args=args,
                    n=n,
                    nsec_env=nsec_env,
                    relays=relays,
                    retry=retry,
                )
            else:
                runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n, status_fn=_set_runtime_status)
                runner.run(
                    nsec=nsec_env,
                    relays=relays,
                    poll_seconds=args.poll_seconds,
                    publish_interval_seconds=args.publish_interval_seconds,
                    retry_failed_after_seconds=retry,
                    api_limit_per_source=args.api_limit_per_source,
                    new_source_lookback_days=args.new_source_lookback_days,
                )
            return

        if args.cmd == "sync-profile":
            sync_profile(
                store=store,
                n=n,
                nsec_arg=args.nsec,
                relays_arg=args.relays,
                import_relays=args.import_relays,
                enable_imported=args.enable_imported,
                disable_missing=args.disable_missing,
                timeout_seconds=args.timeout_seconds,
            )
            return

        if args.cmd == "refresh":
            runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n)
            runner.ingest_sources_once(
                api_limit_per_source=args.api_limit_per_source,
                new_source_lookback_days=args.new_source_lookback_days,
            )
            return

        if args.cmd == "repair-db":
            repair_db(store, n, print)
            return

        if args.cmd == "resync-source":
            _resync_source(store, n, args.source_id, print)
            return

        if args.cmd == "retry-failed":
            if args.source_id:
                try:
                    sid = int(args.source_id)
                except ValueError:
                    raise SystemExit("source_id must be an integer")
                count = store.retry_failed_for_source(sid, older_than_seconds=0)
                print(f"Re-queued failed items for source {sid}: {count}")
            else:
                count = store.retry_failed(older_than_seconds=0)
                print(f"Re-queued failed items: {count}")
            return

        if args.cmd == "set-rate":
            if args.min_interval_seconds is not None:
                store.set_setting("min_publish_interval_seconds", str(int(args.min_interval_seconds)))
            if args.max_posts_per_hour is not None:
                store.set_setting("max_posts_per_hour", str(int(args.max_posts_per_hour)))
            if args.max_posts_per_day_per_source is not None:
                store.set_setting("max_posts_per_day_per_source", str(int(args.max_posts_per_day_per_source)))
            min_interval, max_per_hour = store.get_publish_limits()
            max_per_day_per_source = store.get_daily_source_limit()
            print(
                "Rate limits: "
                f"min_interval_seconds={min_interval}, "
                f"max_posts_per_hour={max_per_hour}, "
                f"max_posts_per_day_per_source={max_per_day_per_source}"
            )
            return

        if args.cmd == "show-rate":
            min_interval, max_per_hour = store.get_publish_limits()
            max_per_day_per_source = store.get_daily_source_limit()
            print(
                "Rate limits: "
                f"min_interval_seconds={min_interval}, "
                f"max_posts_per_hour={max_per_hour}, "
                f"max_posts_per_day_per_source={max_per_day_per_source}"
            )
            return

        if args.cmd == "set-nsec":
            nsec = args.nsec
            if not nsec:
                nsec = getpass.getpass("Enter nsec: ").strip()
            if not nsec:
                raise SystemExit("nsec cannot be empty.")
            store_type, path = set_stored_nsec(args.db, nsec)
            if store_type == "keyring":
                print("Stored nsec in OS keyring for this DB path.")
            else:
                print(f"Stored nsec in file: {path}")
            return

        if args.cmd == "clear-nsec":
            removed = clear_stored_nsec(args.db)
            print("Removed stored nsec." if removed else "No stored nsec found.")
            return

    finally:
        store.close()


def _interactive_shell(db_path: str, n: UrlNormaliser, stop_event: threading.Event) -> None:
    store = Store(db_path, n)
    store.init_schema()
    _interactive_first_run(store, db_path, n)
    commands = _interactive_commands()

    def _relay_tokens() -> list[str]:
        rows = store.list_relays()
        out: list[str] = []
        for (rid, _enabled, url, _url_norm, _last_used_ts, _last_error) in rows:
            out.append(str(rid))
            if url:
                out.append(str(url))
        return out

    def _source_ids() -> list[str]:
        rows = store.list_sources()
        return [str(r[0]) for r in rows]

    class _InteractiveCompleter(Completer):
        def get_completions(self, document, complete_event):
            text = document.text_before_cursor
            try:
                parts = shlex.split(text)
            except ValueError:
                parts = text.split()
            if text.endswith(" "):
                parts.append("")
            if len(parts) <= 1:
                word = parts[0] if parts else ""
                for c in commands:
                    if c.startswith(word):
                        yield Completion(c, start_position=-len(word))
                return

            cmd = _normalize_cmd(parts[0])
            current = parts[-1]

            if cmd in ("enable-relay", "disable-relay", "remove-relay", "edit-relay"):
                if len(parts) <= 2:
                    for val in _relay_tokens():
                        if val.startswith(current):
                            yield Completion(val, start_position=-len(current))
                return

            if cmd in ("enable-source", "disable-source", "set-rss", "set-channel", "edit-source", "set-source-lookback", "remove-source"):
                if len(parts) <= 2:
                    for val in _source_ids():
                        if val.startswith(current):
                            yield Completion(val, start_position=-len(current))
                return

    def _history_path() -> str:
        base = os.path.dirname(os.path.abspath(db_path)) or "."
        return os.path.join(base, ".peertube2nostr_history")

    def _log(msg: str) -> None:
        print(msg)

    print("== PeerTube2Nostr Interactive ==")
    print("Type '/' for commands. 'quit' to exit.")
    _log(_interactive_dashboard(store, db_path))

    try:
        session = None
        if PromptSession is not None:
            try:
                style = Style.from_dict(
                    {"prompt": "ansicyan bold", "toolbar": "ansiblack bg:ansiwhite"}
                )
                session = PromptSession(
                    message=[("class:prompt", "> ")],
                    history=FileHistory(_history_path()),
                    auto_suggest=AutoSuggestFromHistory(),
                    completer=_InteractiveCompleter(),
                    style=style,
                    bottom_toolbar=lambda: _status_toolbar(store, db_path),
                )
            except Exception:
                session = None

        arg_prompts = _interactive_arg_prompts()
        while True:
            try:
                if session is not None:
                    line = session.prompt().strip()
                else:
                    line = input("> ").strip()
            except EOFError:
                line = "quit"
            if not line:
                continue
            parts = shlex.split(line)
            cmd = _normalize_cmd(parts[0])
            args = parts[1:]

            if cmd not in commands and not args and line.startswith(("http://", "https://")):
                if _maybe_add_url_as_source(store, n, line.strip(), _log):
                    continue

            if cmd == "edit-source" and len(args) < 3:
                try:
                    source_id = args[0] if args else input("Source id: ").strip()
                except EOFError:
                    source_id = ""
                if not source_id:
                    _log("Canceled.")
                    continue
                try:
                    choice = (input("Change what? (channel/rss/both): ").strip().lower() or "both")
                except EOFError:
                    choice = ""
                if choice not in ("channel", "rss", "both"):
                    _log("Choose: channel, rss, or both.")
                    continue
                channel_url = ""
                rss_url = ""
                if choice in ("channel", "both"):
                    try:
                        channel_url = input("Channel URL (blank to skip, 'none' to clear): ").strip()
                    except EOFError:
                        channel_url = ""
                if choice in ("rss", "both"):
                    try:
                        rss_url = input("RSS URL (blank to skip, 'none' to clear): ").strip()
                    except EOFError:
                        rss_url = ""
                _apply_edit_source(store, n, source_id, channel_url or None, rss_url or None, _log)
                continue

            if cmd == "set-rate" and len(args) < 3:
                try:
                    min_int = args[0] if len(args) > 0 else input("Min interval seconds: ").strip()
                except EOFError:
                    min_int = ""
                try:
                    max_per = args[1] if len(args) > 1 else input("Max posts per hour: ").strip()
                except EOFError:
                    max_per = ""
                try:
                    max_day = args[2] if len(args) > 2 else input("Max posts per day per source: ").strip()
                except EOFError:
                    max_day = ""
                parsed = _parse_set_rate_args(
                    [f"--min-interval-seconds={min_int}"] if min_int else []
                    + ([f"--max-posts-per-hour={max_per}"] if max_per else [])
                    + ([f"--max-posts-per-day-per-source={max_day}"] if max_day else [])
                )
                if isinstance(parsed, str):
                    _log(parsed)
                else:
                    if parsed.min_interval_seconds is not None:
                        store.set_setting("min_publish_interval_seconds", str(int(parsed.min_interval_seconds)))
                    if parsed.max_posts_per_hour is not None:
                        store.set_setting("max_posts_per_hour", str(int(parsed.max_posts_per_hour)))
                    if parsed.max_posts_per_day_per_source is not None:
                        store.set_setting(
                            "max_posts_per_day_per_source",
                            str(int(parsed.max_posts_per_day_per_source)),
                        )
                    min_interval, max_per_hour = store.get_publish_limits()
                    max_per_day_per_source = store.get_daily_source_limit()
                    _log(
                        "Rate limits: "
                        f"min_interval_seconds={min_interval}, "
                        f"max_posts_per_hour={max_per_hour}, "
                        f"max_posts_per_day_per_source={max_per_day_per_source}"
                    )
                continue

            if cmd == "resync-source" and len(args) < 1:
                try:
                    source_id = input("Source id: ").strip()
                except EOFError:
                    source_id = ""
                if not source_id:
                    _log("Canceled.")
                    continue
                _resync_source(store, n, int(source_id), _log)
                continue

            if cmd == "retry-failed" and len(args) < 1:
                try:
                    source_id = input("Source id (blank for all): ").strip()
                except EOFError:
                    source_id = ""
                if not source_id:
                    count = store.retry_failed(older_than_seconds=0)
                    _log(f"Re-queued failed items: {count}")
                else:
                    count = store.retry_failed_for_source(int(source_id), older_than_seconds=0)
                    _log(f"Re-queued failed items for source {source_id}: {count}")
                continue

            if cmd == "repair-db":
                repair_db(store, n, _log)
                continue

            if cmd in arg_prompts and len(args) < len(arg_prompts[cmd]):
                prompts = arg_prompts[cmd][len(args):]
                for prompt in prompts:
                    try:
                        val = input(f"{prompt}: ").strip()
                    except EOFError:
                        val = ""
                    if not val:
                        _log("Canceled.")
                        break
                    args.append(val)
                else:
                    pass
                if len(args) < len(arg_prompts[cmd]):
                    continue

            should_quit = _dispatch_command(store, n, db_path, cmd, args, _log)
            if should_quit:
                stop_event.set()
                return
    finally:
        store.close()


def _interactive_first_run(store: Store, db_path: str, n: UrlNormaliser) -> None:
    has_sources = store.count_sources() > 0
    has_nsec = bool(get_stored_nsec(db_path))
    if has_sources and has_nsec:
        return

    print("First run setup (press Enter to skip any step).")

    if store.count_relays() == 0:
        ans = input("Seed default relays? [Y/n]: ").strip().lower()
        if ans in ("", "y", "yes"):
            store.seed_default_relays_if_empty()
            print("Seeded default relays.")

    if not has_nsec:
        ans = input("Set nsec now? [Y/n]: ").strip().lower()
        if ans in ("", "y", "yes"):
            nsec = getpass.getpass("Enter nsec: ").strip()
            if nsec:
                store_type, path = set_stored_nsec(db_path, nsec)
                if store_type == "keyring":
                    print("Stored nsec in OS keyring for this DB path.")
                else:
                    print(f"Stored nsec in file: {path}")

    if not has_sources:
        channel_url = input("Add PeerTube channel URL (blank to skip): ").strip()
        if channel_url:
            try:
                sid = store.add_channel_source(channel_url)
                print(f"Added channel source id={sid}")
            except Exception as ex:
                print(f"Failed to add channel: {ex}")

            rss_url = input("Add RSS fallback URL (blank to skip): ").strip()
            if rss_url:
                try:
                    rss_norm = n.normalise_feed_url(rss_url)
                    if not n.looks_like_peertube_feed(rss_norm):
                        print("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
                    store.set_source_rss(sid, rss_url)
                    print(f"Set RSS fallback for source {sid} (canonical: {rss_norm})")
                except Exception as ex:
                    print(f"Failed to set RSS: {ex}")


def _interactive_commands() -> list[str]:
    return [
        "help", "status", "init", "refresh", "repair-db", "resync-source", "retry-failed", "sync-profile", "set-rate", "show-rate",
        "list-relays", "add-relay", "remove-relay", "edit-relay", "enable-relay", "disable-relay",
        "list-sources", "add-channel", "add-source", "add-rss", "set-rss", "set-channel", "edit-source", "set-source-lookback", "enable-source", "disable-source", "remove-source",
        "set-nsec", "clear-nsec",
        "quit", "exit",
    ]


def _interactive_arg_prompts() -> dict[str, list[str]]:
    return {
        "add-relay": ["Relay URL"],
        "remove-relay": ["Relay id or URL"],
        "edit-relay": ["Relay id or URL", "New relay URL"],
        "enable-relay": ["Relay id or URL"],
        "disable-relay": ["Relay id or URL"],
        "add-channel": ["Channel URL"],
        "add-source": ["Channel or RSS URL"],
        "add-rss": ["RSS URL"],
        "set-rss": ["Source id", "RSS URL"],
        "set-channel": ["Source id", "Channel URL"],
        "edit-source": ["Source id"],
        "set-source-lookback": ["Source id", "Lookback days (or 'none')"],
        "set-rate": ["Min interval seconds", "Max posts per hour", "Max posts per day per source"],
        "enable-source": ["Source id"],
        "disable-source": ["Source id"],
        "remove-source": ["Source id"],
        "resync-source": ["Source id"],
        "retry-failed": ["Source id (blank for all)"],
    }


def _emit_help(log_fn) -> None:
    for line in _help_lines():
        log_fn(line)


def _help_lines() -> list[str]:
    return [
        "Commands:",
        "  help | / | ?                     Show this help",
        "  status                            Show counts + nsec status",
        "  init                              Init DB + seed relays (if empty)",
        "  refresh                           Ingest sources once (manual)",
        "  repair-db                         Repair/normalise DB fields",
        "  resync-source <id>                Clear pending + re-ingest one source",
        "  retry-failed [id]                 Requeue failed items (all or by source)",
        "  sync-profile [--relays ...]       Fetch kind 0 + 10002 for your pubkey",
        "  show-rate                         Show publish rate limits",
        "  set-rate [--min-interval-seconds N] [--max-posts-per-hour N]  Set limits",
        "  list-relays                       List relays",
        "  add-relay <url>                   Add relay",
        "  remove-relay <id|url>             Remove relay",
        "  edit-relay <id|url> <new_url>     Edit relay URL",
        "  enable-relay <id|url>             Enable relay",
        "  disable-relay <id|url>            Disable relay",
        "  list-sources                      List sources",
        "  add-channel <url>                 Add PeerTube channel",
        "  add-source <url>                  Add source (channel or RSS)",
        "  add-rss <url>                     Add RSS-only source",
        "  set-rss <id> <url>                Set RSS fallback",
        "  set-channel <id> <url>            Set channel URL (API primary)",
        "  edit-source <id> [--channel-url X] [--rss-url Y]  Edit source URLs",
        "  set-source-lookback <id> <days|none>  Set per-source lookback days",
        "  enable-source <id>                Enable source",
        "  disable-source <id>               Disable source",
        "  remove-source <id>                Remove source",
        "  set-nsec [nsec]                   Store nsec (prompt if omitted)",
        "  clear-nsec                        Remove stored nsec",
        "  quit | exit                       Stop",
    ]


def _dispatch_command(store: Store, n: UrlNormaliser, db_path: str, cmd: str, args: list[str], log_fn) -> bool:
    cmd = _normalize_cmd(cmd)
    if cmd in ("/", "?", "help"):
        _emit_help(log_fn)
        return False
    if cmd in ("quit", "exit"):
        return True
    if cmd == "status":
        relays = store.get_enabled_relays()
        pending = store.count_pending()
        sources = store.count_sources()
        has_nsec = bool(get_stored_nsec(db_path))
        log_fn(f"Relays enabled: {len(relays)} | Sources: {sources} | Pending: {pending} | Nsec set: {has_nsec}")
        return False
    if cmd == "init":
        store.init_schema()
        store.seed_default_relays_if_empty()
        log_fn(f"Initialised DB: {db_path}")
        return False
    if cmd == "sync-profile":
        parsed = _parse_sync_profile_args(args)
        if isinstance(parsed, str):
            log_fn(parsed)
            return False
        try:
            sync_profile(
                store=store,
                n=n,
                nsec_arg=parsed.nsec,
                relays_arg=parsed.relays,
                import_relays=parsed.import_relays,
                enable_imported=parsed.enable_imported,
                disable_missing=parsed.disable_missing,
                timeout_seconds=parsed.timeout_seconds,
                log_fn=log_fn,
            )
        except SystemExit as ex:
            msg = str(ex) or "sync-profile failed."
            log_fn(msg)
        except Exception as ex:
            log_fn(f"sync-profile error: {ex}")
        return False
    if cmd == "refresh":
        api_limit = int(os.environ.get("API_LIMIT_PER_SOURCE", "50"))
        lookback_days = int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30"))
        runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n, log_fn=log_fn)
        runner.ingest_sources_once(api_limit, lookback_days)
        return False
    if cmd == "repair-db":
        repair_db(store, n, log_fn)
        return False
    if cmd == "resync-source" and len(args) == 1:
        try:
            sid = int(args[0])
        except ValueError:
            log_fn("source_id must be an integer")
            return False
        _resync_source(store, n, sid, log_fn)
        return False
    if cmd == "retry-failed":
        if args:
            try:
                sid = int(args[0])
            except ValueError:
                log_fn("source_id must be an integer")
                return False
            count = store.retry_failed_for_source(sid, older_than_seconds=0)
            log_fn(f"Re-queued failed items for source {sid}: {count}")
        else:
            count = store.retry_failed(older_than_seconds=0)
            log_fn(f"Re-queued failed items: {count}")
        return False
    if cmd == "show-rate":
        min_interval, max_per_hour = store.get_publish_limits()
        log_fn(f"Rate limits: min_interval_seconds={min_interval}, max_posts_per_hour={max_per_hour}")
        return False
    if cmd == "set-rate":
        parsed = _parse_set_rate_args(args)
        if isinstance(parsed, str):
            log_fn(parsed)
            return False
        if parsed.min_interval_seconds is not None:
            store.set_setting("min_publish_interval_seconds", str(int(parsed.min_interval_seconds)))
        if parsed.max_posts_per_hour is not None:
            store.set_setting("max_posts_per_hour", str(int(parsed.max_posts_per_hour)))
        if parsed.max_posts_per_day_per_source is not None:
            store.set_setting("max_posts_per_day_per_source", str(int(parsed.max_posts_per_day_per_source)))
        min_interval, max_per_hour = store.get_publish_limits()
        max_per_day_per_source = store.get_daily_source_limit()
        log_fn(
            "Rate limits: "
            f"min_interval_seconds={min_interval}, "
            f"max_posts_per_hour={max_per_hour}, "
            f"max_posts_per_day_per_source={max_per_day_per_source}"
        )
        return False
    if cmd == "edit-source":
        parsed = _parse_edit_source_args(args)
        if isinstance(parsed, str):
            log_fn(parsed)
            return False
        _apply_edit_source(store, n, parsed.source_id, parsed.channel_url, parsed.rss_url, log_fn)
        return False
    if cmd == "list-relays":
        rows = store.list_relays()
        if not rows:
            log_fn("No relays.")
        else:
            log_fn("id\tenabled\trelay_url\tcanonical\tlast_used\tlast_error")
            for (rid, enabled, url, url_norm, last_used_ts, last_error) in rows:
                lu = str(last_used_ts) if last_used_ts else "-"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                log_fn(f"{rid}\t{enabled}\t{url}\t{url_norm}\t{lu}\t{le}")
        return False
    if cmd == "add-relay" and len(args) == 1:
        rid = store.add_relay(args[0])
        log_fn(f"Added relay id={rid}")
        return False
    if cmd == "add-source" and len(args) == 1:
        if not _maybe_add_url_as_source(store, n, args[0], log_fn):
            log_fn("URL did not look like a PeerTube channel or RSS feed.")
        return False
    if cmd == "remove-relay" and len(args) == 1:
        c = store.remove_relay(args[0])
        log_fn(f"Removed: {c}")
        return False
    if cmd == "edit-relay" and len(args) == 2:
        c = store.update_relay_url(args[0], args[1])
        log_fn(f"Updated: {c}")
        return False
    if cmd == "enable-relay" and len(args) == 1:
        c = store.set_relay_enabled(args[0], True)
        log_fn(f"Enabled: {c}")
        return False
    if cmd == "disable-relay" and len(args) == 1:
        c = store.set_relay_enabled(args[0], False)
        log_fn(f"Disabled: {c}")
        return False
    if cmd == "list-sources":
        rows = store.list_sources()
        if not rows:
            log_fn("No sources.")
        else:
            log_fn("id\tenabled\tapi_base\tapi_channel\trss_url\tlookback\tlast_status\tlast_polled\tlast_error")
            for (sid, enabled, api_base, api_channel, api_channel_url, rss_url, lookback_days, last_polled_ts, last_error) in rows:
                lp = str(last_polled_ts) if last_polled_ts else "-"
                lb = str(lookback_days) if lookback_days is not None else "-"
                if last_polled_ts:
                    status = "ERR" if last_error else "OK"
                else:
                    status = "NEVER"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                api = f"{api_base or ''} {api_channel or ''}".strip()
                log_fn(f"{sid}\t{enabled}\t{api}\t{rss_url or ''}\t{lb}\t{status}\t{lp}\t{le}")
        return False
    if cmd == "add-channel" and len(args) == 1:
        sid = store.add_channel_source(args[0])
        log_fn(f"Added channel source id={sid}")
        return False
    if cmd == "add-rss" and len(args) == 1:
        rss_norm = n.normalise_feed_url(args[0])
        if not n.looks_like_peertube_feed(rss_norm):
            log_fn("Warning: RSS URL does not look like a typical PeerTube feed (still adding).")
        sid = store.add_rss_source(args[0])
        log_fn(f"Added RSS source id={sid} (canonical: {rss_norm})")
        return False
    if cmd == "set-rss" and len(args) == 2:
        rss_norm = n.normalise_feed_url(args[1])
        if not n.looks_like_peertube_feed(rss_norm):
            log_fn("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
        store.set_source_rss(int(args[0]), args[1])
        log_fn(f"Set RSS fallback for source {args[0]} (canonical: {rss_norm})")
        return False
    if cmd == "set-channel" and len(args) == 2:
        store.set_source_channel(int(args[0]), args[1])
        log_fn(f"Set channel URL for source {args[0]}")
        return False
    if cmd == "set-source-lookback" and len(args) == 2:
        val = str(args[1]).strip().lower()
        if val in ("none", "null", "off"):
            store.set_source_lookback(int(args[0]), None)
            log_fn(f"Cleared lookback for source {args[0]}")
            return False
        try:
            days = int(val)
        except ValueError:
            log_fn("lookback_days must be an integer or 'none'")
            return False
        store.set_source_lookback(int(args[0]), days)
        log_fn(f"Set lookback_days={days} for source {args[0]}")
        return False
    if not args and cmd.startswith(("http://", "https://")):
        if _maybe_add_url_as_source(store, n, cmd, log_fn):
            return False
    if cmd == "enable-source" and len(args) == 1:
        c = store.set_source_enabled(int(args[0]), True)
        log_fn(f"Enabled: {c}")
        return False
    if cmd == "disable-source" and len(args) == 1:
        c = store.set_source_enabled(int(args[0]), False)
        log_fn(f"Disabled: {c}")
        return False
    if cmd == "remove-source" and len(args) == 1:
        c = store.remove_source(int(args[0]))
        log_fn(f"Removed: {c}")
        return False
    if cmd == "set-nsec":
        nsec = args[0] if args else getpass.getpass("Enter nsec: ").strip()
        if not nsec:
            log_fn("nsec cannot be empty.")
            return False
        store_type, path = set_stored_nsec(db_path, nsec)
        if store_type == "keyring":
            log_fn("Stored nsec in OS keyring for this DB path.")
        else:
            log_fn(f"Stored nsec in file: {path}")
        return False
    if cmd == "clear-nsec":
        removed = clear_stored_nsec(db_path)
        log_fn("Removed stored nsec." if removed else "No stored nsec found.")
        return False

    log_fn("Unknown or invalid command. Type '/' for usage.")
    return False


def _status_toolbar(store: Store, db_path: str) -> str:
    relays = store.count_relays()
    sources = store.count_sources()
    pending = store.count_pending()
    posted = store.count_posted()
    failed = store.count_failed()
    has_nsec = bool(get_stored_nsec(db_path))
    nsec_txt = "nsec:yes" if has_nsec else "nsec:no"
    status = _get_runtime_status()
    status_txt = f" status:{status}" if status else ""
    return f" relays:{relays} sources:{sources} pending:{pending} posted:{posted} failed:{failed} {nsec_txt}{status_txt} "


def _interactive_dashboard(store: Store, db_path: str) -> str:
    relays = store.count_relays()
    sources = store.count_sources()
    pending = store.count_pending()
    posted = store.count_posted()
    failed = store.count_failed()
    last_poll = store.last_polled_ts()
    last_posted = store.last_posted_ts()
    min_interval, max_per_hour = store.get_publish_limits()
    has_nsec = "yes" if get_stored_nsec(db_path) else "no"
    now = int(time.time())
    poll_age = f"{now - last_poll}s ago" if last_poll else "never"
    post_age = f"{now - last_posted}s ago" if last_posted else "never"
    status = _get_runtime_status() or "idle"
    lines = [
        "Dashboard:",
        f"  Relays: {relays}",
        f"  Sources: {sources}",
        f"  Pending: {pending}",
        f"  Posted: {posted}",
        f"  Failed: {failed}",
        f"  Last poll: {poll_age}",
        f"  Last post: {post_age}",
        f"  Rate: min_interval={min_interval}s, max_per_hour={max_per_hour}",
        f"  Nsec set: {has_nsec}",
        f"  Status: {status}",
        "  Hint: type '/' to open the command palette",
    ]
    return "\n".join(lines)


_RUNTIME_STATUS = ""


def _set_runtime_status(value: str) -> None:
    global _RUNTIME_STATUS
    _RUNTIME_STATUS = value


def _get_runtime_status() -> str:
    return _RUNTIME_STATUS


def _format_dashboard_panels(store: Store, db_path: str) -> dict[str, str]:
    relays = store.count_relays()
    sources = store.count_sources()
    pending = store.count_pending()
    posted = store.count_posted()
    failed = store.count_failed()
    last_poll = store.last_polled_ts()
    last_posted = store.last_posted_ts()
    min_interval, max_per_hour = store.get_publish_limits()
    max_per_day_per_source = store.get_daily_source_limit()
    has_nsec = "yes" if get_stored_nsec(db_path) else "no"
    now = int(time.time())
    poll_age = f"{now - last_poll}s ago" if last_poll else "never"
    post_age = f"{now - last_posted}s ago" if last_posted else "never"
    status = _get_runtime_status() or "idle"
    next_post = _estimate_next_post(store, db_path)

    counts = "\n".join(
        [
            "Counts",
            f"Relays:   {relays}",
            f"Sources:  {sources}",
            f"Pending:  {pending}",
            f"Posted:   {posted}",
            f"Failed:   {failed}",
        ]
    )
    activity = "\n".join(
        [
            "Activity",
            f"Last poll: {poll_age}",
            f"Last post: {post_age}",
            f"Status:    {status}",
            f"Next post: {next_post}",
            f"Nsec set:  {has_nsec}",
        ]
    )
    rate = "\n".join(
        [
            "Rate Limits",
            f"Min interval: {min_interval}s",
            f"Max/hour:     {max_per_hour}",
            f"Max/day/src:  {max_per_day_per_source}",
        ]
    )
    pending_lines: list[str] = []
    rows = store.list_pending(limit=200)
    if not rows:
        pending_lines.append("(none)")
    else:
        for (vid, sid, title, watch_url, _first_seen_ts, published_ts, api_base, api_channel, _rss_url) in rows:
            label = title or watch_url or f"video {vid}"
            source_label = f"{api_base or ''} {api_channel or ''}".strip() or f"source {sid}"
            if len(label) > 70:
                label = label[:67] + "..."
            if published_ts:
                age = int(time.time()) - int(published_ts)
                age_txt = f"{age//3600}h" if age >= 3600 else f"{age//60}m"
            else:
                age_txt = "?"
            pending_lines.append(f"{label} ({age_txt}) [{source_label}]")
    return {"counts": counts, "activity": activity, "rate": rate, "queue": pending_lines}


def _estimate_next_post(store: Store, db_path: str) -> str:
    if store.count_pending() == 0:
        return "none"
    if not get_stored_nsec(db_path):
        return "nsec missing"
    min_interval, max_per_hour = store.get_publish_limits()
    max_per_day_per_source = store.get_daily_source_limit()
    now_ts = int(time.time())
    last_posted = store.last_posted_ts() or 0
    wait_interval = max(0, min_interval - (now_ts - last_posted)) if last_posted else 0
    posted_last_hour = store.count_posted_since(now_ts - 3600)
    wait_rate = 0
    if posted_last_hour >= max_per_hour:
        oldest = store.oldest_posted_since(now_ts - 3600)
        if oldest:
            wait_rate = max(0, 3600 - (now_ts - oldest))
    pending = store.next_pending()
    wait_day = 0
    if pending:
        sid = int(pending["source_id"])
        posted_last_day = store.count_posted_since_for_source(sid, now_ts - 86400)
        if posted_last_day >= max_per_day_per_source:
            oldest_day = store.oldest_posted_since_for_source(sid, now_ts - 86400)
            if oldest_day:
                wait_day = max(0, 86400 - (now_ts - oldest_day))
    wait = max(wait_interval, wait_rate)
    wait = max(wait, wait_day)
    if wait == 0:
        return "now"
    return f"in {wait}s"


def _sleep_interruptible(seconds: int, stop_event: Optional[threading.Event]) -> bool:
    if seconds <= 0:
        return True
    if stop_event is None:
        time.sleep(seconds)
        return True
    end = time.time() + seconds
    while time.time() < end:
        if stop_event.is_set():
            return False
        time.sleep(0.2)
    return True


def _parse_any_timestamp(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            try:
                dt = parsedate_to_datetime(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except Exception:
                return None
    return None


def _parse_sync_profile_args(args: list[str]) -> argparse.Namespace | str:
    p = argparse.ArgumentParser(prog="sync-profile", add_help=False)
    p.add_argument("--nsec", default=None)
    p.add_argument("--relays", default=None)
    p.add_argument("--import-relays", action="store_true")
    p.add_argument("--enable-imported", action="store_true")
    p.add_argument("--disable-missing", action="store_true")
    p.add_argument("--timeout-seconds", type=int, default=8)
    try:
        return p.parse_args(args)
    except SystemExit:
        return "Usage: sync-profile [--relays a,b] [--nsec nsec] [--import-relays] [--enable-imported] [--disable-missing] [--timeout-seconds N]"


def _parse_edit_source_args(args: list[str]) -> argparse.Namespace | str:
    p = argparse.ArgumentParser(prog="edit-source", add_help=False)
    p.add_argument("source_id")
    p.add_argument("--channel-url", dest="channel_url", default=None)
    p.add_argument("--rss-url", dest="rss_url", default=None)
    try:
        ns = p.parse_args(args)
    except SystemExit:
        return "Usage: edit-source <id> [--channel-url URL] [--rss-url URL]"
    if not ns.channel_url and not ns.rss_url:
        return "Provide --channel-url and/or --rss-url."
    return ns


def _parse_set_rate_args(args: list[str]) -> argparse.Namespace | str:
    p = argparse.ArgumentParser(prog="set-rate", add_help=False)
    p.add_argument("--min-interval-seconds", type=int, default=None)
    p.add_argument("--max-posts-per-hour", type=int, default=None)
    p.add_argument("--max-posts-per-day-per-source", type=int, default=None)
    try:
        ns = p.parse_args(args)
    except SystemExit:
        return "Usage: set-rate [--min-interval-seconds N] [--max-posts-per-hour N] [--max-posts-per-day-per-source N]"
    if ns.min_interval_seconds is None and ns.max_posts_per_hour is None and ns.max_posts_per_day_per_source is None:
        return "Provide --min-interval-seconds and/or --max-posts-per-hour and/or --max-posts-per-day-per-source."
    return ns


def _maybe_add_url_as_source(store: Store, n: UrlNormaliser, url: str, log_fn) -> bool:
    raw = (url or "").strip()
    if not raw:
        return False
    try:
        n.extract_channel_ref(raw)
        sid = store.add_channel_source(raw)
        log_fn(f"Added channel source id={sid}")
        return True
    except Exception:
        pass
    try:
        rss_norm = n.normalise_feed_url(raw)
        if n.looks_like_peertube_feed(rss_norm):
            sid = store.add_rss_source(raw)
            log_fn(f"Added RSS source id={sid} (canonical: {rss_norm})")
            return True
    except Exception:
        return False
    return False


def _normalize_cmd(cmd: str) -> str:
    if not cmd:
        return cmd
    raw = cmd.strip().lower()
    if raw.startswith("/"):
        raw = raw[1:]
    return raw.rstrip(".:,;")


def _npub_from_pubkey(pubkey_obj) -> Optional[str]:
    for attr in ("bech32", "to_bech32", "npub", "to_npub"):
        fn = getattr(pubkey_obj, attr, None)
        if callable(fn):
            try:
                val = fn()
                if isinstance(val, str) and val.startswith("npub"):
                    return val
            except Exception:
                continue
    return None


def _privkey_to_hex(priv) -> Optional[str]:
    for attr in ("hex", "to_hex", "private_key", "secret", "raw_secret"):
        val = getattr(priv, attr, None)
        try:
            if callable(val):
                v = val()
            else:
                v = val
        except Exception:
            continue
        if isinstance(v, str) and v:
            return v
    return None


def _apply_edit_source(store: Store, n: UrlNormaliser, source_id: str, channel_url: Optional[str], rss_url: Optional[str], log_fn) -> None:
    try:
        sid = int(str(source_id).strip())
    except ValueError:
        log_fn("Invalid source id.")
        return
    updates = []
    if channel_url:
        if str(channel_url).strip().lower() in ("none", "null", "off", "-"):
            store.clear_source_channel(sid)
            updates.append("channel cleared")
        else:
            store.set_source_channel(sid, channel_url)
            updates.append("channel")
    if rss_url:
        if str(rss_url).strip().lower() in ("none", "null", "off", "-"):
            store.clear_source_rss(sid)
            updates.append("rss cleared")
        else:
            rss_norm = n.normalise_feed_url(rss_url)
            if not n.looks_like_peertube_feed(rss_norm):
                log_fn("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
            store.set_source_rss(sid, rss_url)
            updates.append("rss")
    if not updates:
        log_fn("No changes requested.")
        return
    log_fn(f"Updated source {sid}: {', '.join(updates)}")
    _resync_source(store, n, sid, log_fn)


def _resync_source(store: Store, n: UrlNormaliser, source_id: int, log_fn) -> None:
    cleared = store.clear_pending_for_source(source_id)
    if cleared:
        log_fn(f"Cleared pending items: {cleared}")
    api_limit = int(os.environ.get("API_LIMIT_PER_SOURCE", "50"))
    lookback_days = int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30"))
    runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n, log_fn=log_fn)
    runner.ingest_source_once(source_id, api_limit, lookback_days)


def repair_db(store: Store, n: UrlNormaliser, log_fn) -> None:
    counts = {"relays": 0, "sources": 0, "videos": 0, "published_ts": 0}

    rows = store.conn.execute("SELECT id, relay_url FROM relays").fetchall()
    for rid, relay_url in rows:
        try:
            norm = n.normalise_relay_url(relay_url)
        except Exception:
            continue
        store.conn.execute("UPDATE relays SET relay_url_norm=? WHERE id=?", (norm, rid))
        counts["relays"] += 1

    rows = store.conn.execute("SELECT id, api_base, api_channel_url, rss_url FROM sources").fetchall()
    for sid, api_base, api_channel_url, rss_url in rows:
        if api_base:
            try:
                base_norm = n.normalise_http_url(api_base)
                store.conn.execute("UPDATE sources SET api_base_norm=? WHERE id=?", (base_norm, sid))
            except Exception:
                pass
        if api_channel_url:
            try:
                chan_norm = n.normalise_http_url(api_channel_url)
                store.conn.execute("UPDATE sources SET api_channel_url_norm=? WHERE id=?", (chan_norm, sid))
            except Exception:
                pass
        if rss_url:
            try:
                rss_norm = n.normalise_feed_url(rss_url)
                store.conn.execute("UPDATE sources SET rss_url_norm=? WHERE id=?", (rss_norm, sid))
            except Exception:
                pass
        counts["sources"] += 1

    rows = store.conn.execute(
        "SELECT id, watch_url, published_ts, first_seen_ts FROM videos"
    ).fetchall()
    for vid, watch_url, published_ts, first_seen_ts in rows:
        try:
            norm = n.normalise_watch_url(watch_url)
            store.conn.execute("UPDATE videos SET watch_url_norm=? WHERE id=?", (norm, vid))
            counts["videos"] += 1
        except Exception:
            pass
        if published_ts is None and first_seen_ts is not None:
            store.conn.execute("UPDATE videos SET published_ts=? WHERE id=?", (int(first_seen_ts), vid))
            counts["published_ts"] += 1

    store.conn.commit()
    log_fn(
        "Repair complete: "
        f"relays={counts['relays']} sources={counts['sources']} "
        f"videos_normed={counts['videos']} published_ts_filled={counts['published_ts']}"
    )


def _event_get(ev, key: str) -> Any:
    if isinstance(ev, dict):
        return ev.get(key)
    return getattr(ev, key, None)


def _extract_event_from_msg(msg) -> Optional[Any]:
    if msg is None:
        return None
    if isinstance(msg, dict) and "event" in msg:
        return msg.get("event")
    ev = getattr(msg, "event", None)
    if ev is not None:
        return ev
    return msg


def _fetch_latest_profile_events(relays: list[str], pubkey_hex: str, timeout_seconds: int) -> tuple[dict[int, Any], int]:
    rm = RelayManager(timeout=timeout_seconds)
    relay_errors = 0
    for r in relays:
        try:
            rm.add_relay(r)
        except Exception:
            relay_errors += 1

    filters = [{"authors": [pubkey_hex], "kinds": [0, 10002]}]

    try:
        if hasattr(rm, "add_subscription"):
            sub_id = f"pt2n-sync-{int(time.time() * 1000)}"
            sub = rm.add_subscription(sub_id)
            if hasattr(sub, "add_filters"):
                sub.add_filters(filters)
            elif hasattr(sub, "set_filters"):
                sub.set_filters(filters)
        elif hasattr(rm, "add_subscription_on_all_relays"):
            rm.add_subscription_on_all_relays("pt2n-sync", filters)
    except Exception:
        relay_errors += 1

    try:
        if hasattr(rm, "open_connections"):
            rm.open_connections()
    except Exception:
        relay_errors += 1

    latest: dict[int, Any] = {}
    start = time.time()
    mp = getattr(rm, "message_pool", None)
    while time.time() - start < timeout_seconds:
        got = False
        if mp is not None and hasattr(mp, "has_events") and hasattr(mp, "get_event"):
            while mp.has_events():
                got = True
                msg = mp.get_event()
                ev = _extract_event_from_msg(msg)
                if ev is None:
                    continue
                kind = int(_event_get(ev, "kind") or 0)
                created_at = int(_event_get(ev, "created_at") or 0)
                if kind not in (0, 10002):
                    continue
                prev = latest.get(kind)
                prev_ts = int(_event_get(prev, "created_at") or 0) if prev else 0
                if created_at > prev_ts:
                    latest[kind] = ev
        elif hasattr(rm, "run_sync"):
            try:
                rm.run_sync()
            except Exception:
                relay_errors += 1
                break
        if not got:
            time.sleep(0.1)

    try:
        if hasattr(rm, "close_connections"):
            rm.close_connections()
    except Exception:
        relay_errors += 1

    return latest, relay_errors


def _parse_profile_content(ev) -> dict:
    content = _event_get(ev, "content") or ""
    if isinstance(content, str):
        try:
            return json.loads(content) if content else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _parse_nip65_relays(ev) -> list[dict]:
    tags = _event_get(ev, "tags") or []
    out: dict[str, dict] = {}
    for tag in tags:
        if not isinstance(tag, (list, tuple)) or len(tag) < 2:
            continue
        if tag[0] != "r":
            continue
        url = str(tag[1]).strip()
        if not url:
            continue
        markers = {str(x).lower() for x in tag[2:]}
        if "read" in markers or "write" in markers:
            read = "read" in markers
            write = "write" in markers
        else:
            read = True
            write = True
        if url not in out:
            out[url] = {"url": url, "read": read, "write": write}
        else:
            out[url]["read"] = out[url]["read"] or read
            out[url]["write"] = out[url]["write"] or write
    return list(out.values())


def _format_rw(read: bool, write: bool) -> str:
    if read and write:
        return "read/write"
    if read:
        return "read"
    if write:
        return "write"
    return "none"


def sync_profile(
    store: Store,
    n: UrlNormaliser,
    nsec_arg: Optional[str],
    relays_arg: Optional[str],
    import_relays: bool,
    enable_imported: bool,
    disable_missing: bool,
    timeout_seconds: int,
    log_fn=print,
) -> None:
    nsec = os.environ.get("NOSTR_NSEC") or nsec_arg or get_stored_nsec(store.db_path)
    if not nsec:
        raise SystemExit("Provide nsec via --nsec or NOSTR_NSEC.")

    if relays_arg and relays_arg.strip():
        relays = [n.normalise_relay_url(x.strip()) for x in relays_arg.split(",") if x.strip()]
    else:
        relays_env = os.environ.get("NOSTR_RELAYS")
        if relays_env and relays_env.strip():
            relays = [n.normalise_relay_url(x.strip()) for x in relays_env.split(",") if x.strip()]
        else:
            relays = store.get_enabled_relays() or DEFAULT_RELAYS

    priv = PrivateKey.from_nsec(nsec)
    pub = priv.public_key
    pub_hex = pub.hex()
    npub = _npub_from_pubkey(pub) or "-"

    log_fn(f"Pubkey: {pub_hex} | npub: {npub}")
    log_fn(f"Bootstrap relays: {', '.join(relays)}")

    latest, relay_errors = _fetch_latest_profile_events(relays, pub_hex, timeout_seconds)

    profile_ev = latest.get(0)
    relays_ev = latest.get(10002)

    if not profile_ev:
        log_fn("No profile metadata found.")
    else:
        meta = _parse_profile_content(profile_ev)
        name = (meta.get("name") or meta.get("display_name") or "").strip()
        display = (meta.get("display_name") or meta.get("displayName") or "").strip()
        nip05 = (meta.get("nip05") or "").strip()
        website = (meta.get("website") or meta.get("url") or "").strip()
        picture = (meta.get("picture") or "").strip()

        log_fn("Profile:")
        if name or display:
            log_fn(f"  name: {name or display}")
        if display and display != name:
            log_fn(f"  display_name: {display}")
        if nip05:
            log_fn(f"  nip05: {nip05}")
        if website:
            log_fn(f"  website: {website}")
        if picture:
            log_fn(f"  picture: {picture}")

    if not relays_ev:
        log_fn("No NIP-65 relay list found.")
        if relay_errors:
            log_fn(f"Relay errors: {relay_errors}")
        return

    nip65 = _parse_nip65_relays(relays_ev)
    if not nip65:
        log_fn("No NIP-65 relay list found.")
        if relay_errors:
            log_fn(f"Relay errors: {relay_errors}")
        return

    log_fn("NIP-65 relays:")
    for r in nip65:
        log_fn(f"  {r['url']} ({_format_rw(r['read'], r['write'])})")

    if relay_errors:
        log_fn(f"Relay errors: {relay_errors}")

    if import_relays:
        imported_norms: set[str] = set()
        import_errors = 0
        for r in nip65:
            try:
                norm = n.normalise_relay_url(r["url"])
            except Exception:
                import_errors += 1
                continue
            try:
                store.add_relay_with_enabled(norm, enabled=enable_imported)
                if not enable_imported:
                    store.set_relay_enabled(norm, False)
                imported_norms.add(norm)
            except Exception:
                import_errors += 1

        log_fn(f"Imported relays: {len(imported_norms)}")
        if import_errors:
            log_fn(f"Import errors: {import_errors}")

        if disable_missing:
            rows = store.list_relays()
            disabled = 0
            for (rid, _enabled, _url, url_norm, _last_used_ts, _last_error) in rows:
                if not url_norm:
                    continue
                if url_norm not in imported_norms:
                    store.set_relay_enabled(str(rid), False)
                    disabled += 1
            log_fn(f"Disabled missing relays: {disabled}")


def _run_interactive(
    args: argparse.Namespace,
    n: UrlNormaliser,
    nsec_env: Optional[str],
    relays: Optional[list[str]],
    retry: Optional[int],
) -> None:
    stop_event = threading.Event()
    log_queue: Optional[Queue] = Queue() if App is not None else None

    def _log_fn(msg: str) -> None:
        if log_queue is not None:
            log_queue.put(msg)
        else:
            print(msg)

    def _status_fn(msg: str) -> None:
        _set_runtime_status(msg)
        if log_queue is None:
            return

    def _runner_thread() -> None:
        thread_store = Store(args.db, n)
        thread_store.init_schema()
        try:
            thread_runner = Runner(
                thread_store,
                PeerTubeClient(n),
                NostrPublisher(),
                n,
                log_fn=_log_fn if log_queue else None,
                status_fn=_status_fn,
            )
            thread_runner.run(
                nsec=nsec_env,
                relays=relays,
                poll_seconds=args.poll_seconds,
                publish_interval_seconds=args.publish_interval_seconds,
                retry_failed_after_seconds=retry,
                api_limit_per_source=args.api_limit_per_source,
                new_source_lookback_days=args.new_source_lookback_days,
                stop_event=stop_event,
            )
        finally:
            thread_store.close()

    t = threading.Thread(target=_runner_thread, daemon=True)
    t.start()

    if App is not None and log_queue is not None:
        _interactive_tui(args.db, n, stop_event, log_queue)
    else:
        _interactive_shell(args.db, n, stop_event)
    t.join()


def _interactive_tui(db_path: str, n: UrlNormaliser, stop_event: threading.Event, log_queue: Queue) -> None:
    class PeerTubeTUI(App):
        CSS = """
        Screen { layout: vertical; }
        #body { height: 1fr; }
        #log { height: 1fr; }
        #status { height: 1; display: none; }
        #input_row { height: 3; }
        #prompt { width: 6; content-align: right middle; color: #aaaaaa; }
        #input { height: 3; border: round #4c9aff; }
        #palette { height: 6; border: round #666666; display: none; }
        #panels { height: auto; }
        #panel_counts { height: auto; }
        #panel_activity { height: auto; }
        #panel_rate { height: auto; }
        #panel_queue { height: 10; }
        #queue_title { height: 1; }
        #queue_list { height: 1fr; }
        .panel {
            border: round #666666;
            padding: 0 1;
            width: 1fr;
        }
        #panel_counts { border: round #4c9aff; }
        #panel_activity { border: round #67b26f; }
        #panel_rate { border: round #ffb347; }
        #panel_queue { border: round #b39ddb; }
        #status { background: #222222; color: #b0bec5; }
        """
        BINDINGS = [
            ("/", "palette", "Commands"),
            ("?", "help", "Help"),
            ("tab", "complete", "Complete"),
            ("down", "palette_down", "Next"),
            ("up", "palette_up", "Prev"),
            ("d", "toggle_dashboard", "Dashboard"),
            ("pageup", "queue_up", "Queue Up"),
            ("pagedown", "queue_down", "Queue Down"),
            ("ctrl+l", "clear", "Clear"),
            ("ctrl+c", "quit", "Quit"),
        ]
        TAB_FOCUS_NEXT = False
        ENABLE_TAB_FOCUS = False

        def __init__(self) -> None:
            super().__init__()
            self.store = Store(db_path, n)
            self.store.init_schema()
            self._wizard_queue: list[tuple[str, callable, bool]] = []
            self._wizard_active = False
            self._pending_secret = False
            self._palette_visible = False
            self._commands = _interactive_commands()
            self._palette_map: dict[str, str] = {}
            self._palette_gen = 0
            self._palette_force = False
            self._pending_cmd: Optional[str] = None
            self._pending_args: list[str] = []
            self._pending_prompts: list[tuple[str, bool]] = []
            self._pending_allow_blank = False
            self._last_prompt = ""
            self._pending_edit_choice = ""
            self._palette_mode = "commands"
            self._dashboard_visible = True
            self._queue_cache: list[str] = []

        class CommandInput(Input):
            def key_tab(self) -> None:
                self.app.action_complete()
                self.focus()

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            with Vertical(id="body"):
                with Horizontal(id="panels"):
                    yield Static(id="panel_counts", classes="panel")
                    yield Static(id="panel_activity", classes="panel")
                    yield Static(id="panel_rate", classes="panel")
                with Vertical(id="panel_queue", classes="panel"):
                    yield Static("Next Posts", id="queue_title")
                    yield ListView(id="queue_list")
                yield RichLog(id="log", wrap=True, highlight=True, markup=False)
                yield Static(id="status")
                with Horizontal(id="input_row"):
                    yield Static("cmd>", id="prompt")
                    yield self.CommandInput(id="input", placeholder="Type / for commands")
                yield ListView(id="palette")
            yield Footer()

        def on_mount(self) -> None:
            self.set_interval(0.25, self._drain_logs)
            self.set_interval(1.0, self._refresh_status)
            self._log("== PeerTube2Nostr Interactive ==")
            self._log("Type '/' for commands. 'quit' to exit.")
            self._start_wizard_if_needed()
            self._apply_dashboard_visibility()

        def on_unmount(self) -> None:
            stop_event.set()
            self.store.close()

        def _log(self, msg: str) -> None:
            self.query_one("#log", RichLog).write(msg)

        def _emit_help(self) -> None:
            for line in _help_lines():
                self._log(line)

        def _drain_logs(self) -> None:
            while True:
                try:
                    msg = log_queue.get_nowait()
                except Empty:
                    break
                self._log(msg)

        def _refresh_status(self) -> None:
            if self._dashboard_visible:
                panels = _format_dashboard_panels(self.store, db_path)
                self.query_one("#panel_counts", Static).update(panels["counts"])
                self.query_one("#panel_activity", Static).update(panels["activity"])
                self.query_one("#panel_rate", Static).update(panels["rate"])
                self._update_queue_list(panels["queue"])

        def action_help(self) -> None:
            self._emit_help()

        def action_palette(self) -> None:
            inp = self.query_one("#input", Input)
            if not inp.value:
                inp.value = "/"
            inp.focus()
            self._palette_mode = "commands"
            self._show_palette(True)
            self._update_palette(inp.value)

        def action_toggle_dashboard(self) -> None:
            self._dashboard_visible = not self._dashboard_visible
            self._apply_dashboard_visibility()

        def action_complete(self) -> None:
            inp = self.query_one("#input", Input)
            text = inp.value
            matches = self._palette_matches(text)
            if not matches:
                return
            choice = matches[0]
            inp.value = choice + " "
            inp.focus()
            self._show_palette(False)
            self._palette_force = True

        def action_palette_down(self) -> None:
            if not self._palette_visible:
                self.action_palette()
            palette = self.query_one("#palette", ListView)
            palette.focus()
            palette.action_cursor_down()

        def action_palette_up(self) -> None:
            if not self._palette_visible:
                self.action_palette()
            palette = self.query_one("#palette", ListView)
            palette.focus()
            palette.action_cursor_up()

        def action_queue_up(self) -> None:
            if not self._dashboard_visible:
                return
            queue = self.query_one("#queue_list", ListView)
            queue.focus()
            for _ in range(5):
                queue.action_cursor_up()

        def action_queue_down(self) -> None:
            if not self._dashboard_visible:
                return
            queue = self.query_one("#queue_list", ListView)
            queue.focus()
            for _ in range(5):
                queue.action_cursor_down()

        def action_clear(self) -> None:
            self.query_one("#log", RichLog).clear()

        def action_quit(self) -> None:
            stop_event.set()
            self.exit()

        def on_input_changed(self, event: Input.Changed) -> None:
            if self._wizard_active:
                return
            if getattr(self, "_palette_force", False):
                self._palette_force = False
                return
            text = event.value
            if self._palette_mode != "commands":
                return
            if text.startswith("/") or self._palette_visible:
                self._show_palette(True)
                self._update_palette(text)
            else:
                self._show_palette(False)

        def on_input_submitted(self, event: Input.Submitted) -> None:
            line = event.value.strip()
            event.input.value = ""
            if not line:
                return

            if self._wizard_active:
                self._handle_wizard_input(line)
                return

            if self._pending_secret:
                self._pending_secret = False
                event.input.password = False
                event.input.placeholder = "Type / for commands"
                cmd = "set-nsec"
                args = [line]
            else:
                if self._pending_cmd:
                    if line.lower() in ("cancel", "exit"):
                        self._reset_pending(event.input, canceled=True)
                        return
                    if not line and not self._pending_allow_blank:
                        self._reset_pending(event.input, canceled=True)
                        return
                    self._pending_args.append(line)
                    if self._pending_prompts:
                        self._prompt_next_pending(event.input)
                        return
                    cmd = self._pending_cmd
                    args = self._pending_args
                    if cmd == "edit-source":
                        if len(args) == 1 and not self._pending_edit_choice:
                            self._pending_prompts = [("Change what? (channel/rss/both)", False)]
                            self._prompt_next_pending(event.input)
                            return
                        if self._pending_edit_choice == "":
                            self._pending_edit_choice = args[1].strip().lower() if len(args) > 1 else ""
                            if self._pending_edit_choice not in ("channel", "rss", "both"):
                                self._log("Choose: channel, rss, or both.")
                                self._pending_args = [args[0]]
                                self._pending_prompts = [("Change what? (channel/rss/both)", False)]
                                self._prompt_next_pending(event.input)
                                return
                            prompts = []
                            if self._pending_edit_choice in ("channel", "both"):
                                prompts.append(("Channel URL (blank to skip, 'none' to clear)", True))
                            if self._pending_edit_choice in ("rss", "both"):
                                prompts.append(("RSS URL (blank to skip, 'none' to clear)", True))
                            self._pending_prompts = prompts
                            self._pending_args = [args[0], self._pending_edit_choice]
                            self._prompt_next_pending(event.input)
                            return
                        source_id = args[0] if len(args) > 0 else ""
                        choice = args[1] if len(args) > 1 else ""
                        channel_url = None
                        rss_url = None
                        cursor = 2
                        if choice in ("channel", "both"):
                            channel_url = args[cursor] if len(args) > cursor and args[cursor] else None
                            cursor += 1
                        if choice in ("rss", "both"):
                            rss_url = args[cursor] if len(args) > cursor and args[cursor] else None
                        _apply_edit_source(self.store, n, source_id, channel_url, rss_url, self._log)
                        self._reset_pending(event.input)
                        return
                    if cmd == "add-channel":
                        self._wiz_add_channel(args[0] if args else "")
                        self._reset_pending(event.input)
                        return
                    if cmd == "add-rss":
                        if not _maybe_add_url_as_source(self.store, n, args[0] if args else "", self._log):
                            self._log("Invalid RSS URL.")
                        self._reset_pending(event.input)
                        return
                    if cmd == "add-source":
                        self._wiz_add_source(args[0] if args else "")
                        self._reset_pending(event.input)
                        return
                    if cmd == "set-rate":
                        min_int = args[0] if len(args) > 0 else ""
                        max_per = args[1] if len(args) > 1 else ""
                        max_day = args[2] if len(args) > 2 else ""
                        parsed = _parse_set_rate_args(
                            ([f"--min-interval-seconds={min_int}"] if min_int else [])
                            + ([f"--max-posts-per-hour={max_per}"] if max_per else [])
                            + ([f"--max-posts-per-day-per-source={max_day}"] if max_day else [])
                        )
                        if isinstance(parsed, str):
                            self._log(parsed)
                        else:
                            if parsed.min_interval_seconds is not None:
                                self.store.set_setting("min_publish_interval_seconds", str(int(parsed.min_interval_seconds)))
                            if parsed.max_posts_per_hour is not None:
                                self.store.set_setting("max_posts_per_hour", str(int(parsed.max_posts_per_hour)))
                            if parsed.max_posts_per_day_per_source is not None:
                                self.store.set_setting(
                                    "max_posts_per_day_per_source",
                                    str(int(parsed.max_posts_per_day_per_source)),
                                )
                            min_interval, max_per_hour = self.store.get_publish_limits()
                            max_per_day_per_source = self.store.get_daily_source_limit()
                            self._log(
                                "Rate limits: "
                                f"min_interval_seconds={min_interval}, "
                                f"max_posts_per_hour={max_per_hour}, "
                                f"max_posts_per_day_per_source={max_per_day_per_source}"
                            )
                        self._reset_pending(event.input)
                        return
                    if cmd == "resync-source":
                        source_id = args[0] if args else ""
                        if not source_id:
                            self._log("Canceled.")
                        else:
                            _resync_source(self.store, n, int(source_id), self._log)
                        self._reset_pending(event.input)
                        return
                    if cmd == "retry-failed":
                        if args:
                            count = self.store.retry_failed_for_source(int(args[0]), older_than_seconds=0)
                            self._log(f"Re-queued failed items for source {args[0]}: {count}")
                        else:
                            count = self.store.retry_failed(older_than_seconds=0)
                            self._log(f"Re-queued failed items: {count}")
                        self._reset_pending(event.input)
                        return
                    self._reset_pending(event.input)
                else:
                    if line.startswith(("http://", "https://")) and self._last_prompt:
                        lp = self._last_prompt.lower()
                        if "channel or rss" in lp:
                            self._wiz_add_source(line)
                            return
                        if "channel url" in lp:
                            self._wiz_add_channel(line)
                            return
                        if "rss url" in lp:
                            if not _maybe_add_url_as_source(self.store, n, line, self._log):
                                self._log("Invalid RSS URL.")
                            return

                    parts = shlex.split(line)
                    cmd = _normalize_cmd(parts[0])
                    args = parts[1:]

                    if cmd not in self._commands and not args and line.startswith(("http://", "https://")):
                        if _maybe_add_url_as_source(self.store, n, line.strip(), self._log):
                            return

                    if cmd == "set-nsec" and not args:
                        self._pending_secret = True
                        event.input.password = True
                        event.input.placeholder = "Enter nsec:"
                        self._log("Enter nsec:")
                        return

                    if cmd == "/":
                        self._show_palette(True)
                        self._update_palette("/")
                        return

                    arg_prompts = _interactive_arg_prompts()
                    if cmd in arg_prompts and len(args) < len(arg_prompts[cmd]):
                        self._pending_cmd = cmd
                        self._pending_args = args
                        self._pending_prompts = [(p, False) for p in arg_prompts[cmd][len(args):]]
                        self._prompt_next_pending(event.input)
                        return
                    if cmd == "edit-source" and len(args) < 1:
                        self._pending_cmd = cmd
                        self._pending_args = args
                        prompts = [("Source id", False)]
                        self._pending_prompts = prompts[len(args):]
                        self._prompt_next_pending(event.input)
                        return
                    if cmd == "set-rate" and len(args) < 3:
                        self._pending_cmd = cmd
                        self._pending_args = args
                        prompts = [
                            ("Min interval seconds", False),
                            ("Max posts per hour", False),
                            ("Max posts per day per source", False),
                        ]
                        self._pending_prompts = prompts[len(args):]
                        self._prompt_next_pending(event.input)
                        return
                    if cmd == "resync-source" and len(args) < 1:
                        self._pending_cmd = cmd
                        self._pending_args = args
                        prompts = [("Source id", False)]
                        self._pending_prompts = prompts[len(args):]
                        self._prompt_next_pending(event.input)
                        return
                    if cmd == "repair-db":
                        repair_db(self.store, n, self._log)
                        return
                    if cmd == "retry-failed":
                        if not args:
                            self._pending_cmd = cmd
                            self._pending_args = args
                            prompts = [("Source id (blank for all)", True)]
                            self._pending_prompts = prompts[len(args):]
                            self._prompt_next_pending(event.input)
                            return

            should_quit = _dispatch_command(self.store, n, db_path, cmd, args, self._log)
            if should_quit:
                self.action_quit()

        def on_list_view_selected(self, event: ListView.Selected) -> None:
            if not self._palette_visible:
                return
            value = ""
            item_id = event.item.id or ""
            if item_id in self._palette_map:
                value = self._palette_map[item_id]
            if not value:
                label = event.item.query_one(Label)
                if hasattr(label, "text"):
                    value = str(label.text).strip()
                else:
                    value = str(label).strip()
            inp = self.query_one("#input", Input)
            if value:
                inp.value = value + " "
            inp.focus()
            self._show_palette(False)
            self._palette_mode = "commands"

        def _start_wizard_if_needed(self) -> None:
            has_sources = self.store.count_sources() > 0
            has_nsec = bool(get_stored_nsec(db_path))

            if self.store.count_relays() == 0:
                self._wizard_queue.append(("Seed default relays? [Y/n]:", self._wiz_seed_relays, False))
            if not has_nsec:
                self._wizard_queue.append(("Set nsec now? [Y/n]:", self._wiz_ask_nsec, False))
            if not has_sources:
                self._wizard_queue.append(("Add PeerTube channel URL (blank to skip):", self._wiz_add_channel, False))

            if self._wizard_queue:
                self._wizard_active = True
                self._advance_wizard()

        def _show_palette(self, show: bool) -> None:
            self._palette_visible = show
            palette = self.query_one("#palette", ListView)
            palette.styles.display = "block" if show else "none"

        def _palette_matches(self, query: str) -> list[str]:
            q = query.lstrip("/").strip().lower()
            matches = []
            for cmd in self._commands:
                if not q:
                    matches.append(cmd)
                elif cmd.startswith(q) or q in cmd:
                    matches.append(cmd)
            return matches

        def _update_palette(self, query: str) -> None:
            if not self._palette_visible:
                return
            if self._palette_mode != "commands":
                return
            matches = self._palette_matches(query)
            palette = self.query_one("#palette", ListView)
            palette.clear()
            self._palette_gen += 1
            self._palette_map = {}
            for i, cmd in enumerate(matches[:50]):
                item_id = f"cmd_{self._palette_gen}_{i}"
                self._palette_map[item_id] = cmd
                palette.append(ListItem(Label(cmd), id=item_id))

        def _set_palette_items(self, items: list[tuple[str, str]], mode: str) -> None:
            self._palette_mode = mode
            palette = self.query_one("#palette", ListView)
            palette.clear()
            self._palette_gen += 1
            self._palette_map = {}
            for i, (label, value) in enumerate(items[:50]):
                item_id = f"pick_{self._palette_gen}_{i}"
                self._palette_map[item_id] = value
                palette.append(ListItem(Label(label), id=item_id))
            self._show_palette(True)

        def _apply_dashboard_visibility(self) -> None:
            panels = self.query_one("#panels", Horizontal)
            queue = self.query_one("#panel_queue", Vertical)
            if self._dashboard_visible:
                panels.styles.display = "block"
                queue.styles.display = "block"
            else:
                panels.styles.display = "none"
                queue.styles.display = "none"

        def _update_queue_list(self, items: list[str]) -> None:
            if items == self._queue_cache:
                return
            self._queue_cache = list(items)
            lv = self.query_one("#queue_list", ListView)
            lv.clear()
            for text in items:
                lv.append(ListItem(Label(text)))

        def _advance_wizard(self) -> None:
            if not self._wizard_queue:
                self._wizard_active = False
                inp = self.query_one("#input", Input)
                inp.password = False
                inp.placeholder = "Type / for commands"
                self._log("Wizard complete.")
                return
            prompt, _handler, password = self._wizard_queue[0]
            inp = self.query_one("#input", Input)
            inp.password = password
            inp.placeholder = prompt
            self._log(prompt)

        def _prompt_next_pending(self, inp: Input) -> None:
            if not self._pending_prompts:
                return
            prompt, allow_blank = self._pending_prompts.pop(0)
            self._last_prompt = prompt
            self._pending_allow_blank = allow_blank
            inp.placeholder = prompt
            self._log(prompt)
            pl = prompt.lower()
            if "source id" in pl:
                rows = self.store.list_sources()
                items = []
                for (sid, _enabled, api_base, api_channel, _api_channel_url, rss_url, _lookback_days, _last_polled_ts, _last_error) in rows:
                    label = f"{sid}: {api_base or ''} {api_channel or ''}".strip()
                    if rss_url:
                        label = f"{label} | rss"
                    items.append((label, str(sid)))
                if items:
                    self._set_palette_items(items, "sources")
            elif "relay id" in pl:
                rows = self.store.list_relays()
                items = []
                for (rid, _enabled, url, _url_norm, _last_used_ts, _last_error) in rows:
                    items.append((f"{rid}: {url}", str(rid)))
                if items:
                    self._set_palette_items(items, "relays")

        def _reset_pending(self, inp: Input, canceled: bool = False) -> None:
            self._pending_cmd = None
            self._pending_args = []
            self._pending_prompts = []
            self._pending_allow_blank = False
            self._last_prompt = ""
            self._pending_edit_choice = ""
            inp.placeholder = "Type / for commands"
            if canceled:
                self._log("Canceled.")

        def _handle_wizard_input(self, value: str) -> None:
            prompt, handler, _password = self._wizard_queue.pop(0)
            new_steps = handler(value.strip()) or []
            if new_steps:
                self._wizard_queue = new_steps + self._wizard_queue
            self._advance_wizard()

        def _wiz_seed_relays(self, value: str) -> list[tuple[str, callable, bool]]:
            ans = value.lower()
            if ans in ("", "y", "yes"):
                self.store.seed_default_relays_if_empty()
                self._log("Seeded default relays.")
            return []

        def _wiz_ask_nsec(self, value: str) -> list[tuple[str, callable, bool]]:
            ans = value.lower()
            if ans in ("", "y", "yes"):
                return [("Enter nsec:", self._wiz_set_nsec, True)]
            return []

        def _wiz_set_nsec(self, value: str) -> list[tuple[str, callable, bool]]:
            if not value:
                self._log("nsec cannot be empty.")
                return []
            store_type, path = set_stored_nsec(db_path, value)
            if store_type == "keyring":
                self._log("Stored nsec in OS keyring for this DB path.")
            else:
                self._log(f"Stored nsec in file: {path}")
            return []

        def _wiz_add_channel(self, value: str) -> list[tuple[str, callable, bool]]:
            if not value:
                return []
            try:
                sid = self.store.add_channel_source(value)
                self._log(f"Added channel source id={sid}")
            except Exception as ex:
                self._log(f"Failed to add channel: {ex}")
                return []
            return [("Add RSS fallback URL (blank to skip):", lambda v: self._wiz_set_rss(v, sid), False)]

        def _wiz_set_rss(self, value: str, sid: int) -> list[tuple[str, callable, bool]]:
            if not value:
                return []
            try:
                rss_norm = n.normalise_feed_url(value)
                if not n.looks_like_peertube_feed(rss_norm):
                    self._log("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
                self.store.set_source_rss(sid, value)
                self._log(f"Set RSS fallback for source {sid} (canonical: {rss_norm})")
            except Exception as ex:
                self._log(f"Failed to set RSS: {ex}")
            return []

        def _wiz_add_source(self, value: str) -> list[tuple[str, callable, bool]]:
            if not value:
                return []
            try:
                sid = self.store.add_channel_source(value)
                self._log(f"Added channel source id={sid}")
                return [("Add RSS fallback URL (blank to skip):", lambda v: self._wiz_set_rss(v, sid), False)]
            except Exception:
                pass
            try:
                rss_norm = n.normalise_feed_url(value)
                if not n.looks_like_peertube_feed(rss_norm):
                    self._log("Warning: RSS URL does not look like a typical PeerTube feed (still adding).")
                sid = self.store.add_rss_source(value)
                self._log(f"Added RSS source id={sid} (canonical: {rss_norm})")
            except Exception as ex:
                self._log(f"Failed to add source: {ex}")
            return []

    PeerTubeTUI().run()


if __name__ == "__main__":
    main()
