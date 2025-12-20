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
import getpass
import os
import re
import sqlite3
import sys
import threading
import time
import shlex
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
    from textual.containers import Vertical
    from textual.widgets import Header, Footer, Input, Static, RichLog
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
            re.compile(r"^/video-channels/([^/]+)$"),
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
        path = p.path
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
                last_error TEXT
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

                status TEXT NOT NULL DEFAULT 'pending', -- pending|posted|failed
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

        self._add_column("videos", "peertube_instance", "TEXT")
        self._add_column("videos", "channel_name", "TEXT")
        self._add_column("videos", "channel_url", "TEXT")
        self._add_column("videos", "account_name", "TEXT")
        self._add_column("videos", "account_url", "TEXT")

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_watch_norm ON videos(watch_url_norm);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_first_seen ON videos(first_seen_ts);")

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
        raw = (relay_url or "").strip()
        norm = self.n.normalise_relay_url(raw)
        ts = self.n.now_ts()
        self.conn.execute(
            "INSERT OR IGNORE INTO relays(relay_url, relay_url_norm, enabled, created_ts) VALUES (?, ?, 1, ?)",
            (raw, norm, ts),
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
                   rss_url,
                   last_polled_ts, last_error
            FROM sources
            ORDER BY id ASC
            """
        ).fetchall()

    def get_enabled_sources(self) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT id, api_base, api_channel, api_channel_url, rss_url
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
                }
            )
        return out

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
             status, first_seen_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
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
                ts,
            ),
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
            ORDER BY v.first_seen_ts ASC
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
            candidates.append((score, fu))

        for f in (v.get("files") or []):
            consider_file(f)
        for pl in (v.get("streamingPlaylists") or []):
            for f in (pl.get("files") or []):
                consider_file(f)

        if not candidates:
            return None
        candidates.sort(reverse=True, key=lambda x: x[0])
        return candidates[0][1]

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
        ev = Event(kind=1, public_key=priv.public_key.hex(), content=content, tags=tags)
        priv.sign_event(ev)

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
    ) -> None:
        self.store = store
        self.pt = pt
        self.pub = pub
        self.n = n
        self.log_fn = log_fn

    def _log(self, msg: str) -> None:
        if self.log_fn:
            self.log_fn(msg)
        else:
            print(msg)

    def ingest_sources_once(self, api_limit_per_source: int) -> None:
        sources = self.store.get_enabled_sources()
        for s in sources:
            sid = s["id"]
            api_base = s.get("api_base")
            api_channel = s.get("api_channel")
            rss_url = s.get("rss_url")

            inserted = 0
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
                            continue

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
                        )
                        self.store.insert_pending(item)
                        inserted += 1

                    self.store.mark_source_polled(sid, None)
                    if inserted:
                        self._log(f"[source {sid}] API new items: {inserted}")
                    continue  # API succeeded, no need RSS fallback

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
                            continue

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
                        )
                        self.store.insert_pending(item)
                        inserted += 1

                    self.store.mark_source_polled(sid, None if not err else err)
                    if inserted:
                        self._log(f"[source {sid}] RSS new items: {inserted}")
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

                self.ingest_sources_once(api_limit_per_source=api_limit_per_source)

                # publish at most one per loop iteration
                if nsec:
                    self.publish_one_pending(nsec=nsec, relays=relays or [])

                time.sleep(publish_interval_seconds)
                time.sleep(poll_seconds)
            except KeyboardInterrupt:
                self._log("\nStopped.")
                return
            except Exception as ex:
                self._log(f"Loop error: {ex}")
                time.sleep(poll_seconds)


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

    s = sub.add_parser("add-rss", help="Add an RSS-only source (fallback ingest only)")
    s.add_argument("rss_url")
    s.set_defaults(cmd="add-rss")

    s = sub.add_parser("set-rss", help="Set/replace RSS fallback URL for an existing source id")
    s.add_argument("source_id", type=int)
    s.add_argument("rss_url")
    s.set_defaults(cmd="set-rss")

    s = sub.add_parser("enable-source", help="Enable a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="enable-source")

    s = sub.add_parser("disable-source", help="Disable a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="disable-source")

    sub.add_parser("list-sources", help="List sources").set_defaults(cmd="list-sources")

    s = sub.add_parser("add-relay", help="Add a relay (validated and de-duped)")
    s.add_argument("relay_url")
    s.set_defaults(cmd="add-relay")

    s = sub.add_parser("remove-relay", help="Remove a relay by id or URL")
    s.add_argument("relay_id_or_url")
    s.set_defaults(cmd="remove-relay")

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
    s.add_argument("--publish-interval-seconds", type=int, default=int(os.environ.get("PUBLISH_INTERVAL_SECONDS", "1")))
    s.add_argument("--retry-failed-after-seconds", type=int, default=int(os.environ.get("RETRY_FAILED_AFTER_SECONDS", "3600")))
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.set_defaults(cmd="run")

    s = sub.add_parser("interactive", help="Run with an interactive CLI to manage sources/relays/nsec")
    s.add_argument("--nsec", default=None, help="nsec signing key (or set NOSTR_NSEC)")
    s.add_argument("--relays", default=None, help="Comma-separated relay URLs (overrides DB if provided)")
    s.add_argument("--poll-seconds", type=int, default=int(os.environ.get("POLL_SECONDS", "300")))
    s.add_argument("--publish-interval-seconds", type=int, default=int(os.environ.get("PUBLISH_INTERVAL_SECONDS", "1")))
    s.add_argument("--retry-failed-after-seconds", type=int, default=int(os.environ.get("RETRY_FAILED_AFTER_SECONDS", "3600")))
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.set_defaults(cmd="interactive")

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

        if args.cmd == "enable-source":
            c = store.set_source_enabled(args.source_id, True)
            print(f"Enabled: {c}")
            return

        if args.cmd == "disable-source":
            c = store.set_source_enabled(args.source_id, False)
            print(f"Disabled: {c}")
            return

        if args.cmd == "list-sources":
            rows = store.list_sources()
            if not rows:
                print("No sources.")
                return
            print("id\tenabled\tapi_base\tapi_channel\trss_url\tlast_polled\tlast_error")
            for (sid, enabled, api_base, api_channel, api_channel_url, rss_url, last_polled_ts, last_error) in rows:
                lp = str(last_polled_ts) if last_polled_ts else "-"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                api = f"{api_base or ''} {api_channel or ''}".strip()
                print(f"{sid}\t{enabled}\t{api}\t{rss_url or ''}\t{lp}\t{le}")
            return

        if args.cmd == "add-relay":
            rid = store.add_relay(args.relay_url)
            print(f"Added relay id={rid}")
            return

        if args.cmd == "remove-relay":
            c = store.remove_relay(args.relay_id_or_url)
            print(f"Removed: {c}")
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
                runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n)
                runner.run(
                    nsec=nsec_env,
                    relays=relays,
                    poll_seconds=args.poll_seconds,
                    publish_interval_seconds=args.publish_interval_seconds,
                    retry_failed_after_seconds=retry,
                    api_limit_per_source=args.api_limit_per_source,
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

            cmd = parts[0]
            current = parts[-1]

            if cmd in ("enable-relay", "disable-relay", "remove-relay"):
                if len(parts) <= 2:
                    for val in _relay_tokens():
                        if val.startswith(current):
                            yield Completion(val, start_position=-len(current))
                return

            if cmd in ("enable-source", "disable-source", "set-rss"):
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
    _emit_help(_log)

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
            cmd = parts[0].lower()
            args = parts[1:]

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
        "help", "status", "init",
        "list-relays", "add-relay", "remove-relay", "enable-relay", "disable-relay",
        "list-sources", "add-channel", "add-rss", "set-rss", "enable-source", "disable-source",
        "set-nsec", "clear-nsec",
        "quit", "exit",
    ]


def _emit_help(log_fn) -> None:
    for line in _help_lines():
        log_fn(line)


def _help_lines() -> list[str]:
    return [
        "Commands:",
        "  help | / | ?                     Show this help",
        "  status                            Show counts + nsec status",
        "  init                              Init DB + seed relays (if empty)",
        "  list-relays                       List relays",
        "  add-relay <url>                   Add relay",
        "  remove-relay <id|url>             Remove relay",
        "  enable-relay <id|url>             Enable relay",
        "  disable-relay <id|url>            Disable relay",
        "  list-sources                      List sources",
        "  add-channel <url>                 Add PeerTube channel",
        "  add-rss <url>                     Add RSS-only source",
        "  set-rss <id> <url>                Set RSS fallback",
        "  enable-source <id>                Enable source",
        "  disable-source <id>               Disable source",
        "  set-nsec [nsec]                   Store nsec (prompt if omitted)",
        "  clear-nsec                        Remove stored nsec",
        "  quit | exit                       Stop",
    ]


def _dispatch_command(store: Store, n: UrlNormaliser, db_path: str, cmd: str, args: list[str], log_fn) -> bool:
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
    if cmd == "remove-relay" and len(args) == 1:
        c = store.remove_relay(args[0])
        log_fn(f"Removed: {c}")
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
            log_fn("id\tenabled\tapi_base\tapi_channel\trss_url\tlast_polled\tlast_error")
            for (sid, enabled, api_base, api_channel, api_channel_url, rss_url, last_polled_ts, last_error) in rows:
                lp = str(last_polled_ts) if last_polled_ts else "-"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                api = f"{api_base or ''} {api_channel or ''}".strip()
                log_fn(f"{sid}\t{enabled}\t{api}\t{rss_url or ''}\t{lp}\t{le}")
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
    if cmd == "enable-source" and len(args) == 1:
        c = store.set_source_enabled(int(args[0]), True)
        log_fn(f"Enabled: {c}")
        return False
    if cmd == "disable-source" and len(args) == 1:
        c = store.set_source_enabled(int(args[0]), False)
        log_fn(f"Disabled: {c}")
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
    has_nsec = bool(get_stored_nsec(db_path))
    nsec_txt = "nsec:yes" if has_nsec else "nsec:no"
    return f" relays:{relays} sources:{sources} pending:{pending} {nsec_txt} "


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

    def _runner_thread() -> None:
        thread_store = Store(args.db, n)
        thread_store.init_schema()
        try:
            thread_runner = Runner(thread_store, PeerTubeClient(n), NostrPublisher(), n, log_fn=_log_fn if log_queue else None)
            thread_runner.run(
                nsec=nsec_env,
                relays=relays,
                poll_seconds=args.poll_seconds,
                publish_interval_seconds=args.publish_interval_seconds,
                retry_failed_after_seconds=retry,
                api_limit_per_source=args.api_limit_per_source,
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
        #status { height: 1; }
        #input { height: 3; }
        """
        BINDINGS = [
            ("/", "help", "Help"),
            ("?", "help", "Help"),
            ("ctrl+l", "clear", "Clear"),
            ("ctrl+c", "quit", "Quit"),
        ]

        def __init__(self) -> None:
            super().__init__()
            self.store = Store(db_path, n)
            self.store.init_schema()
            self._wizard_queue: list[tuple[str, callable, bool]] = []
            self._wizard_active = False
            self._pending_secret = False

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            with Vertical(id="body"):
                yield RichLog(id="log", wrap=True, highlight=True, markup=False)
                yield Static(id="status")
                yield Input(id="input", placeholder="Type / for commands")
            yield Footer()

        def on_mount(self) -> None:
            self.set_interval(0.25, self._drain_logs)
            self.set_interval(1.0, self._refresh_status)
            self._log("== PeerTube2Nostr Interactive ==")
            self._log("Type '/' for commands. 'quit' to exit.")
            self._emit_help()
            self._start_wizard_if_needed()

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
            self.query_one("#status", Static).update(_status_toolbar(self.store, db_path))

        def action_help(self) -> None:
            self._emit_help()

        def action_clear(self) -> None:
            self.query_one("#log", RichLog).clear()

        def action_quit(self) -> None:
            stop_event.set()
            self.exit()

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
                parts = shlex.split(line)
                cmd = parts[0].lower()
                args = parts[1:]

                if cmd == "set-nsec" and not args:
                    self._pending_secret = True
                    event.input.password = True
                    event.input.placeholder = "Enter nsec:"
                    self._log("Enter nsec:")
                    return

            should_quit = _dispatch_command(self.store, n, db_path, cmd, args, self._log)
            if should_quit:
                self.action_quit()

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

    PeerTubeTUI().run()


if __name__ == "__main__":
    main()
