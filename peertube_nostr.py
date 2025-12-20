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
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import urlparse, urlunparse

import feedparser
import requests
from pynostr.event import Event
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager


DEFAULT_RELAYS = ["wss://relay.damus.io", "wss://nos.lol"]


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
    def __init__(self, store: Store, pt: PeerTubeClient, pub: NostrPublisher, n: UrlNormaliser) -> None:
        self.store = store
        self.pt = pt
        self.pub = pub
        self.n = n

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
                        print(f"[source {sid}] API new items: {inserted}")
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
                        print(f"[source {sid}] RSS new items: {inserted}")
                except Exception as ex:
                    self.store.mark_source_polled(sid, f"{err + '; ' if err else ''}RSS failed: {ex}")
                    print(f"[source {sid}] RSS error: {ex}")
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
            print(f"Published {eid} | {pending.get('title') or pending.get('watch_url')}")
        except Exception as ex:
            self.store.mark_failed(pending["id"], str(ex))
            for r in relays:
                self.store.mark_relay_used(r, str(ex))
            print(f"Publish failed: {ex}")

    def run(self, nsec: str, relays: list[str], poll_seconds: int, publish_interval_seconds: int, retry_failed_after_seconds: Optional[int], api_limit_per_source: int) -> None:
        print(f"Relays: {', '.join(relays)}")
        print(f"Poll: {poll_seconds}s | Publish spacing: {publish_interval_seconds}s | API limit/source: {api_limit_per_source}")

        last_retry_check = 0

        while True:
            try:
                now = self.n.now_ts()
                if retry_failed_after_seconds is not None:
                    if last_retry_check == 0 or (now - last_retry_check) >= 60:
                        n = self.store.retry_failed(retry_failed_after_seconds)
                        if n:
                            print(f"Re-queued failed items for retry: {n}")
                        last_retry_check = now

                self.ingest_sources_once(api_limit_per_source=api_limit_per_source)

                # publish at most one per loop iteration
                self.publish_one_pending(nsec=nsec, relays=relays)

                time.sleep(publish_interval_seconds)
                time.sleep(poll_seconds)
            except KeyboardInterrupt:
                print("\nStopped.")
                return
            except Exception as ex:
                print(f"Loop error: {ex}")
                time.sleep(poll_seconds)


def parse_cli() -> argparse.Namespace:
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

    return p.parse_args()


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

        if args.cmd == "run":
            store.seed_default_relays_if_empty()

            nsec = os.environ.get("NOSTR_NSEC") or args.nsec
            if not nsec:
                raise SystemExit("Provide nsec via --nsec or NOSTR_NSEC env var.")

            relays_env = os.environ.get("NOSTR_RELAYS")
            relays_cli = args.relays

            if relays_env and relays_env.strip():
                relays = [n.normalise_relay_url(x.strip()) for x in relays_env.split(",") if x.strip()]
            elif relays_cli and relays_cli.strip():
                relays = [n.normalise_relay_url(x.strip()) for x in relays_cli.split(",") if x.strip()]
            else:
                relays = store.get_enabled_relays() or DEFAULT_RELAYS

            retry = args.retry_failed_after_seconds
            if retry == 0:
                retry = None

            runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n)
            runner.run(
                nsec=nsec,
                relays=relays,
                poll_seconds=args.poll_seconds,
                publish_interval_seconds=args.publish_interval_seconds,
                retry_failed_after_seconds=retry,
                api_limit_per_source=args.api_limit_per_source,
            )
            return

    finally:
        store.close()


if __name__ == "__main__":
    main()
