import re
import time
import calendar
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Tuple, Any
from urllib.parse import urlparse, urlunparse

class UrlNormaliser:
    ALLOWED_RELAY_SCHEMES = {"wss", "ws"}
    ALLOWED_HTTP_SCHEMES = {"https", "http"}

    def __init__(self) -> None:
        self._watch_patterns = [
            re.compile(r"/videos/watch/([A-Za-z0-9_-]+)"),
            re.compile(r"/w/([A-Za-z0-9_-]+)"),
        ]
        self._channel_patterns = [
            re.compile(r"^/c/([^/]+)$"),
            re.compile(r"^/c/([^/]+)/videos$"),
            re.compile(r"^/video-channels/([^/]+)$"),
            re.compile(r"^/video-channels/([^/]+)/videos$"),
            re.compile(r"^/accounts/([^/]+)$"),
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
        u = self.normalise_http_url(channel_url)
        p = urlparse(u)
        path = p.path.rstrip("/")
        if path.endswith("/videos"):
            path = path[: -len("/videos")]
        for pat in self._channel_patterns:
            m = pat.match(path)
            if m:
                return f"{p.scheme}://{p.netloc}", m.group(1)
        seg = path.strip("/").split("/")[-1] if path.strip("/") else ""
        if not seg:
            raise ValueError("Could not extract channel handle from URL")
        return f"{p.scheme}://{p.netloc}", seg

def parse_any_timestamp(value: Any) -> Optional[int]:
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
