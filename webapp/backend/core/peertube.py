import requests
import feedparser
from typing import Optional, List, Dict, Tuple, Any, Callable
from .utils import UrlNormaliser, parse_any_timestamp
from .models import IngestedItem
from .database import Store

class PeerTubeClient:
    def __init__(self, n: UrlNormaliser) -> None:
        self.n = n
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "peertube-nostr-publisher/0.1"})

    def _get_json(self, url: str, params: Optional[dict] = None) -> Optional[dict]:
        try:
            r = self.session.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def list_channel_videos(self, api_base: str, channel: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        base = self.n.normalise_http_url(api_base)
        url = f"{base}/api/v1/video-channels/{channel}/videos"
        params = {"start": 0, "count": min(limit, 100), "sort": "-publishedAt"}
        data = self._get_json(url, params=params)
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return data["data"]
        return None

    def parse_rss(self, rss_url: str) -> List[dict]:
        d = feedparser.parse(rss_url)
        return list(reversed(d.entries or []))

    def enrich_video(self, watch_url: str) -> Tuple[Optional[str], ...]:
        x = self.n.extract_watch_id(watch_url)
        if not x: return (None,) * 12
        base, vid = x
        v = self._get_json(f"{base}/api/v1/videos/{vid}")
        if not isinstance(v, dict): return (base, vid) + (None,) * 10
        
        hls = self._pick_hls_url(v)
        mp4 = self._pick_best_mp4_url(v)
        instance, ch_name, ch_url, acc_name, acc_url = self._extract_attribution(base, v)
        api_title = (v.get("name") or "").strip() or None
        api_desc = (v.get("description") or "").strip() or None
        thumb = v.get("thumbnailPath")
        if thumb and not thumb.startswith("http"):
            thumb = f"{base}{thumb}"
        return base, vid, mp4, hls, instance, ch_name, ch_url, acc_name, acc_url, api_title, api_desc, thumb

    def _pick_hls_url(self, v: dict) -> Optional[str]:
        for pl in (v.get("streamingPlaylists") or []):
            for key in ("playlistUrl", "hlsUrl", "url"):
                val = pl.get(key)
                if isinstance(val, str) and val.startswith("http") and val.endswith(".m3u8"): return val
        return None

    def _pick_best_mp4_url(self, v: dict) -> Optional[str]:
        candidates = []
        def consider(f):
            fu = f.get("fileUrl") or f.get("url")
            if isinstance(fu, str) and fu.startswith("http") and (".mp4" in fu.lower() or "mp4" in (f.get("mimeType") or "").lower()):
                res = f.get("resolution", {})
                candidates.append((int(res.get("height") or 0), int(f.get("size") or 0), fu))
        for f in (v.get("files") or []): consider(f)
        for pl in (v.get("streamingPlaylists") or []):
            for f in (pl.get("files") or []): consider(f)
        if not candidates: return None
        candidates.sort(key=lambda x: (x[0] <= 720, x[0], x[1]), reverse=True)
        return candidates[0][2]

    def _extract_attribution(self, base: str, v: dict) -> Tuple[Optional[str], ...]:
        ch = v.get("channel") or {}
        acc = v.get("account") or {}
        ch_name = ch.get("displayName") or ch.get("name") or None
        ch_url = ch.get("url") or (f"{base}/c/{ch.get('name')}" if ch.get("name") else None)
        acc_name = acc.get("displayName") or acc.get("name") or None
        acc_url = acc.get("url") or (f"{base}/a/{acc.get('name')}" if acc.get("name") else None)
        return base, ch_name, ch_url, acc_name, acc_url

class IngestPipeline:
    def __init__(self, store: Store, pt: PeerTubeClient, log_fn: Callable) -> None:
        self.store = store
        self.pt = pt
        self.log_fn = log_fn

    def ingest_entries(self, source_id: int, entries: list[dict], entry_key_fn: Callable, watch_url_fn: Callable, 
                       title_fn: Callable, summary_fn: Callable, published_ts_fn: Callable, cutoff_ts: Optional[int], 
                       channel_url_fallback: Optional[str]) -> Tuple[int, int]:
        inserted = skipped = 0
        for entry in entries:
            key = str(entry_key_fn(entry) or "")
            watch = watch_url_fn(entry)
            if not key or not watch: continue
            
            pub_ts = published_ts_fn(entry)
            
            base, vid, mp4, hls, inst, ch_n, ch_u, acc_n, acc_u, api_t, api_s, thumb = self.pt.enrich_video(watch)
            item = IngestedItem(
                source_id=source_id, entry_key=key, watch_url=watch, title=api_t or title_fn(entry), 
                summary=api_s or summary_fn(entry), peertube_base=base, peertube_video_id=vid, 
                hls_url=hls, mp4_url=mp4, peertube_instance=inst, channel_name=ch_n, 
                channel_url=ch_u or channel_url_fallback, account_name=acc_n, account_url=acc_u, 
                published_ts=pub_ts, thumbnail_url=thumb
            )
            self.store.insert_pending(item)
            inserted += 1
        return inserted, skipped
