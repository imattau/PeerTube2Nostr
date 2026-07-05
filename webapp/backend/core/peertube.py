from typing import Optional, List, Dict, Any, Tuple, Callable

from core.database import Store, IngestedItem
from core.utils import UrlNormaliser


class PeerTubeClient:
    def __init__(self, n: UrlNormaliser, log_fn: Optional[Callable] = None) -> None:
        self.n = n
        self.log_fn = log_fn
        import requests
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "peertube-nostr-publisher/0.1"})

    def _log(self, msg: str) -> None:
        if self.log_fn:
            self.log_fn(msg)

    def _get_json(self, url: str, params: Optional[dict] = None, timeout: int = 15) -> Optional[dict]:
        try:
            r = self.session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as ex:
            self._log(f"HTTP error fetching {url}: {ex}")
            return None

    def list_channel_videos(self, api_base: str, channel: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        base = self.n.normalise_http_url(api_base).rstrip("/")
        url = f"{base}/api/v1/video-channels/{channel}/videos"

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
        import feedparser
        d = feedparser.parse(rss_url)
        entries = d.entries or []
        return list(reversed(entries))

    def enrich_video(self, watch_url: str) -> tuple:
        x = self.n.extract_watch_id(watch_url)
        if not x:
            return (None,) * 15

        base, vid = x
        v = self._get_json(f"{base}/api/v1/videos/{vid}")
        if not isinstance(v, dict):
            return (base, vid) + (None,) * 13

        hls = self._pick_hls_url(v)
        mp4, w, h = self._pick_best_mp4(v)
        duration = v.get("duration")
        instance, channel_name, channel_url, account_name, account_url = self._extract_attribution(base, v)
        api_title = (v.get("name") or "").strip() or None
        api_desc = (v.get("description") or "").strip() or None
        thumb = v.get("thumbnailPath")
        if thumb and not thumb.startswith("http"):
            thumb = f"{base}{thumb}"
        return base, vid, mp4, hls, duration, w, h, instance, channel_name, channel_url, account_name, account_url, api_title, api_desc, thumb

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
    def _pick_best_mp4(v: dict) -> tuple:
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
            candidates.append((height, width, size, fu))

        for f in (v.get("files") or []):
            consider_file(f)
        for pl in (v.get("streamingPlaylists") or []):
            for f in (pl.get("files") or []):
                consider_file(f)

        if not candidates:
            return (None, None, None)
        with_height = [c for c in candidates if c[0] > 0]
        if with_height:
            under = [c for c in with_height if c[0] <= 720]
            if under:
                under.sort(reverse=True, key=lambda x: (x[0], x[2]))
                _, w, _, url = under[0]
                return (url, w, under[0][0])
            over = sorted(with_height, key=lambda x: (x[0], x[2]))
            return (over[0][3], over[0][1], over[0][0])
        candidates.sort(reverse=True, key=lambda x: x[2])
        return (candidates[0][3], candidates[0][1], candidates[0][0])

    @staticmethod
    def _extract_attribution(base: str, v: dict) -> Tuple:
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


class IngestPipeline:
    def __init__(self, store: Store, pt: PeerTubeClient, n: UrlNormaliser, log_fn: Callable) -> None:
        self.store = store
        self.pt = pt
        self.n = n
        self.log_fn = log_fn

    def ingest_entries(
        self,
        source_id: int,
        entries: list[dict],
        entry_key_fn: callable,
        watch_url_fn: callable,
        title_fn: callable,
        summary_fn: callable,
        published_ts_fn: callable,
        cutoff_ts: Optional[int],
        channel_url_fallback: Optional[str],
    ) -> tuple:
        inserted = 0
        skipped = 0
        for entry in entries:
            entry_key = str(entry_key_fn(entry) or "")
            if not entry_key:
                continue
            watch_url = watch_url_fn(entry)
            if not watch_url:
                continue
            if self.store.video_exists(source_id, entry_key):
                published_ts = published_ts_fn(entry)
                if published_ts:
                    self.store.update_published_ts_if_null(source_id, entry_key, published_ts)
                continue
            if cutoff_ts:
                published_ts = published_ts_fn(entry)
                if published_ts and published_ts < cutoff_ts:
                    skipped += 1
                    continue
            published_ts = published_ts_fn(entry)

            title = title_fn(entry)
            summary = summary_fn(entry)

            base, v_api_id, mp4, hls, dur, w, h, instance, ch_name, ch_url, acc_name, acc_url, api_title, api_desc, thumb = self.pt.enrich_video(watch_url)
            if api_title:
                title = api_title
            if api_desc:
                summary = api_desc

            item = IngestedItem(
                source_id=source_id,
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
                channel_url=ch_url or channel_url_fallback,
                account_name=acc_name,
                account_url=acc_url,
                published_ts=published_ts,
                thumbnail_url=thumb,
                duration=dur,
                width=w,
                height=h,
            )
            self.store.insert_pending(item)
            inserted += 1
        return inserted, skipped
