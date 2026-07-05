from typing import Optional, List

from pynostr.event import Event
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager


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


class NostrPublisher:
    @staticmethod
    def _build_content(p: dict) -> str:
        return (p.get("summary") or "").strip()

    @staticmethod
    def _build_tags(p: dict) -> tuple:
        title = (p.get("title") or "").strip()
        author = (p.get("channel_name") or p.get("account_name") or "unknown").strip()
        mp4 = p.get("direct_url")
        hls = p.get("hls_url")
        thumb = p.get("thumbnail_url")
        duration = p.get("duration")
        w = p.get("width")
        h = p.get("height")
        watch_url = p.get("watch_url")
        channel_url = p.get("channel_url")
        published_ts = p.get("published_ts")
        peertube_instance = p.get("peertube_instance")
        peertube_video_id = p.get("peertube_video_id")

        kind = 22 if (h and w and h > w) else 21

        tags: list[list[str]] = []

        if title:
            tags.append(["title", title])
        if published_ts:
            tags.append(["published_at", str(published_ts)])

        def add_imeta(url: str, mime: str) -> None:
            imeta = ["imeta", f"url {url}", f"m {mime}"]
            if thumb:
                imeta.append(f"image {thumb}")
            if duration:
                imeta.append(f"duration {duration}")
            if w and h:
                imeta.append(f"dim {w}x{h}")
            tags.append(imeta)

        if mp4:
            add_imeta(str(mp4), "video/mp4")
        if hls:
            add_imeta(str(hls), "application/x-mpegURL")

        tags.append(["t", "video"])
        tags.append(["t", "peertube"])
        if watch_url:
            tags.append(["r", str(watch_url)])
        if channel_url:
            tags.append(["r", str(channel_url)])
        if title and author:
            tags.append(["alt", f"PeerTube video: {title} by {author}"])
        if peertube_video_id and watch_url:
            tags.append(["origin", "peertube", str(peertube_video_id), str(watch_url)])
        if peertube_instance:
            tags.append(["peertube:instance", str(peertube_instance)])

        return kind, tags

    @staticmethod
    def publish(nsec: str, relays: list[str], content: str, kind: int, tags: list[list[str]]) -> str:
        priv = PrivateKey.from_nsec(nsec)
        pub_hex = priv.public_key.hex()
        try:
            ev = Event(kind=kind, public_key=pub_hex, content=content, tags=tags)
        except TypeError:
            try:
                ev = Event(kind=kind, pubkey=pub_hex, content=content, tags=tags)
            except TypeError:
                ev = Event(content=content, kind=kind, tags=tags)
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

        rm = RelayManager(timeout=15)
        for r in relays:
            rm.add_relay(r)
        rm.publish_event(ev)
        rm.run_sync()
        return ev.id
