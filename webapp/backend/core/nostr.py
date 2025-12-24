from pynostr.event import Event
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager
from typing import List, Optional

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
        if title: lines.append(title)
        if channel_name: lines.append(f"By: {str(channel_name).strip()}")
        if channel_url: lines.append(f"Channel: {str(channel_url).strip()}")
        lines.append("")
        if mp4: lines.append(str(mp4).strip())
        if hls and hls != mp4: lines.append(str(hls).strip())
        if watch: lines.append(watch)
        if summary:
            lines.append("")
            lines.append(summary)
        return "\n".join(lines).strip()

    @staticmethod
    def _build_tags(p: dict) -> list[list[str]]:
        tags = [["t", "video"], ["t", "peertube"]]
        watch_url = p.get("watch_url")
        channel_url = p.get("channel_url")
        title = (p.get("title") or "").strip()
        author = (p.get("channel_name") or p.get("account_name") or "unknown").strip()
        mp4 = p.get("direct_url")
        hls = p.get("hls_url")

        if mp4:
            tags.extend([["url", str(mp4)], ["m", "video/mp4"]])
        elif hls:
            tags.extend([["url", str(hls)], ["m", "application/x-mpegURL"]])
        if watch_url: tags.append(["r", str(watch_url)])
        if channel_url: tags.append(["r", str(channel_url)])
        if title: tags.append(["alt", f"PeerTube video: {title} by {author}"])
        if p.get("peertube_instance"): tags.append(["peertube:instance", str(p["peertube_instance"])])
        return tags

    @staticmethod
    def publish(nsec: str, relays: List[str], content: str, tags: list[list[str]]) -> str:
        priv = PrivateKey.from_nsec(nsec)
        pub_hex = priv.public_key.hex()
        ev = Event(kind=1, content=content, tags=tags)
        # Handle different pynostr versions
        if hasattr(ev, "pubkey"): ev.pubkey = pub_hex
        elif hasattr(ev, "public_key"): ev.public_key = pub_hex
        
        if hasattr(priv, "sign_event"): priv.sign_event(ev)
        else: ev.sign(priv.hex())

        rm = RelayManager(timeout=6)
        for r in relays: rm.add_relay(r)
        rm.publish_event(ev)
        rm.run_sync()
        return ev.id
