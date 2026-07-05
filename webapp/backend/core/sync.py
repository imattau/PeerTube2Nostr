import json
import time
from typing import Optional

from pynostr.filters import Filters, FiltersList
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager

from core.database import Store
from core.utils import UrlNormaliser


def import_nip65_relays(
    nsec: str,
    store: Store,
    n: UrlNormaliser,
    bootstrap_relays: list[str],
    log_fn=print,
) -> int:
    priv = PrivateKey.from_nsec(nsec)
    pub = priv.public_key
    pub_hex = pub.hex()

    rm = RelayManager(timeout=8)
    for r in bootstrap_relays:
        try:
            rm.add_relay(r)
        except Exception:
            pass

    filters = FiltersList([Filters(authors=[pub_hex], kinds=[0, 10002])])

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
        pass

    try:
        if hasattr(rm, "open_connections"):
            rm.open_connections()
    except Exception:
        pass

    relays_ev = None
    start = time.time()
    mp = getattr(rm, "message_pool", None)
    while time.time() - start < 8:
        if mp is not None and hasattr(mp, "has_events") and hasattr(mp, "get_event"):
            while mp.has_events():
                msg = mp.get_event()
                ev = _extract_event_from_msg(msg)
                if ev is None:
                    continue
                kind = int(_event_get(ev, "kind") or 0)
                if kind == 10002:
                    relays_ev = ev
        elif hasattr(rm, "run_sync"):
            try:
                rm.run_sync()
            except Exception:
                break
        time.sleep(0.1)

    try:
        if hasattr(rm, "close_connections"):
            rm.close_connections()
    except Exception:
        pass

    if not relays_ev:
        return 0

    nip65 = _parse_nip65_relays(relays_ev)
    if not nip65:
        return 0

    imported = 0
    for r in nip65:
        try:
            norm = n.normalise_relay_url(r["url"])
        except Exception:
            continue
        try:
            store.add_relay_with_enabled(norm, enabled=False)
            imported += 1
        except Exception:
            pass

    if imported:
        log_fn(f"Imported {imported} NIP-65 relays")

    return imported


def _event_get(ev, key: str):
    if isinstance(ev, dict):
        return ev.get(key)
    return getattr(ev, key, None)


def _extract_event_from_msg(msg):
    if msg is None:
        return None
    if isinstance(msg, dict) and "event" in msg:
        return msg.get("event")
    ev = getattr(msg, "event", None)
    if ev is not None:
        return ev
    return msg


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
