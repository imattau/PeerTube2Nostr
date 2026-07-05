import json
import time
from typing import Optional

from pynostr.event import Event
from pynostr.filters import Filters, FiltersList
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager

from core.database import Store
from core.utils import UrlNormaliser
from core.sync_state import unwrap_gift_wrap, unseal


def import_nip65_relays(
    nsec: str,
    store: Store,
    n: UrlNormaliser,
    bootstrap_relays: list[str],
    log_fn=print,
) -> int:
    priv = PrivateKey.from_nsec(nsec)
    pub_hex = priv.public_key.hex()
    priv_hex = priv.hex()

    rm = RelayManager(timeout=8)
    for r in bootstrap_relays:
        try:
            rm.add_relay(r)
        except Exception:
            pass

    # Query both plain kind 10002 and encrypted gift wraps (kind 1059)
    filters = FiltersList([
        Filters(authors=[pub_hex], kinds=[0, 10002]),
        Filters(kinds=[1059], pubkey_refs=[pub_hex], limit=10),
    ])
    try:
        rm.add_subscription_on_all_relays("pt2n-sync", filters)
    except Exception:
        pass

    rm.run_sync()

    relays_ev = None
    mp = getattr(rm, "message_pool", None)
    if mp is not None:
        while mp.has_events():
            msg = mp.get_event()
            ev = _extract_event_from_msg(msg)
            if ev is None:
                continue
            ev_kind = int(_event_get(ev, "kind") or 0)
            if ev_kind == 10002:
                relays_ev = ev
            elif ev_kind == 1059:
                # Try to unwrap gift wrap → find a kind 10002 rumor inside
                try:
                    ev_obj = _to_event(ev)
                    seal = unwrap_gift_wrap(ev_obj, priv_hex)
                    if seal is not None and int(getattr(seal, "kind", 0) or 0) == 13:
                        rumor = unseal(seal, priv_hex)
                        if rumor is not None and int(getattr(rumor, "kind", 0) or 0) == 10002:
                            relays_ev = rumor
                except Exception:
                    pass

    try:
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


def _to_event(raw) -> Optional[Event]:
    if isinstance(raw, Event):
        return raw
    if isinstance(raw, dict):
        return Event.from_dict(raw)
    d = getattr(raw, "to_dict", None)
    if d:
        return Event.from_dict(d())
    return None


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
