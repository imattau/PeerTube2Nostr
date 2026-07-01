import time
from typing import Any, Optional

from pynostr.filters import Filters, FiltersList
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager

from .database import Store
from .utils import UrlNormaliser


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


def _parse_nip65_relays(ev) -> list[str]:
    tags = _event_get(ev, "tags") or []
    urls: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if not isinstance(tag, (list, tuple)) or len(tag) < 2:
            continue
        if tag[0] != "r":
            continue
        url = str(tag[1]).strip()
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def _fetch_kind10002(
    relays: list[str],
    pubkey_hex: str,
    timeout_seconds: int = 10,
) -> Optional[Any]:
    rm = RelayManager(timeout=timeout_seconds)
    for r in relays:
        try:
            rm.add_relay(r)
        except Exception:
            pass

    filters = FiltersList([Filters(authors=[pubkey_hex], kinds=[10002])])

    try:
        if hasattr(rm, "add_subscription"):
            sub_id = f"nip65-sync-{int(time.time() * 1000)}"
            sub = rm.add_subscription(sub_id)
            if hasattr(sub, "add_filters"):
                sub.add_filters(filters)
            elif hasattr(sub, "set_filters"):
                sub.set_filters(filters)
        elif hasattr(rm, "add_subscription_on_all_relays"):
            rm.add_subscription_on_all_relays("nip65-sync", filters)
    except Exception:
        pass

    try:
        if hasattr(rm, "open_connections"):
            rm.open_connections()
    except Exception:
        pass

    latest: Optional[Any] = None
    latest_ts = 0
    start = time.time()
    mp = getattr(rm, "message_pool", None)

    while time.time() - start < timeout_seconds:
        got_new = False
        if mp is not None and hasattr(mp, "has_events") and hasattr(mp, "get_event"):
            while mp.has_events():
                got_new = True
                msg = mp.get_event()
                ev = _extract_event_from_msg(msg)
                if ev is None:
                    continue
                created_at = int(_event_get(ev, "created_at") or 0)
                if created_at > latest_ts:
                    latest = ev
                    latest_ts = created_at
        elif hasattr(rm, "run_sync"):
            try:
                rm.run_sync()
            except Exception:
                break
        if not got_new:
            time.sleep(0.1)

    try:
        if hasattr(rm, "close_connections"):
            rm.close_connections()
    except Exception:
        pass

    return latest


def import_nip65_relays(
    nsec: str,
    store: Store,
    n: UrlNormaliser,
    bootstrap_relays: list[str],
    log_fn=print,
) -> int:
    priv = PrivateKey.from_nsec(nsec)
    pub_hex = priv.public_key.hex()

    if not bootstrap_relays:
        log_fn("No bootstrap relays available to fetch NIP-65 relay list.")
        return 0

    log_fn(f"Fetching NIP-65 relay list from {len(bootstrap_relays)} relay(s)...")
    event = _fetch_kind10002(bootstrap_relays, pub_hex, timeout_seconds=10)

    if event is None:
        log_fn("No NIP-65 relay list found.")
        return 0

    nip65_urls = _parse_nip65_relays(event)
    if not nip65_urls:
        log_fn("No relays in NIP-65 relay list.")
        return 0

    imported = 0
    errors = 0
    for url in nip65_urls:
        try:
            norm = n.normalise_relay_url(url)
            store.add_relay(norm, enabled=True)
            imported += 1
        except Exception:
            errors += 1

    log_fn(f"Imported {imported} relay(s) from Nostr profile.")
    if errors:
        log_fn(f"Import errors: {errors}")
    return imported
