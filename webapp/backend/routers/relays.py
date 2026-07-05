import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from pynostr.filters import Filters, FiltersList
from pynostr.relay_manager import RelayManager

from core.database import Store, get_stored_nsec
from core.utils import UrlNormaliser, DEFAULT_RELAYS
from core.sync import import_nip65_relays
from auth import verify_api_key
from dependencies import get_store, get_normaliser

router = APIRouter(prefix="/api/relays", tags=["relays"], dependencies=[Depends(verify_api_key)])


@router.get("")
def list_relays(store: Store = Depends(get_store)):
    rows = store.list_relays()
    result = []
    for (rid, enabled, url, url_norm, last_used_ts, last_error, latency_ms) in rows:
        result.append({
            "id": rid,
            "enabled": bool(enabled),
            "relay_url": url,
            "relay_url_norm": url_norm,
            "last_used_ts": last_used_ts,
            "last_error": last_error,
            "latency_ms": latency_ms,
        })
    return {"relays": result}


@router.post("")
def add_relay(relay_url: str = Body(..., embed=True), store: Store = Depends(get_store)):
    try:
        rid = store.add_relay(relay_url)
        return {"id": rid, "relay_url": relay_url}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{relay_id}")
def update_relay(relay_id: int, relay_url: str, store: Store = Depends(get_store)):
    c = store.update_relay_url(str(relay_id), relay_url)
    if not c:
        raise HTTPException(status_code=404, detail="Relay not found")
    return {"ok": True}


@router.delete("/{relay_id}")
def remove_relay(relay_id: int, store: Store = Depends(get_store)):
    c = store.remove_relay(str(relay_id))
    if not c:
        raise HTTPException(status_code=404, detail="Relay not found")
    return {"ok": True}


@router.post("/{relay_id}/enable")
def enable_relay(relay_id: int, store: Store = Depends(get_store)):
    store.set_relay_enabled(str(relay_id), True)
    return {"ok": True}


@router.post("/{relay_id}/disable")
def disable_relay(relay_id: int, store: Store = Depends(get_store)):
    store.set_relay_enabled(str(relay_id), False)
    return {"ok": True}


@router.post("/check")
def check_relays(
    store: Store = Depends(get_store),
):
    relay_urls = store.get_enabled_relays()
    results: list[dict] = []
    for url in relay_urls:
        rm: Optional[RelayManager] = None
        try:
            rm = RelayManager(timeout=4)
            rm.add_relay(url)
            filters = FiltersList([Filters(limit=0)])
            rm.add_subscription_on_all_relays("health", filters)
            start = time.time()
            rm.run_sync()
            elapsed = int((time.time() - start) * 1000)
            mp = rm.message_pool
            healthy = mp is not None and (mp.has_eose_notices() or mp.has_events() or mp.has_notices())
            if healthy:
                store.update_relay_latency(url, elapsed)
                results.append({"relay_url": url, "latency_ms": elapsed, "error": None})
            else:
                store.mark_relay_used(url, "No response from relay")
                results.append({"relay_url": url, "latency_ms": None, "error": "No response"})
        except Exception as e:
            store.mark_relay_used(url, str(e))
            results.append({"relay_url": url, "latency_ms": None, "error": str(e)})
        finally:
            if rm is not None:
                try:
                    rm.close_connections()
                except Exception:
                    pass
    return {"results": results}


@router.post("/import-nip65")
def import_nip65(
    bootstrap_relays: list[str] = Query(default=[]),
    store: Store = Depends(get_store),
    n: UrlNormaliser = Depends(get_normaliser),
):
    nsec = get_stored_nsec(store.db_path)
    if not nsec:
        raise HTTPException(status_code=400, detail="No NSEC configured. Set one in settings first.")
    if not bootstrap_relays:
        bootstrap_relays = list(store.get_enabled_relays())
    # Always include well-known relays to find the list even if it's not
    # on the user's configured relays.
    seen = set()
    merged: list[str] = []
    for r in list(bootstrap_relays) + DEFAULT_RELAYS:
        if r not in seen:
            seen.add(r)
            merged.append(r)
    if not merged:
        raise HTTPException(status_code=400, detail="No bootstrap relays available. Add at least one relay first.")
    count = import_nip65_relays(nsec=nsec, store=store, n=n, bootstrap_relays=merged, log_fn=print)
    return {"imported": count}
