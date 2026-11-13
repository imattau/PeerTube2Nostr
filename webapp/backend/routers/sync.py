from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from core.database import Store, get_stored_nsec
from core.relays import RelaySelectionError, resolve_publish_relays
from core.sync_state import StateSyncer
from auth import verify_api_key
from dependencies import get_store, get_db_path

router = APIRouter(prefix="/api/sync", tags=["sync"], dependencies=[Depends(verify_api_key)])


def make_syncer(
    store: Store,
    db_path: str,
    relays: Optional[list[str]] = None,
) -> Optional[StateSyncer]:
    nsec = get_stored_nsec(db_path)
    if not nsec:
        return None
    try:
        relay_list = resolve_publish_relays(store, relays)
    except RelaySelectionError:
        return None
    if not relay_list:
        return None
    return StateSyncer(store, nsec, relay_list)


def _make_syncer(store, db_path, relays=None):
    return make_syncer(store, db_path, relays)


@router.post("")
def sync(
    relays: Optional[list[str]] = Query(default=None),
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    syncer = _make_syncer(store, db_path, relays)
    if not syncer:
        return {"event_id": ""}
    eid = syncer.sync_all()
    if not eid:
        return {"event_id": ""}
    return {"event_id": eid}


@router.get("/status")
def sync_status(
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    syncer = _make_syncer(store, db_path)
    if not syncer:
        return {"available": False, "nsec": False, "relays": False}
    return {"available": True, "pubkey": syncer._pub_hex, "relay_count": len(syncer.relays)}


@router.get("/restore")
def restore(
    relays: Optional[list[str]] = Query(default=None),
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    syncer = _make_syncer(store, db_path, relays)
    if not syncer:
        return {"found": False}
    data = syncer.fetch_state()
    if data is None:
        return {"found": False}
    return {
        "found": True,
        "version": data.get("version"),
        "ts": data.get("ts"),
        "video_count": len(data.get("videos", [])),
        "source_count": len(data.get("sources", [])),
    }
