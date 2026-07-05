from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from core.database import Store, get_stored_nsec
from core.sync_state import StateSyncer
from auth import verify_api_key
from dependencies import get_store, get_db_path

router = APIRouter(prefix="/api/sync", tags=["sync"], dependencies=[Depends(verify_api_key)])


def _make_syncer(store: Store, db_path: str) -> Optional[StateSyncer]:
    nsec = get_stored_nsec(db_path)
    if not nsec:
        return None
    relays = store.get_enabled_relays()
    if not relays:
        return None
    return StateSyncer(store, nsec, relays)


@router.post("")
def sync(
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    syncer = _make_syncer(store, db_path)
    if not syncer:
        raise HTTPException(status_code=400, detail="nsec not configured or no relays available")
    eid = syncer.sync_all()
    if not eid:
        raise HTTPException(status_code=500, detail="Sync failed")
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
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    syncer = _make_syncer(store, db_path)
    if not syncer:
        raise HTTPException(status_code=400, detail="nsec not configured or no relays available")
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
