from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from core.database import Store
from core.peertube import PeerTubeClient
from core.nostr import NostrPublisher
from core.runner import Runner
from core.utils import UrlNormaliser
from core.database import get_stored_nsec
from core.sync_state import StateSyncer
from auth import verify_api_key
from dependencies import get_store, get_normaliser, get_db_path

router = APIRouter(prefix="/api/sources", tags=["sources"], dependencies=[Depends(verify_api_key)])


def _sync_sources(store: Store, db_path: str) -> None:
    from routers.sync import make_syncer
    syncer = make_syncer(store, db_path)
    if syncer:
        try:
            syncer.sync_all()
        except Exception:
            pass


@router.get("")
def list_sources(store: Store = Depends(get_store)):
    rows = store.list_sources()
    result = []
    for (sid, enabled, api_base, api_channel, api_channel_url, rss_url,
         lookback_days, last_polled_ts, last_error) in rows:
        result.append({
            "id": sid,
            "enabled": bool(enabled),
            "api_base": api_base,
            "api_channel": api_channel,
            "api_channel_url": api_channel_url,
            "rss_url": rss_url,
            "lookback_days": lookback_days,
            "last_polled_ts": last_polled_ts,
            "last_error": last_error,
        })
    return {"sources": result}


@router.get("/{source_id}")
def get_source(source_id: int, store: Store = Depends(get_store)):
    s = store.get_source_by_id(source_id)
    if not s:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"source": s}


@router.post("")
def add_source(url: str, store: Store = Depends(get_store), n: UrlNormaliser = Depends(get_normaliser), db_path: str = Depends(get_db_path)):
    try:
        n.extract_channel_ref(url)
        sid = store.add_channel_source(url)
        _sync_sources(store, db_path)
        return {"id": sid, "type": "channel"}
    except Exception:
        pass
    try:
        rss_norm = n.normalise_feed_url(url)
        if n.looks_like_peertube_feed(rss_norm):
            sid = store.add_rss_source(url)
            return {"id": sid, "type": "rss"}
    except Exception:
        pass
    raise HTTPException(status_code=400, detail="URL does not look like a PeerTube channel or RSS feed")


@router.post("/{source_id}/channel")
def set_channel(source_id: int, channel_url: str, store: Store = Depends(get_store)):
    c = store.set_source_channel(source_id, channel_url)
    if not c:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}


@router.delete("/{source_id}/channel")
def clear_channel(source_id: int, store: Store = Depends(get_store)):
    c = store.clear_source_channel(source_id)
    if not c:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}


@router.post("/{source_id}/rss")
def set_rss(source_id: int, rss_url: str, store: Store = Depends(get_store)):
    c = store.set_source_rss(source_id, rss_url)
    if not c:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}


@router.delete("/{source_id}/rss")
def clear_rss(source_id: int, store: Store = Depends(get_store)):
    c = store.clear_source_rss(source_id)
    if not c:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}


@router.post("/{source_id}/enable")
def enable_source(source_id: int, store: Store = Depends(get_store), db_path: str = Depends(get_db_path)):
    store.set_source_enabled(source_id, True)
    _sync_sources(store, db_path)
    return {"ok": True}


@router.post("/{source_id}/disable")
def disable_source(source_id: int, store: Store = Depends(get_store), db_path: str = Depends(get_db_path)):
    store.set_source_enabled(source_id, False)
    _sync_sources(store, db_path)
    return {"ok": True}


@router.delete("/{source_id}")
def remove_source(source_id: int, store: Store = Depends(get_store), db_path: str = Depends(get_db_path)):
    c = store.remove_source(source_id)
    if not c:
        raise HTTPException(status_code=404, detail="Source not found")
    _sync_sources(store, db_path)
    return {"ok": True}


@router.post("/{source_id}/resync")
def resync_source(
    source_id: int,
    api_limit: int = 50,
    lookback_days: int = 30,
    store: Store = Depends(get_store),
    n: UrlNormaliser = Depends(get_normaliser),
):
    result = {"cleared": 0, "inserted": 0, "source_id": source_id}
    cleared = store.clear_pending_for_source(source_id)
    result["cleared"] = cleared
    runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n)
    runner.ingest_source_once(source_id, api_limit, lookback_days)
    s = store.get_source_by_id(source_id)
    if s:
        result["last_polled_ts"] = s.get("last_polled_ts")
    return result


@router.post("/{source_id}/retry")
def retry_source(source_id: int, store: Store = Depends(get_store)):
    count = store.retry_failed_for_source(source_id, older_than_seconds=0)
    return {"requeued": count}


@router.post("/{source_id}/lookback")
def set_lookback(source_id: int, days: int, store: Store = Depends(get_store)):
    c = store.set_source_lookback(source_id, days)
    if not c:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}
