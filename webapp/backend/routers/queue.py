import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Body

from core.database import Store
from core.nostr import NostrPublisher
from core.runner import PendingSelector
from core.sync_state import StateSyncer
from core.database import get_stored_nsec
from auth import verify_api_key
from dependencies import get_store, get_db_path

router = APIRouter(prefix="/api/queue", tags=["queue"], dependencies=[Depends(verify_api_key)])


def _sync_state(store: Store, db_path: str) -> None:
    from routers.sync import make_syncer
    syncer = make_syncer(store, db_path)
    if syncer:
        try:
            syncer.sync_all()
        except Exception:
            pass


@router.get("")
def list_queue(
    status: str = Query("pending", pattern="^(pending|posted|failed|cancelled)$"),
    limit: int = Query(200, ge=1, le=1000),
    store: Store = Depends(get_store),
):
    videos = store.list_videos(status=status, limit=limit)
    return {"videos": videos, "count": len(videos)}


@router.get("/pending")
def list_pending(
    limit: int = Query(200, ge=1, le=1000),
    store: Store = Depends(get_store),
):
    videos = store.list_videos(status="pending", limit=limit)
    return {"videos": videos, "count": len(videos)}


@router.get("/next")
def next_pending(store: Store = Depends(get_store)):
    from core.runner import RateLimiter
    import time
    now_ts = int(time.time())
    selector = PendingSelector(store)
    pending = selector.next_eligible(now_ts)
    if not pending:
        return {"video": None, "eligible": False}
    rate = RateLimiter(store, now_ts)
    wait = rate.next_wait(int(pending["source_id"]))
    return {"video": pending, "wait_seconds": wait, "eligible": wait == 0}


@router.get("/counts")
def queue_counts(store: Store = Depends(get_store)):
    return {
        "pending": store.count_pending(),
        "failed": store.count_failed(),
        "posted": store.count_posted(),
    }

@router.get("/{video_id}/event-data")
def get_event_data(video_id: int, store: Store = Depends(get_store)):
    video = store.get_video_by_id(video_id)
    if not video:
        raise HTTPException(404, "Video not found")
    if video["status"] != "pending":
        raise HTTPException(400, f"Video status is '{video['status']}', not pending")
    kind, tags = NostrPublisher._build_tags(video)
    content = NostrPublisher._build_content(video)
    relays = store.get_enabled_relays()
    author = (video.get("channel_name") or video.get("account_name") or "unknown").strip()
    return {"content": content, "kind": kind, "tags": tags, "relays": relays, "author": author}


@router.post("/publish-signed")
def publish_signed(
    video_id: int = Body(...),
    event_json: dict = Body(...),
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    video = store.get_video_by_id(video_id)
    if not video:
        raise HTTPException(404, "Video not found")
    if video["status"] != "pending":
        raise HTTPException(400, f"Video status is '{video['status']}', not pending")

    from pynostr.event import Event
    from pynostr.relay_manager import RelayManager

    ev = Event(
        kind=event_json.get("kind", 1),
        pubkey=event_json.get("pubkey", ""),
        content=event_json.get("content", ""),
        tags=event_json.get("tags", []),
        created_at=event_json.get("created_at", 0),
    )
    ev.id = event_json.get("id", "")
    ev.sig = event_json.get("sig", "")

    relays = store.get_enabled_relays()
    if not relays:
        raise HTTPException(400, "No relays configured")
    if not ev.sig or not ev.id:
        raise HTTPException(400, "Signed event must have id and sig")

    rm = RelayManager(timeout=6)
    for r in relays:
        rm.add_relay(r)
    rm.publish_event(ev)
    rm.run_sync()

    store.mark_posted(video_id, ev.id)
    for r in relays:
        store.mark_relay_used(r, None)

    from routers.sync import make_syncer
    syncer = make_syncer(store, db_path)
    if syncer:
        try:
            syncer.sync_all()
        except Exception:
            pass

    return {"ok": True, "event_id": ev.id}


@router.get("/metrics")
def queue_metrics(store: Store = Depends(get_store)):
    return store.get_metrics()


@router.post("/retry-failed")
def retry_failed(
    older_than_seconds: int = 0,
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    count = store.retry_failed(older_than_seconds)
    _sync_state(store, db_path)
    return {"requeued": count}
