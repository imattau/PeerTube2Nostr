from typing import Optional

from fastapi import APIRouter, Depends, Query

from core.database import Store
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
