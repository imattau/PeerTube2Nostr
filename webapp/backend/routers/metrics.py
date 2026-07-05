from fastapi import APIRouter, Depends

from core.database import Store
from core.models import DashboardMetrics
from auth import verify_api_key
from dependencies import get_store, get_db_path

router = APIRouter(prefix="/api", tags=["metrics"], dependencies=[Depends(verify_api_key)])


@router.get("/metrics")
def get_metrics(
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    metrics = DashboardMetrics.from_store(store, db_path)
    return {
        "relays": metrics.relays,
        "sources": metrics.sources,
        "pending": metrics.pending,
        "posted": metrics.posted,
        "failed": metrics.failed,
        "last_poll_ts": metrics.last_poll_ts,
        "last_posted_ts": metrics.last_posted_ts,
        "min_interval": metrics.min_interval,
        "max_per_hour": metrics.max_per_hour,
        "max_per_day_per_source": metrics.max_per_day_per_source,
        "has_nsec": metrics.has_nsec,
        "status": metrics.status,
        "now_ts": metrics.now_ts,
        "next_post": metrics.next_post,
        "poll_age": metrics.poll_age(),
        "post_age": metrics.post_age(),
    }


@router.get("/health")
def health_check():
    return {"status": "ok"}
