from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Body

from core.database import Store, get_stored_nsec, set_stored_nsec, clear_stored_nsec
from auth import verify_api_key
from dependencies import get_store, get_db_path

router = APIRouter(prefix="/api/settings", tags=["settings"], dependencies=[Depends(verify_api_key)])


@router.get("")
def get_settings(
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    min_interval, max_per_hour = store.get_publish_limits()
    max_per_day_per_source = store.get_daily_source_limit()
    return {
        "min_publish_interval_seconds": min_interval,
        "max_posts_per_hour": max_per_hour,
        "max_posts_per_day_per_source": max_per_day_per_source,
        "has_nsec": bool(get_stored_nsec(db_path)),
    }


@router.put("")
def update_settings(
    min_publish_interval_seconds: Optional[int] = None,
    max_posts_per_hour: Optional[int] = None,
    max_posts_per_day_per_source: Optional[int] = None,
    store: Store = Depends(get_store),
):
    if min_publish_interval_seconds is not None:
        store.set_setting("min_publish_interval_seconds", str(min_publish_interval_seconds))
    if max_posts_per_hour is not None:
        store.set_setting("max_posts_per_hour", str(max_posts_per_hour))
    if max_posts_per_day_per_source is not None:
        store.set_setting("max_posts_per_day_per_source", str(max_posts_per_day_per_source))
    min_interval, max_per_hour = store.get_publish_limits()
    max_per_day = store.get_daily_source_limit()
    return {
        "min_publish_interval_seconds": min_interval,
        "max_posts_per_hour": max_per_hour,
        "max_posts_per_day_per_source": max_per_day,
    }


@router.get("/nsec")
def get_nsec_status(
    db_path: str = Depends(get_db_path),
):
    nsec = get_stored_nsec(db_path)
    return {"configured": bool(nsec)}


@router.put("/nsec")
def set_nsec(
    nsec: str = Body(..., embed=True),
    db_path: str = Depends(get_db_path),
):
    if not nsec:
        raise HTTPException(status_code=400, detail="NSEC cannot be empty")
    store_type, path = set_stored_nsec(db_path, nsec)
    return {"stored_in": store_type, "path": path}


@router.delete("/nsec")
def delete_nsec(
    store: Store = Depends(get_store),
    db_path: str = Depends(get_db_path),
):
    removed = clear_stored_nsec(db_path)
    store.reset_all()
    return {"removed": removed}
