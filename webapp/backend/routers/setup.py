from fastapi import APIRouter, Depends

from core.database import Store
from auth import verify_api_key
from dependencies import get_store

router = APIRouter(prefix="/api/setup", tags=["setup"], dependencies=[Depends(verify_api_key)])


@router.get("/status")
def setup_status(store: Store = Depends(get_store)):
    complete = store.get_setting("setup_complete") in ("1", "true", True)
    relays = store.count_relays()
    sources = store.count_sources()
    return {
        "complete": complete,
        "relays": relays,
        "sources": sources,
        "needs_onboarding": not complete,
    }


@router.post("/complete")
def mark_complete(store: Store = Depends(get_store)):
    store.set_setting("setup_complete", "1")
    return {"ok": True}
