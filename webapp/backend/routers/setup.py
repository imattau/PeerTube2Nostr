from fastapi import APIRouter, Depends

from core.database import Store, get_stored_nsec
from auth import verify_api_key
from dependencies import get_store, get_db_path

router = APIRouter(prefix="/api/setup", tags=["setup"], dependencies=[Depends(verify_api_key)])


@router.get("/status")
def setup_status(store: Store = Depends(get_store), db_path: str = Depends(get_db_path)):
    complete = store.get_setting("setup_complete") in ("1", "true", True)
    if complete:
        has_nsec = bool(get_stored_nsec(db_path))
        if not has_nsec:
            signing_method = store.get_setting("signing_method")
            if signing_method not in ("nip07", "nip46"):
                complete = False
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
