from fastapi import APIRouter, Depends

from core.database import Store, get_stored_nsec
from auth import get_api_key, verify_api_key
from dependencies import get_store, get_db_path

router = APIRouter(prefix="/api/setup", tags=["setup"], dependencies=[Depends(verify_api_key)])


@router.get("/status")
def setup_status(store: Store = Depends(get_store), db_path: str = Depends(get_db_path)):
    complete = store.get_setting("setup_complete") in ("1", "true", True)
    # One-time cleanup: the old auto-completion code may have set
    # setup_complete without the user ever seeing the wizard.
    if complete and store.get_setting("_setup_cleaned") != "1":
        has_nsec = bool(get_stored_nsec(db_path))
        api_key = bool(get_api_key())
        if not has_nsec and not api_key:
            store.set_setting("setup_complete", "0")
            complete = False
        store.set_setting("_setup_cleaned", "1")
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
