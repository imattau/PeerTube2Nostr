import os
import sys

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_backend_dir = os.path.join(_project_root, 'webapp', 'backend')
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware

from auth import verify_api_key, get_api_key
from routers import sources, relays, queue, metrics, settings, setup, sync
from dependencies import get_store, get_db_path
from ws import ws_router, log_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_key = get_api_key()
    if api_key:
        print(f"API key authentication enabled")
    else:
        print("No API key set — all endpoints are open. Set PEERTUBE2NOSTR_API_KEY to enable auth.")
    store = None
    try:
        db_path = get_db_path()
        from core.database import Store
        from core.utils import UrlNormaliser
        store = Store(db_path, UrlNormaliser())
        store.init_schema()
        store.seed_default_relays_if_empty()
        print(f"Database: {db_path}")
        print(f"  Sources: {store.count_sources()}")
        print(f"  Relays:  {store.count_relays()}")
        print(f"  Pending: {store.count_pending()}")
        print(f"  Posted:  {store.count_posted()}")
        print(f"  Failed:  {store.count_failed()}")
    except Exception as e:
        print(f"Warning: could not open database: {e}")
    finally:
        if store:
            store.close()
    yield


app = FastAPI(
    title="PeerTube2Nostr API",
    description="REST API for PeerTube2Nostr publisher",
    version="0.1.4",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sources.router)
app.include_router(relays.router)
app.include_router(queue.router)
app.include_router(metrics.router)
app.include_router(settings.router)
app.include_router(setup.router)
app.include_router(sync.router)
app.include_router(ws_router)


@app.get("/")
def root():
    return {
        "name": "PeerTube2Nostr API",
        "version": "0.1.4",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }
