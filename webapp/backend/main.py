import os
import sys

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_backend_dir = os.path.join(_project_root, 'webapp', 'backend')
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware

from auth import verify_api_key, get_api_key
from routers import sources, relays, queue, metrics, settings, setup, sync
from dependencies import get_store, get_db_path
from ws import ws_router, log_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_key = get_api_key()
    if api_key:
        log_manager.log("API key authentication enabled", "INFO")
    else:
        log_manager.log("No API key set — all endpoints are open", "WARN")
    store = None
    try:
        db_path = get_db_path()
        from core.database import Store
        from core.utils import UrlNormaliser
        store = Store(db_path, UrlNormaliser())
        store.init_schema()
        store.seed_default_relays_if_empty()
        log_manager.log(f"Database: {db_path}", "INFO")
        log_manager.log(f"Sources: {store.count_sources()} | Relays: {store.count_relays()} | Pending: {store.count_pending()} | Posted: {store.count_posted()} | Failed: {store.count_failed()}", "INFO")
    except Exception as e:
        log_manager.log(f"Database error: {e}", "ERROR")
    finally:
        if store:
            store.close()
    yield
    log_manager.log("Server shutting down", "INFO")


app = FastAPI(
    title="PeerTube2Nostr API",
    description="REST API for PeerTube2Nostr publisher",
    version="0.1.6",
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed = int((time.time() - start) * 1000)
    log_manager.log(f"{request.method} {request.url.path} -> {response.status_code} ({elapsed}ms)", "DEBUG")
    return response


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
        "version": "0.1.6",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }
