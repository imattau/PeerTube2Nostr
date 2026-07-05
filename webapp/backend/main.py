import os
import sys

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_backend_dir = os.path.join(_project_root, 'webapp', 'backend')
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware

from auth import verify_api_key, get_api_key
from routers import sources, relays, queue, metrics, settings, setup, sync
from dependencies import get_store, get_db_path
from ws import ws_router, log_manager

_stop_event: threading.Event | None = None
_runner_thread: threading.Thread | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _stop_event, _runner_thread

    api_key = get_api_key()
    if api_key:
        log_manager.log("API key authentication enabled", "INFO")
    else:
        log_manager.log("No API key set — all endpoints are open", "WARN")

    from core.database import Store
    from core.peertube import PeerTubeClient
    from core.nostr import NostrPublisher
    from core.runner import Runner, _set_runtime_status
    from core.utils import UrlNormaliser

    db_path = get_db_path()
    try:
        store = Store(db_path, UrlNormaliser())
        store.init_schema()
        store.seed_default_relays_if_empty()
        log_manager.log(f"Database: {db_path}", "INFO")
        log_manager.log(f"Sources: {store.count_sources()} | Relays: {store.count_relays()} | Pending: {store.count_pending()} | Posted: {store.count_posted()} | Failed: {store.count_failed()}", "INFO")
        store.close()
    except Exception as e:
        log_manager.log(f"Database error: {e}", "ERROR")

    try:
        n = UrlNormaliser()
        runner_store = Store(db_path, n)
        runner = Runner(
            runner_store, PeerTubeClient(n), NostrPublisher(), n,
            log_fn=lambda msg: log_manager.log(msg, "INFO"),
            status_fn=_set_runtime_status,
        )
        _stop_event = threading.Event()
        _runner_thread = threading.Thread(
            target=runner.run,
            args=(
                None,      # nsec — dynamic (fetches from keyring)
                None,      # relays — dynamic (fetches from DB)
                300,       # poll_seconds
                10,        # publish_interval_seconds
                3600,      # retry_failed_after_seconds
                50,        # api_limit_per_source
                30,        # new_source_lookback_days
                _stop_event,
            ),
            daemon=True,
        )
        _runner_thread.start()
        log_manager.log("Background publisher started", "INFO")
    except Exception as e:
        log_manager.log(f"Failed to start background publisher: {e}", "ERROR")

    yield

    log_manager.log("Server shutting down", "INFO")
    if _stop_event:
        _stop_event.set()
    if _runner_thread and _runner_thread.is_alive():
        _runner_thread.join(timeout=10)


app = FastAPI(
    title="PeerTube2Nostr API",
    description="REST API for PeerTube2Nostr publisher",
    version="0.1.7",
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
