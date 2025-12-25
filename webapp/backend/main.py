from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import os

from .core.manager import AppManager
from .core.database import set_stored_nsec

app = FastAPI(title="PeerTube2Nostr API")

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.environ.get("DB_PATH", "peertube_to_nostr.db")
manager = AppManager(DB_PATH)

async def verify_api_key(api_key: Optional[str] = Header(None, alias="X-API-Key")):
    if not api_key or api_key != manager.get_api_key():
        raise HTTPException(status_code=401, detail="Invalid API Key")

async def verify_setup_token(token: str, background_tasks: BackgroundTasks):
    if not manager.validate_setup_token(token):
        raise HTTPException(status_code=401, detail="Invalid or expired setup token")
    # Mark setup as complete after the request is finished
    background_tasks.add_task(manager.complete_setup)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    manager.start_background_task()

@app.get("/api/setup/status")
async def get_setup_status():
    is_complete = manager.is_setup_complete()
    token = None
    if not is_complete:
        token = manager.get_setup_token()
    return {
        "is_complete": is_complete,
        "setup_token": token
    }

@app.post("/api/setup/complete/{token}", status_code=200)
async def mark_setup_complete(background_tasks: BackgroundTasks, _: None = Depends(verify_setup_token)):
    return {"status": "Setup complete", "api_key": manager.get_api_key()}

@app.post("/api/security/regenerate-key", dependencies=[Depends(verify_api_key)])
async def regenerate_key():
    return {"api_key": manager.regenerate_api_key()}

@app.get("/api/metrics", dependencies=[Depends(verify_api_key)])
async def get_metrics():
    return manager.get_metrics()

@app.get("/api/queue", dependencies=[Depends(verify_api_key)])
async def get_queue():
    # Fetch top 20 pending videos
    cur = manager.store.conn.execute(
        "SELECT id, title, watch_url, thumbnail_url, channel_name FROM videos WHERE status='pending' ORDER BY first_seen_ts ASC LIMIT 20"
    )
    keys = ["id", "title", "watch_url", "thumbnail_url", "channel_name"]
    return [dict(zip(keys, row)) for row in cur.fetchall()]

@app.get("/api/logs", dependencies=[Depends(verify_api_key)])
async def get_logs():
    return {"logs": manager.get_logs()}

@app.get("/api/sources", dependencies=[Depends(verify_api_key)])
async def list_sources():
    return manager.store.list_sources()

class SourceAdd(BaseModel):
    url: str

@app.post("/api/sources", dependencies=[Depends(verify_api_key)])
async def add_source(data: SourceAdd):
    try:
        # Try as channel first
        return {"id": manager.store.add_channel_source(data.url)}
    except Exception:
        try:
            # Try as RSS
            return {"id": manager.store.add_rss_source(data.url)}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/sources/{source_id}", dependencies=[Depends(verify_api_key)])
async def delete_source(source_id: int):
    manager.store.remove_source(source_id)
    return {"status": "ok"}

@app.patch("/api/sources/{source_id}/toggle", dependencies=[Depends(verify_api_key)])
async def toggle_source(source_id: int, enabled: bool):
    manager.store.set_source_enabled(source_id, enabled)
    return {"status": "ok"}

@app.get("/api/relays", dependencies=[Depends(verify_api_key)])
async def list_relays():
    return manager.store.list_relays()

class RelayAdd(BaseModel):
    url: str

@app.post("/api/relays", dependencies=[Depends(verify_api_key)])
async def add_relay(data: RelayAdd):
    try:
        return {"id": manager.store.add_relay(data.url)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/relays/{relay_id}", dependencies=[Depends(verify_api_key)])
async def delete_relay(relay_id: int):
    manager.store.remove_relay(relay_id)
    return {"status": "ok"}

@app.patch("/api/relays/{relay_id}/toggle", dependencies=[Depends(verify_api_key)])
async def toggle_relay(relay_id: int, enabled: bool):
    manager.store.set_relay_enabled(relay_id, enabled)
    return {"status": "ok"}

class NsecAdd(BaseModel):
    nsec: str

@app.post("/api/nsec", dependencies=[Depends(verify_api_key)])
async def set_nsec(data: NsecAdd):
    if not data.nsec.startswith("nsec1"):
        raise HTTPException(status_code=400, detail="Invalid nsec format")
    set_stored_nsec(manager.db_path, data.nsec)
    return {"status": "ok"}

class SettingsUpdate(BaseModel):
    min_interval: Optional[int]
    max_per_hour: Optional[int]
    max_per_day_per_source: Optional[int]

@app.get("/api/settings", dependencies=[Depends(verify_api_key)])
async def get_settings():
    min_int, max_hr = manager.store.get_publish_limits()
    return {
        "min_interval": min_int,
        "max_per_hour": max_hr,
        "max_per_day_per_source": manager.store.get_daily_source_limit()
    }

@app.post("/api/settings", dependencies=[Depends(verify_api_key)])
async def update_settings(data: SettingsUpdate):
    if data.min_interval is not None:
        manager.store.set_setting("min_publish_interval_seconds", str(data.min_interval))
    if data.max_per_hour is not None:
        manager.store.set_setting("max_posts_per_hour", str(data.max_per_hour))
    if data.max_per_day_per_source is not None:
        manager.store.set_setting("max_posts_per_day_per_source", str(data.max_per_day_per_source))
    return {"status": "ok"}

@app.post("/api/actions/sync-profile", dependencies=[Depends(verify_api_key)])
async def sync_profile(background_tasks: BackgroundTasks):
    background_tasks.add_task(manager.sync_profile)
    return {"status": "processing"}

@app.post("/api/actions/repair-db", dependencies=[Depends(verify_api_key)])
async def repair_db(background_tasks: BackgroundTasks):
    background_tasks.add_task(manager.repair_database)
    return {"status": "processing"}

@app.post("/api/control/start", dependencies=[Depends(verify_api_key)])
async def start_task():
    manager.start_background_task()
    return {"status": "ok"}

@app.post("/api/control/stop", dependencies=[Depends(verify_api_key)])
async def stop_task():
    manager.stop_background_task()
    return {"status": "ok"}