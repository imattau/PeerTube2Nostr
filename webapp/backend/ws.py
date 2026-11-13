import asyncio
import json
import time
from datetime import datetime
from typing import Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

ws_router = APIRouter()


class LogManager:
    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._buffer: list[str] = []
        self._max_buffer = 500
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Remember the server event loop so worker threads can broadcast."""
        self._loop = loop

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.add(ws)
        for msg in self._buffer[-100:]:
            await ws.send_text(msg)

    def disconnect(self, ws: WebSocket):
        self._connections.discard(ws)

    def log(self, message: str, level: str = "INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = json.dumps({"timestamp": ts, "level": level, "message": message})
        self._buffer.append(entry)
        if len(self._buffer) > self._max_buffer:
            self._buffer = self._buffer[-self._max_buffer:]
        try:
            asyncio.get_running_loop().create_task(self._broadcast(entry))
            return
        except RuntimeError:
            pass
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            asyncio.run_coroutine_threadsafe(self._broadcast(entry), loop)
        except RuntimeError:
            pass

    async def _broadcast(self, entry: str):
        stale = set()
        for ws in self._connections:
            try:
                await ws.send_text(entry)
            except Exception:
                stale.add(ws)
        self._connections -= stale


log_manager = LogManager()


@ws_router.websocket("/api/ws/logs")
async def websocket_logs(websocket: WebSocket):
    await log_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        log_manager.disconnect(websocket)
