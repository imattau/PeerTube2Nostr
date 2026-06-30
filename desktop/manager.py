import threading
import time
import traceback
from datetime import datetime

import gi
gi.require_version('GLib', '2.0')
from gi.repository import GLib

from desktop.core import Store, Runner, RateLimiter, IngestPipeline
from desktop.core.models import DashboardMetrics


class DesktopAppManager:
    def __init__(self, store: Store, on_update=None):
        self._store = store
        self._on_update = on_update
        self._runner = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._running = False
        self._log_buffer: list[str] = []
        self._error_state: str | None = None

        try:
            rate = RateLimiter(
                min_interval=int(
                    store.get_setting('publish_min_interval') or '20'
                ),
                hourly_cap=int(
                    store.get_setting('publish_hourly_cap') or '3'
                ),
                daily_source_cap=int(
                    store.get_setting('publish_daily_source_limit') or '1'
                ),
            )
            self._runner = Runner(
                store=store,
                rate_limiter=rate,
                publish_callback=self._on_publish,
                error_callback=self._on_error,
            )
        except Exception as e:
            self._log('ERROR', f'Failed to initialise manager: {e}')
            self._error_state = 'init_failed'

    def start(self):
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._log('INFO', 'Background runner started')

    def stop(self):
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._log('INFO', 'Background runner stopped')

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def error_state(self) -> str | None:
        return self._error_state

    def get_logs(self, max_lines: int = 100) -> list[str]:
        return self._log_buffer[-max_lines:]

    def _check_relays_health(self) -> None:
        relays = self._store.get_enabled_relays()
        for r in relays:
            start = time.time()
            try:
                from pynostr.relay_manager import RelayManager
                rm = RelayManager(timeout=5)
                rm.add_relay(r)
                rm.open_connections()
                latency = int((time.time() - start) * 1000)
                try:
                    rm.close_connections()
                except Exception:
                    pass
                self._store.update_relay_latency(r, latency)
            except Exception as e:
                self._store.mark_relay_used(r, str(e))

    def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                self._check_relays_health()

                if not self._runner:
                    time.sleep(5)
                    continue

                self._runner.ingest_sources_once()
                self._runner.publish_one_pending()

                metrics = self._store.get_metrics()
                if isinstance(metrics, dict):
                    metrics_dict = metrics
                else:
                    metrics_dict = {
                        'pending': getattr(metrics, 'pending', 0),
                        'posted_today': getattr(metrics, 'posted_today', 0),
                        'failed': getattr(metrics, 'failed', 0),
                        'active_sources': getattr(metrics, 'active_sources', 0),
                    }

                queue_count = self._store.count_pending()
                self._schedule_update({
                    'metrics': metrics_dict,
                    'queue_count': queue_count,
                })
            except Exception as e:
                self._log('ERROR', f'Runner error: {e}')
                traceback.print_exc()

            for _ in range(60):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

    def _schedule_update(self, data: dict):
        GLib.idle_add(self._do_update, data)

    def _do_update(self, data: dict):
        if self._on_update:
            self._on_update(data)
        return False

    def _on_publish(self, video_title: str):
        self._log('INFO', f'Published "{video_title}"')

    def _on_error(self, message: str):
        self._log('ERROR', message)

    def _log(self, level: str, message: str):
        ts = datetime.now().strftime('%H:%M:%S')
        entry = f'{ts}  {level:8}  {message}'
        self._log_buffer.append(entry)
