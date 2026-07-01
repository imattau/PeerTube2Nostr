import threading
import time
import traceback
from datetime import datetime

import gi
gi.require_version('GLib', '2.0')
from gi.repository import GLib

from desktop.core import Store, Runner, RateLimiter, IngestPipeline
from desktop.core import DashboardMetrics
from desktop.core import PeerTubeClient, NostrPublisher, UrlNormaliser
from core.database import get_stored_nsec


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
            now_ts = int(time.time())
            rate = RateLimiter(store=store, now_ts=now_ts)
            n = UrlNormaliser()
            pt = PeerTubeClient(n, log_fn=self._log)
            pub = NostrPublisher()
            self._runner = Runner(
                store=store,
                pt=pt,
                pub=pub,
                n=n,
                log_fn=self._log,
                status_fn=None,
            )
        except Exception as e:
            self._log(f'Failed to initialise manager: {e}', level='ERROR')
            self._error_state = 'init_failed'

    def start(self):
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._log('Background runner started')

    def stop(self):
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._log('Background runner stopped')

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def error_state(self) -> str | None:
        return self._error_state

    def get_logs(self, max_lines: int = 100) -> list[str]:
        return self._log_buffer[-max_lines:]

    def _check_relays_health(self) -> None:
        from pynostr.filters import FiltersList, Filters
        relays = self._store.get_enabled_relays()
        for r in relays:
            start = time.time()
            try:
                from pynostr.relay_manager import RelayManager
                rm = RelayManager(timeout=5)
                rm.add_relay(r)
                sub_id = f"health-{int(time.time() * 1000)}"
                filters = FiltersList([Filters(limit=0)])
                rm.add_subscription_on_all_relays(sub_id, filters)
                rm.run_sync()
                latency = int((time.time() - start) * 1000)
                mp = rm.message_pool
                if mp.has_eose_notices() or mp.has_events() or mp.has_notices():
                    self._store.update_relay_latency(r, latency)
                else:
                    self._store.mark_relay_used(r, "No response from relay")
                try:
                    rm.close_connections()
                except Exception:
                    pass
            except Exception as e:
                self._store.mark_relay_used(r, str(e))

    def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                self._check_relays_health()

                if self._runner:
                    self._runner.ingest_sources_once(api_limit=50, lookback_days=30)
                    nsec = get_stored_nsec(self._store.db_path)
                    relays = self._store.get_enabled_relays()
                    if nsec and relays:
                        self._runner.publish_one_pending(nsec=nsec, relays=relays)

                metrics_dict = {
                    'pending': self._store.count_pending(),
                    'posted_today': self._store.count_posted(),
                    'failed': self._store.count_failed(),
                    'active_sources': self._store.count_sources(),
                }

                queue_count = self._store.count_pending()
                self._schedule_update({
                    'metrics': metrics_dict,
                    'queue_count': queue_count,
                })
            except Exception as e:
                self._log(f'Runner error: {e}', level='ERROR')
                traceback.print_exc()

            for _ in range(300):
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
        self._log(f'Published "{video_title}"')

    def _on_error(self, message: str):
        self._log(message, level='ERROR')

    def _log(self, message: str, level: str = 'INFO'):
        ts = datetime.now().strftime('%H:%M:%S')
        entry = f'{ts}  {level:8}  {message}'
        self._log_buffer.append(entry)
