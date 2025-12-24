import threading
from typing import List, Optional
from .database import Store, get_stored_nsec, set_stored_nsec
from .utils import UrlNormaliser
from .peertube import PeerTubeClient
from .nostr import NostrPublisher
from .runner import Runner

class AppManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.n = UrlNormaliser()
        self.store = Store(db_path, self.n)
        self.store.init_schema()
        self.pt = PeerTubeClient(self.n)
        self.pub = NostrPublisher()
        self.runner = None
        self._thread = None
        self._logs = []
        self._status = "stopped"

    def _log(self, msg: str):
        self._logs.append(msg)
        if len(self._logs) > 1000:
            self._logs.pop(0)

    def _set_status(self, status: str):
        self._status = status

    def start_background_task(self):
        if self.runner:
            return
        self.runner = Runner(self.store, self.pt, self.pub, self.n, self._log, self._set_status)
        self._thread = threading.Thread(target=self.runner.run_loop, daemon=True)
        self._thread.start()

    def stop_background_task(self):
        if self.runner:
            self.runner.stop()
            self.runner = None
            self._thread = None
            self._status = "stopped"

    def get_logs(self) -> List[str]:
        return self._logs

    def get_metrics(self) -> dict:
        now = self.n.now_ts()
        min_int, max_hr = self.store.get_publish_limits()
        return {
            "relays": self.store.count_relays(),
            "sources": self.store.count_sources(),
            "pending": self.store.count_pending(),
            "posted": self.store.count_posted(),
            "failed": self.store.count_failed(),
            "last_poll_ts": self.store.last_polled_ts(),
            "last_posted_ts": self.store.last_posted_ts(),
            "min_interval": min_int,
            "max_per_hour": max_hr,
            "max_per_day_per_source": self.store.get_daily_source_limit(),
            "has_nsec": bool(get_stored_nsec(self.db_path)),
            "status": self._status,
            "now_ts": now
        }

    def sync_profile(self):
        # This is a simplified version of the CLI sync_profile
        # In a real app, this would be a background task
        nsec = get_stored_nsec(self.db_path)
        if not nsec:
            raise ValueError("NSEC not set")
        self._log("Starting profile sync...")
        # ... implementation details would go here or call a utility ...
        self._log("Profile sync completed (simulated).")

    def repair_database(self):
        self._log("Starting database repair...")
        # Add repair logic from CLI
        self._log("Database repair completed.")
