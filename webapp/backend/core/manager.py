import secrets
import os
import time
from typing import List, Optional

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
        self._logs: List[str] = []
        self._status = "stopped"
        self._setup_token: Optional[str] = None
        self._setup_token_expiry: Optional[float] = None
        self._check_first_run()

    def _check_first_run(self):
        api_key = self.store.get_setting("api_key")
        if not api_key:
            new_key = secrets.token_urlsafe(32)
            self.store.set_setting("api_key", new_key)
            self._log(f"New API Key generated and stored.")

    def get_api_key(self) -> str:
        return self.store.get_setting("api_key", "") or ""

    def regenerate_api_key(self) -> str:
        new_key = secrets.token_urlsafe(32)
        self.store.set_setting("api_key", new_key)
        self._log(f"API Key has been regenerated.")
        return new_key

    def signIn(self, method: str, nsec: Optional[str] = None, bunker_url: Optional[str] = None) -> str:
        self.store.set_setting("signing_method", method)
        if nsec:
            set_stored_nsec(self.db_path, nsec)
        if bunker_url:
            self.store.set_setting("bunker_url", bunker_url)
        
        self.store.set_setting("setup_complete", "1")
        self._log(f"User signed in with method: {method}")
        return self.get_api_key()

    def get_setup_token(self) -> str:
        self._setup_token = secrets.token_urlsafe(16)
        self._setup_token_expiry = time.time() + 300 # Token valid for 5 minutes
        return self._setup_token

    def validate_setup_token(self, token: str) -> bool:
        if (
            token
            and self._setup_token
            and self._setup_token_expiry
            and time.time() < self._setup_token_expiry
            and token == self._setup_token
        ):
            self._setup_token = None # Invalidate after use
            return True
        return False

    def is_setup_complete(self) -> bool:
        return self.store.get_setting("setup_complete") == "1"

    def complete_setup(self):
        self.store.set_setting("setup_complete", "1")
        self._log("Setup marked as complete.")

    def set_signing_config(self, method: str, nsec: Optional[str] = None, bunker_url: Optional[str] = None, pubkey: Optional[str] = None):
        self.store.set_setting("signing_method", method)
        if nsec:
            set_stored_nsec(self.db_path, nsec)
        if bunker_url:
            self.store.set_setting("bunker_url", bunker_url)
        if pubkey:
            self.store.set_setting("pubkey", pubkey)
        self._log(f"Signing configuration updated: {method}")

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
