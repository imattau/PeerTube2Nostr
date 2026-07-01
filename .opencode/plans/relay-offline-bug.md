# Bug Fix: Desktop always showing relays as offline

## Root Cause

The `mark_relay_used()` method in `webapp/backend/core/database.py:230-240` **unconditionally** sets `latency_ms=NULL` in its SQL:

```python
"UPDATE relays SET last_used_ts=?, last_error=?, latency_ms=NULL WHERE relay_url_norm=? OR relay_url=?"
```

This gets called after every successful publish in `runner.py:136`:

```python
for r in relays: self.store.mark_relay_used(r, None)  # error=None but latency still wiped!
```

### Execution flow per loop iteration (desktop/manager.py:103-138):

1. `_check_relays_health()` → if relay connects, calls `update_relay_latency(r, latency)` → sets `latency_ms=150`
2. `publish_one_pending()` → on success, calls `mark_relay_used(r, None)` → **resets `latency_ms=NULL`**
3. UI reads `list_relays()` → sees `latency_ms=None` → renders "Offline"

The latency set by the health check is immediately wiped by the publish call. Relays always appear offline.

## Fix

### File: `webapp/backend/core/database.py`, method `mark_relay_used` (line 230-240)

Only set `latency_ms=NULL` when there is an actual error. When `error=None` (successful use), preserve the existing latency value:

```python
def mark_relay_used(self, relay_url: str, error: Optional[str]) -> None:
    ts = self.n.now_ts()
    try:
        norm = self.n.normalise_relay_url(relay_url)
    except Exception:
        norm = None
    if error:
        self.conn.execute(
            "UPDATE relays SET last_used_ts=?, last_error=?, latency_ms=NULL WHERE relay_url_norm=? OR relay_url=?",
            (ts, error[:1000], norm, relay_url),
        )
    else:
        self.conn.execute(
            "UPDATE relays SET last_used_ts=?, last_error=NULL WHERE relay_url_norm=? OR relay_url=?",
            (ts, norm, relay_url),
        )
    self.conn.commit()
```

### File: `peertube_nostr.py`, method `mark_relay_used` (line 583)

Same fix — this standalone script has an identical copy of `mark_relay_used`. Apply the same change.

## Testing

1. Run existing tests: `cd webapp/backend && python -m pytest tests/test_runner.py -v`
2. Add a new test in `tests/test_runner.py` (or a new test file) verifying that `publish_one_pending` on success does **not** call `mark_relay_used` with `latency_ms=NULL` (i.e., after a successful health check followed by publish, `latency_ms` is preserved)
