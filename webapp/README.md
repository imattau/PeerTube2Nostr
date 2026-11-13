# PeerTube2Nostr Web Dashboard

FastAPI backend + Vue 3 frontend for managing PeerTube sources, the publish
queue, relays and Nostr signing.

## Development

Backend (from the repository root):

```bash
python3 -m uvicorn webapp.backend.main:app --host 127.0.0.1 --port 8000 --reload
```

Frontend:

```bash
cd webapp/frontend
npm install
npm run dev
```

Vite serves the UI on <http://localhost:3000> and proxies `/api` to port 8000.
For both at once, run `npm run dev` from the repository root.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `PEERTUBE2NOSTR_DB_PATH` | `~/.local/share/peertube2nostr/peertube2nostr.db` | SQLite database path. |
| `PEERTUBE2NOSTR_DATA_DIR` | `~/.local/share/peertube2nostr` | Data directory used when no DB path is given. |
| `PEERTUBE2NOSTR_API_KEY` | empty (open) | Bearer token required for API access. |
| `PEERTUBE2NOSTR_DEFAULT_RELAYS` | built-in relay list | Comma/space separated default relays. |
| `PEERTUBE2NOSTR_RELAY_POLICY` | `open` | `open` or `locked` (see below). |
| `NSEC_FILE` | `<db>.nsec` | Fallback nsec file when the OS keyring is unavailable. |

### Relay policy

* `open` — users may add, remove and enable relays, and publish to any relay
  they select.
* `locked` — only admin defaults are published to; relay management endpoints
  return `403`. Defaults are synchronised into the database on startup and are
  never deleted by the sync.

Admin defaults are marked `is_default` and are shown as badges in the UI. They
are added to the database on every startup; removing one while the server is
running is possible under `open`, but it will be restored after a restart.

## Signing modes

| Mode | Where the key lives | When publishing happens |
| --- | --- | --- |
| `nsec` | Server keyring / nsec file | Background runner keeps publishing |
| `nip07` | Browser extension | While the dashboard is open |
| `nip46` | Remote bunker | While the dashboard is open and connected |
| Passkey | Browser (WebAuthn PRF), nsec also stored server-side | Same as `nsec` |

When `nip07` or `nip46` is selected, the server runner skips publishing; the
browser bridge claims one pending video at a time and signs it locally. A video
is claimed atomically, so the server and browser cannot publish the same item.

Client-side publishing requires the dashboard tab to stay open. Without a
server nsec or an always-on remote signer there is no way to publish while the
browser is closed.

## Relay selection

The Relays page has a **Publishing relays** section. Selections are stored in
`localStorage` under `pt2n:publish_relays:<identity>` and default to the admin
defaults (or all enabled relays). The selection is used for note publishing and
for encrypted state sync/restore.

Under a `locked` policy the backend rejects publish requests that target
anything other than the admin defaults.

## Tests

```bash
cd webapp/backend
pip install -r requirements-dev.txt
python3 -m pytest tests -q

cd ../frontend
npm run build   # includes vue-tsc type checking
```

## API highlights

* `GET /api/relays` — relays plus `default_relays` and `relay_policy`.
* `GET /api/settings` — publish limits, signing method, relay policy.
* `GET /api/queue/{id}/event-data?relays=…` — unsigned event data for a video.
* `POST /api/queue/publish-signed` — republish a verified client-signed event
  to the selected relays.
* `POST /api/sync` / `GET /api/sync/restore` — server-side encrypted state sync
  (client-side sync is used for browser signers).
