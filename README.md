# PeerTube2Nostr

Publish PeerTube channel videos to Nostr with proper attribution.

PeerTube2Nostr ingests videos from PeerTube channels using the PeerTube API as the primary source, with RSS/Atom as a fallback. It then publishes a Nostr note per video, embedding a direct MP4 link when available (best for most Nostr clients), with HLS (`.m3u8`) as a fallback, and always including the canonical watch URL.

## Features

* **API-first ingest**: pulls channel videos via PeerTube API (`/api/v1/video-channels/<channel>/videos`)
* **RSS fallback**: optional feed URL used if API fails or is not configured
* **Attribution preserved**: post includes creator/channel name and links where available
* **Client-friendly playback links**:

  * MP4 first (most compatible for Nostr clients)
  * HLS fallback
  * watch URL always included
* **SQLite state**:

  * tracks sources, relays, videos, posting status
  * de-duplication using canonicalised URLs and stable entry keys
* **Relay management**: add/remove/enable/disable relays in the DB
* **Retry**: failed publishes are re-queued after a configurable delay
* **Secure nsec storage**: store the signing key in your OS keyring (optional)

## How it works

1. Add a **channel source** (API primary) using a PeerTube channel URL.
2. Optionally set an **RSS feed fallback** for that source.
3. Add one or more **Nostr relays**.
4. Run the loop:

   * polls sources
   * inserts new videos into the DB
   * publishes one pending video per cycle

## Installation

Python 3.10+ recommended.

```bash
pip install requests feedparser pynostr keyring prompt_toolkit textual
```

## Docker (optional)

This repo includes a `Dockerfile` and `docker-compose.yml` to avoid host dependency conflicts.

```bash
mkdir -p data
docker compose build
```

The compose file sets `DB_PATH=/data/peertube2nostr.db` so you don't need to pass `--db` for Docker commands.

Run commands with Docker:

```bash
# first run: interactive setup (seed relays, set nsec, add channel)
docker compose run --rm peertube2nostr interactive

# or do it step-by-step:
docker compose run --rm peertube2nostr init
docker compose run --rm peertube2nostr set-nsec
docker compose run --rm peertube2nostr add-channel "https://example.tube/c/mychannel"

# run loop
docker compose up
```

Interactive CLI in Docker:

```bash
docker compose run --rm peertube2nostr interactive
```

## Quick start

### 1) Initialise the database

```bash
python peertube_nostr.py init --db peertube2nostr.db
```

### 2) Add relays

```bash
python peertube_nostr.py add-relay wss://relay.damus.io --db peertube2nostr.db
python peertube_nostr.py add-relay wss://nos.lol --db peertube2nostr.db
```

### 3) Add a channel (API primary)

Use a channel URL, for example:

* `https://example.tube/c/mychannel`
* `https://example.tube/video-channels/mychannel`

```bash
python peertube_nostr.py add-channel "https://example.tube/c/mychannel" --db peertube2nostr.db
```

### 4) Optional: set RSS fallback

If the instance/channel provides a feed:

```bash
python peertube_nostr.py set-rss 1 "https://example.tube/feeds/videos.xml?channelId=..." --db peertube2nostr.db
```

### 5) Run

Store your Nostr signing key (nsec) (optional):

```bash
python peertube_nostr.py set-nsec --db peertube2nostr.db
```

This will use the OS keyring when available. If keyring is not available, it falls back to a local file named like `peertube2nostr.db.nsec` (chmod 600). You can override the file location with `NSEC_FILE`.

Or set it per run:

```bash
export NOSTR_NSEC="nsec1..."
python peertube_nostr.py run --db peertube2nostr.db
```

## Configuration

Environment variables (optional):

* `NOSTR_NSEC` (required unless using `--nsec` or `set-nsec`)
* `NSEC_FILE` (optional, path for file-based nsec storage)
* `NOSTR_RELAYS` (comma-separated, overrides relays in DB)
* `POLL_SECONDS` (default `300`)
* `PUBLISH_INTERVAL_SECONDS` (default `10`)
* `min_publish_interval_seconds` (DB setting, default `1200`)
* `max_posts_per_hour` (DB setting, default `3`)
* `max_posts_per_day_per_source` (DB setting, default `1`)
* `RETRY_FAILED_AFTER_SECONDS` (default `3600`)
* `API_LIMIT_PER_SOURCE` (default `50`)
* `NEW_SOURCE_LOOKBACK_DAYS` (default `30`, only applies on first poll of a source)

Example:

```bash
export NOSTR_NSEC="nsec1..."
export NOSTR_RELAYS="wss://nos.lol,wss://relay.damus.io"
export POLL_SECONDS=180
export API_LIMIT_PER_SOURCE=30
python peertube_nostr.py run --db peertube2nostr.db
```

## Commands

### Sources

* `add-channel <channel_url>`: add an API-based source (primary ingest)
* `add-source <url>`: add a source by URL (channel or RSS)
* `add-rss <rss_url>`: add RSS-only source (fallback ingest only)
* `set-rss <source_id> <rss_url>`: set RSS fallback for a source
* `set-channel <source_id> <channel_url>`: set/replace channel URL for a source
* `edit-source <source_id> [--channel-url URL|none] [--rss-url URL|none]`: edit one or both URLs (`none` clears)
* `set-source-lookback <source_id> <days|none>`: override lookback days for a source
* `remove-source <source_id>`: remove a source
* `enable-source <id>` / `disable-source <id>`
* `list-sources`

### Relays

* `add-relay <relay_url>`
* `remove-relay <id|url>`
* `edit-relay <id|url> <new_url>`
* `enable-relay <id|url>` / `disable-relay <id|url>`
* `list-relays`

### Nsec storage

* `set-nsec`: stores in keyring if available, otherwise in `NSEC_FILE` or `<db>.nsec`
* `clear-nsec`: remove the stored nsec (keyring + file)

### Run loop

* `run` (poll + publish)
* `interactive` (poll + publish + interactive CLI)
* `sync-profile` (fetch profile metadata + NIP-65 relay list)
* `refresh` (ingest sources once)
* `repair-db` (normalise/repair DB fields)
* `resync-source <id>` (clear pending + re-ingest one source)
* `retry-failed [id]` (requeue failed items, all or by source)
* `set-rate` / `show-rate` (configure publish throttling)

Example:

```bash
python peertube_nostr.py interactive --db peertube2nostr.db
```

Tip: type `/` in interactive mode to show available commands.
With `textual` installed, `interactive` runs a full-screen TUI with a log view, status bar, command input, and a `/` command palette.
Press `d` to toggle the dashboard panels.
If `textual` isn't available, it falls back to the line-based prompt (with `prompt_toolkit` features if installed).

## Post format (what gets published)

Each video becomes a Nostr note containing:

* Title
* Author/channel credit
* Direct MP4 URL (if available)
* HLS URL (if available and different)
* Canonical watch URL
* Description/summary (if available)

Tags include:

* `t=video`, `t=peertube`
* `url` + `m` for the best available media link (MP4 preferred, else HLS)
* `r` tags referencing the watch URL and channel URL
* additional `peertube:*` tags when available

## Limitations and notes

* PeerTube instances may vary slightly in API behaviour and returned fields. The script uses best-effort parsing.
* Not all videos expose a direct MP4 file URL, depending on instance settings. In those cases the post will include HLS and the watch URL.
* This is a polling loop, not a webhook based system.

## Roadmap (practical next steps)

* Pagination for channel API listing (beyond first `API_LIMIT_PER_SOURCE`)
* Per-source relay sets (post different channels to different relays)
* Better media metadata tags (duration, size, resolution where available)
* Optional Nostr long-form (`kind:30023`) for richer formatting
* Rate limiting per relay + exponential backoff
