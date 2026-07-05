#!/usr/bin/env python3
"""
PeerTube -> Nostr publisher (CLI entry point)

Primary ingest = PeerTube API (channel videos)
Fallback ingest = RSS/Atom feed (if API fails or not configured)

Dependencies:
  pip install requests feedparser pynostr

Examples
  python peertube_nostr.py init --db peertube.db
  python peertube_nostr.py add-relay wss://relay.damus.io --db peertube.db
  python peertube_nostr.py add-channel "https://example.tube/c/mychannel" --db peertube.db
  NOSTR_NSEC="nsec1..." python peertube_nostr.py run --db peertube.db
"""

import os
import sys

_project_root = os.path.dirname(os.path.abspath(__file__))
_backend_dir = os.path.join(_project_root, 'webapp', 'backend')
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

__version__ = "0.1.0"

import argparse
import calendar
import getpass
import json
import shlex
import threading
import time
from datetime import datetime
from queue import Queue, Empty
from typing import Optional

from pynostr.event import Event
from pynostr.filters import Filters, FiltersList
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.styles import Style
except Exception:
    PromptSession = None
try:
    from textual.app import App, ComposeResult
    from textual.containers import Vertical, Horizontal
    from textual.widgets import Header, Footer, Input, Static, RichLog, ListView, ListItem, Label
except Exception:
    App = None

from core.database import Store, get_stored_nsec, set_stored_nsec, clear_stored_nsec
from core.peertube import PeerTubeClient
from core.nostr import NostrPublisher
from core.runner import Runner, RateLimiter, PendingSelector, _get_runtime_status, _set_runtime_status
from core.utils import UrlNormaliser, DEFAULT_RELAYS, _parse_any_timestamp, _format_table, _sleep_interruptible
from core.models import DashboardMetrics


# ---------------------------------------------------------------------------
# CLI definitions
# ---------------------------------------------------------------------------

def parse_cli() -> argparse.Namespace:
    argv = sys.argv[1:]
    db_value = os.environ.get("DB_PATH", "peertube2nostr.db")
    filtered_argv: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] == "--db" and i + 1 < len(argv):
            db_value = argv[i + 1]
            i += 2
        elif argv[i].startswith("--db="):
            db_value = argv[i][5:]
            i += 1
        else:
            filtered_argv.append(argv[i])
            i += 1

    p = argparse.ArgumentParser(description="PeerTube channel videos -> Nostr (API primary, RSS fallback)")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument("--db", default=db_value)

    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Initialise DB schema").set_defaults(cmd="init")

    s = sub.add_parser("add-channel", help="Add a channel source (API primary) using channel URL")
    s.add_argument("channel_url")
    s.set_defaults(cmd="add-channel")

    s = sub.add_parser("add-source", help="Add a source by URL (channel or RSS)")
    s.add_argument("url")
    s.set_defaults(cmd="add-source")

    s = sub.add_parser("add-rss", help="Add an RSS-only source (fallback ingest only)")
    s.add_argument("rss_url")
    s.set_defaults(cmd="add-rss")

    s = sub.add_parser("set-rss", help="Set/replace RSS fallback URL for an existing source id")
    s.add_argument("source_id", type=int)
    s.add_argument("rss_url")
    s.set_defaults(cmd="set-rss")

    s = sub.add_parser("set-channel", help="Set/replace channel URL (API primary) for an existing source id")
    s.add_argument("source_id", type=int)
    s.add_argument("channel_url")
    s.set_defaults(cmd="set-channel")

    s = sub.add_parser("edit-source", help="Edit source URLs (channel and/or RSS)")
    s.add_argument("source_id", type=int)
    s.add_argument("--channel-url", dest="channel_url", default=None)
    s.add_argument("--rss-url", dest="rss_url", default=None)
    s.set_defaults(cmd="edit-source")

    s = sub.add_parser("set-source-lookback", help="Set lookback days for a source (first poll only)")
    s.add_argument("source_id", type=int)
    s.add_argument("lookback_days")
    s.set_defaults(cmd="set-source-lookback")

    s = sub.add_parser("enable-source", help="Enable a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="enable-source")

    s = sub.add_parser("disable-source", help="Disable a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="disable-source")

    s = sub.add_parser("remove-source", help="Remove a source by id")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="remove-source")

    sub.add_parser("list-sources", help="List sources").set_defaults(cmd="list-sources")

    s = sub.add_parser("add-relay", help="Add a relay (validated and de-duped)")
    s.add_argument("relay_url")
    s.set_defaults(cmd="add-relay")

    s = sub.add_parser("remove-relay", help="Remove a relay by id or URL")
    s.add_argument("relay_id_or_url")
    s.set_defaults(cmd="remove-relay")

    s = sub.add_parser("edit-relay", help="Edit a relay URL by id or URL")
    s.add_argument("relay_id_or_url")
    s.add_argument("new_relay_url")
    s.set_defaults(cmd="edit-relay")

    s = sub.add_parser("enable-relay", help="Enable a relay by id or URL")
    s.add_argument("relay_id_or_url")
    s.set_defaults(cmd="enable-relay")

    s = sub.add_parser("disable-relay", help="Disable a relay by id or URL")
    s.add_argument("relay_id_or_url")
    s.set_defaults(cmd="disable-relay")

    sub.add_parser("list-relays", help="List relays").set_defaults(cmd="list-relays")

    s = sub.add_parser("run", help="Run polling and publishing loop")
    s.add_argument("--nsec", default=None, help="nsec signing key (or set NOSTR_NSEC)")
    s.add_argument("--relays", default=None, help="Comma-separated relay URLs (overrides DB if provided)")
    s.add_argument("--poll-seconds", type=int, default=int(os.environ.get("POLL_SECONDS", "300")))
    s.add_argument("--publish-interval-seconds", type=int, default=int(os.environ.get("PUBLISH_INTERVAL_SECONDS", "10")))
    s.add_argument("--retry-failed-after-seconds", type=int, default=int(os.environ.get("RETRY_FAILED_AFTER_SECONDS", "3600")))
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.add_argument("--new-source-lookback-days", type=int, default=int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30")))
    s.add_argument("--dry-run", action="store_true", help="Preview what would be published without sending to relays")
    s.set_defaults(cmd="run")

    s = sub.add_parser("interactive", help="Run with an interactive CLI to manage sources/relays/nsec")
    s.add_argument("--nsec", default=None)
    s.add_argument("--relays", default=None)
    s.add_argument("--poll-seconds", type=int, default=int(os.environ.get("POLL_SECONDS", "300")))
    s.add_argument("--publish-interval-seconds", type=int, default=int(os.environ.get("PUBLISH_INTERVAL_SECONDS", "10")))
    s.add_argument("--retry-failed-after-seconds", type=int, default=int(os.environ.get("RETRY_FAILED_AFTER_SECONDS", "3600")))
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.add_argument("--new-source-lookback-days", type=int, default=int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30")))
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(cmd="interactive")

    s = sub.add_parser("sync-profile", help="Sync profile metadata + NIP-65 relay list from relays")
    s.add_argument("--nsec", default=None)
    s.add_argument("--relays", default=None)
    s.add_argument("--import-relays", action="store_true", help="Import NIP-65 relays into DB")
    s.add_argument("--enable-imported", action="store_true", help="Enable imported relays (default: disabled)")
    s.add_argument("--disable-missing", action="store_true", help="Disable DB relays not present in NIP-65 list")
    s.add_argument("--timeout-seconds", type=int, default=8)
    s.set_defaults(cmd="sync-profile")

    s = sub.add_parser("refresh", help="Ingest sources once (manual refresh)")
    s.add_argument("--api-limit-per-source", type=int, default=int(os.environ.get("API_LIMIT_PER_SOURCE", "50")))
    s.add_argument("--new-source-lookback-days", type=int, default=int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30")))
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(cmd="refresh")

    s = sub.add_parser("repair-db", help="Repair/normalise DB fields after updates")
    s.set_defaults(cmd="repair-db")

    s = sub.add_parser("resync-source", help="Clear pending and re-ingest a single source")
    s.add_argument("source_id", type=int)
    s.set_defaults(cmd="resync-source")

    s = sub.add_parser("retry-failed", help="Requeue failed items")
    s.add_argument("source_id", nargs="?", default=None)
    s.set_defaults(cmd="retry-failed")

    s = sub.add_parser("set-rate", help="Set publish rate limits")
    s.add_argument("--min-interval-seconds", type=int, default=None)
    s.add_argument("--max-posts-per-hour", type=int, default=None)
    s.add_argument("--max-posts-per-day-per-source", type=int, default=None)
    s.set_defaults(cmd="set-rate")

    sub.add_parser("show-rate", help="Show publish rate limits").set_defaults(cmd="show-rate")

    s = sub.add_parser("set-nsec", help="Store nsec securely in OS keyring for this DB path")
    s.add_argument("--nsec", default=None, help="nsec signing key (prompted if omitted)")
    s.set_defaults(cmd="set-nsec")

    sub.add_parser("clear-nsec", help="Remove stored nsec from OS keyring for this DB path").set_defaults(cmd="clear-nsec")

    s = sub.add_parser("completions", help="Generate shell completion script (bash/zsh)")
    s.add_argument("shell", nargs="?", default="bash", choices=["bash", "zsh"])
    s.set_defaults(cmd="completions")

    return p.parse_args(filtered_argv)


def main() -> None:
    args = parse_cli()
    n = UrlNormaliser()
    store = Store(args.db, n)
    store.init_schema()

    try:
        if args.cmd == "init":
            store.seed_default_relays_if_empty()
            print(f"Initialised DB: {args.db}")
            return

        if args.cmd == "add-channel":
            sid = store.add_channel_source(args.channel_url)
            print(f"Added channel source id={sid}")
            return

        if args.cmd == "add-source":
            if not _maybe_add_url_as_source(store, n, args.url, print):
                raise SystemExit("URL did not look like a PeerTube channel or RSS feed.")
            return

        if args.cmd == "add-rss":
            rss_norm = n.normalise_feed_url(args.rss_url)
            if not n.looks_like_peertube_feed(rss_norm):
                print("Warning: RSS URL does not look like a typical PeerTube feed (still adding).")
            sid = store.add_rss_source(args.rss_url)
            print(f"Added RSS source id={sid} (canonical: {rss_norm})")
            return

        if args.cmd == "set-rss":
            rss_norm = n.normalise_feed_url(args.rss_url)
            if not n.looks_like_peertube_feed(rss_norm):
                print("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
            c = store.set_source_rss(args.source_id, args.rss_url)
            if not c:
                print(f"Source {args.source_id} not found.")
                return
            print(f"Set RSS fallback for source {args.source_id} (canonical: {rss_norm})")
            return

        if args.cmd == "set-channel":
            c = store.set_source_channel(args.source_id, args.channel_url)
            if not c:
                print(f"Source {args.source_id} not found.")
                return
            print(f"Set channel URL for source {args.source_id}")
            return

        if args.cmd == "enable-source":
            c = store.set_source_enabled(args.source_id, True)
            print(f"Enabled: {c}")
            return

        if args.cmd == "disable-source":
            c = store.set_source_enabled(args.source_id, False)
            print(f"Disabled: {c}")
            return

        if args.cmd == "remove-source":
            c = store.remove_source(args.source_id)
            print(f"Removed: {c}")
            return

        if args.cmd == "list-sources":
            rows = store.list_sources()
            if not rows:
                print("No sources.")
                return
            table_rows: list[list[str]] = []
            for (sid, enabled, api_base, api_channel, _api_channel_url, rss_url, lookback_days, last_polled_ts, last_error) in rows:
                lp = str(last_polled_ts) if last_polled_ts else "-"
                lb = str(lookback_days) if lookback_days is not None else "-"
                if last_polled_ts:
                    status = "ERR" if last_error else "OK"
                else:
                    status = "NEVER"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                table_rows.append([str(sid), str(enabled), api_base or "", api_channel or "", rss_url or "", lb, status, lp, le])
            for line in _format_table(
                ["id", "enabled", "api_base", "api_channel", "rss_url", "lookback", "last_status", "last_polled", "last_error"],
                table_rows,
            ):
                print(line)
            return

        if args.cmd == "add-relay":
            rid = store.add_relay(args.relay_url)
            print(f"Added relay id={rid}")
            return

        if args.cmd == "remove-relay":
            c = store.remove_relay(args.relay_id_or_url)
            print(f"Removed: {c}")
            return

        if args.cmd == "edit-relay":
            c = store.update_relay_url(args.relay_id_or_url, args.new_relay_url)
            print(f"Updated: {c}")
            return

        if args.cmd == "edit-source":
            _apply_edit_source(store, n, str(args.source_id), args.channel_url, args.rss_url, print)
            return

        if args.cmd == "set-source-lookback":
            val = str(args.lookback_days).strip().lower()
            if val in ("none", "null", "off"):
                c = store.set_source_lookback(args.source_id, None)
                if not c:
                    print(f"Source {args.source_id} not found.")
                    return
                print(f"Cleared lookback for source {args.source_id}")
                return
            try:
                days = int(val)
            except ValueError:
                raise SystemExit("lookback_days must be an integer or 'none'")
            c = store.set_source_lookback(args.source_id, days)
            if not c:
                print(f"Source {args.source_id} not found.")
                return
            print(f"Set lookback_days={days} for source {args.source_id}")
            return

        if args.cmd == "enable-relay":
            c = store.set_relay_enabled(args.relay_id_or_url, True)
            print(f"Enabled: {c}")
            return

        if args.cmd == "disable-relay":
            c = store.set_relay_enabled(args.relay_id_or_url, False)
            print(f"Disabled: {c}")
            return

        if args.cmd == "list-relays":
            rows = store.list_relays()
            if not rows:
                print("No relays.")
                return
            table_rows: list[list[str]] = []
            for (rid, enabled, url, url_norm, last_used_ts, last_error, _latency_ms) in rows:
                lu = str(last_used_ts) if last_used_ts else "-"
                le = (last_error or "").replace("\n", " ")
                if len(le) > 80:
                    le = le[:77] + "..."
                table_rows.append([str(rid), str(enabled), url, url_norm, lu, le])
            for line in _format_table(
                ["id", "enabled", "relay_url", "canonical", "last_used", "last_error"],
                table_rows,
            ):
                print(line)
            return

        if args.cmd in ("run", "interactive"):
            store.seed_default_relays_if_empty()

            nsec_env = os.environ.get("NOSTR_NSEC")
            if args.nsec and not nsec_env:
                print("Warning: passing --nsec on the command line is insecure (visible in process list).")
                print("Use NOSTR_NSEC env var instead, or run 'set-nsec' to store it securely.")
            nsec_env = nsec_env or args.nsec
            nsec = nsec_env or get_stored_nsec(args.db)
            if args.cmd == "run" and not nsec:
                raise SystemExit("Provide nsec via --nsec or NOSTR_NSEC, or run set-nsec to store it.")

            relays_env = os.environ.get("NOSTR_RELAYS")
            relays_cli = args.relays

            if relays_env and relays_env.strip():
                relays = [n.normalise_relay_url(x.strip()) for x in relays_env.split(",") if x.strip()]
            elif relays_cli and relays_cli.strip():
                relays = [n.normalise_relay_url(x.strip()) for x in relays_cli.split(",") if x.strip()]
            else:
                relays = None

            retry = args.retry_failed_after_seconds
            if retry == 0:
                retry = None

            if args.cmd == "interactive":
                _run_interactive(
                    args=args,
                    n=n,
                    nsec_env=nsec_env,
                    relays=relays,
                    retry=retry,
                )
            else:
                runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n, status_fn=_set_runtime_status, dry_run=args.dry_run)
                runner.run(
                    nsec=nsec_env,
                    relays=relays,
                    poll_seconds=args.poll_seconds,
                    publish_interval_seconds=args.publish_interval_seconds,
                    retry_failed_after_seconds=retry,
                    api_limit_per_source=args.api_limit_per_source,
                    new_source_lookback_days=args.new_source_lookback_days,
                )
            return

        if args.cmd == "sync-profile":
            sync_profile(
                store=store,
                n=n,
                nsec_arg=args.nsec,
                relays_arg=args.relays,
                import_relays=args.import_relays,
                enable_imported=args.enable_imported,
                disable_missing=args.disable_missing,
                timeout_seconds=args.timeout_seconds,
            )
            return

        if args.cmd == "refresh":
            runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n, dry_run=args.dry_run)
            runner.ingest_sources_once(
                api_limit=args.api_limit_per_source,
                lookback_days=args.new_source_lookback_days,
            )
            return

        if args.cmd == "repair-db":
            repair_db(store, n, print)
            return

        if args.cmd == "resync-source":
            _resync_source(store, n, args.source_id, print)
            return

        if args.cmd == "retry-failed":
            if args.source_id:
                try:
                    sid = int(args.source_id)
                except ValueError:
                    raise SystemExit("source_id must be an integer")
                count = store.retry_failed_for_source(sid, older_than_seconds=0)
                print(f"Re-queued failed items for source {sid}: {count}")
            else:
                count = store.retry_failed(older_than_seconds=0)
                print(f"Re-queued failed items: {count}")
            return

        if args.cmd == "set-rate":
            if args.min_interval_seconds is not None:
                store.set_setting("min_publish_interval_seconds", str(int(args.min_interval_seconds)))
            if args.max_posts_per_hour is not None:
                store.set_setting("max_posts_per_hour", str(int(args.max_posts_per_hour)))
            if args.max_posts_per_day_per_source is not None:
                store.set_setting("max_posts_per_day_per_source", str(int(args.max_posts_per_day_per_source)))
            min_interval, max_per_hour = store.get_publish_limits()
            max_per_day_per_source = store.get_daily_source_limit()
            print(
                "Rate limits: "
                f"min_interval_seconds={min_interval}, "
                f"max_posts_per_hour={max_per_hour}, "
                f"max_posts_per_day_per_source={max_per_day_per_source}"
            )
            return

        if args.cmd == "show-rate":
            min_interval, max_per_hour = store.get_publish_limits()
            max_per_day_per_source = store.get_daily_source_limit()
            print(
                "Rate limits: "
                f"min_interval_seconds={min_interval}, "
                f"max_posts_per_hour={max_per_hour}, "
                f"max_posts_per_day_per_source={max_per_day_per_source}"
            )
            return

        if args.cmd == "set-nsec":
            nsec = args.nsec
            if not nsec:
                nsec = getpass.getpass("Enter nsec: ").strip()
            if not nsec:
                raise SystemExit("nsec cannot be empty.")
            store_type, path = set_stored_nsec(args.db, nsec)
            if store_type == "keyring":
                print("Stored nsec in OS keyring for this DB path.")
            else:
                print(f"Stored nsec in file: {path}")
            return

        if args.cmd == "clear-nsec":
            removed = clear_stored_nsec(args.db)
            print("Removed stored nsec." if removed else "No stored nsec found.")
            return

        if args.cmd == "completions":
            cmds = sorted(_interactive_commands() + ["help", "--help", "-h"])
            script_fn = os.path.basename(__file__)
            fn_id = script_fn.replace(".", "_").replace("/", "_")
            if args.shell == "zsh":
                lines = [
                    "#compdef " + script_fn,
                    "_" + script_fn + "() {",
                    '  local -a cmds',
                    '  cmds=(',
                    '    ' + " ".join('"' + c + ':' + c + '"' for c in cmds),
                    '  )',
                    "  _describe 'command' cmds",
                    "}",
                    "_" + script_fn,
                ]
            else:
                lines = [
                    "_" + fn_id + "() {",
                    '  local cur="${COMP_WORDS[$COMP_CWORD]}"',
                    '  local prev="${COMP_WORDS[$COMP_CWORD-1]}"',
                    '  local cmds="' + " ".join(cmds) + '"',
                    '  COMPREPLY=($(compgen -W "$cmds" -- "$cur"))',
                    "}",
                    "complete -F _" + fn_id + " " + script_fn,
                ]
            print("\n".join(lines))
            return

    except SystemExit as ex:
        msg = str(ex)
        if msg:
            print(msg)
        sys.exit(1)
    except Exception as ex:
        print(f"Error: {ex}")
        sys.exit(1)
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Interactive shell (prompt_toolkit-based)
# ---------------------------------------------------------------------------

def _interactive_shell(db_path: str, n: UrlNormaliser, stop_event: threading.Event) -> None:
    store = Store(db_path, n)
    store.init_schema()
    _interactive_first_run(store, db_path, n)
    commands = _interactive_commands()

    def _relay_tokens() -> list[str]:
        rows = store.list_relays()
        out: list[str] = []
        for (rid, _enabled, url, _url_norm, _last_used_ts, _last_error, _latency_ms) in rows:
            out.append(str(rid))
            if url:
                out.append(str(url))
        return out

    def _source_ids() -> list[str]:
        rows = store.list_sources()
        return [str(r[0]) for r in rows]

    class _InteractiveCompleter(Completer):
        def get_completions(self, document, complete_event):
            text = document.text_before_cursor
            try:
                parts = shlex.split(text)
            except ValueError:
                parts = text.split()
            if text.endswith(" "):
                parts.append("")
            if len(parts) <= 1:
                word = parts[0] if parts else ""
                for c in commands:
                    if c.startswith(word):
                        yield Completion(c, start_position=-len(word))
                return

            cmd = _normalize_cmd(parts[0])
            current = parts[-1]

            if cmd in ("enable-relay", "disable-relay", "remove-relay", "edit-relay"):
                if len(parts) <= 2:
                    for val in _relay_tokens():
                        if val.startswith(current):
                            yield Completion(val, start_position=-len(current))
                return

            if cmd in ("enable-source", "disable-source", "set-rss", "set-channel", "edit-source", "set-source-lookback", "remove-source"):
                if len(parts) <= 2:
                    for val in _source_ids():
                        if val.startswith(current):
                            yield Completion(val, start_position=-len(current))
                return

    def _history_path() -> str:
        base = os.path.dirname(os.path.abspath(db_path)) or "."
        return os.path.join(base, ".peertube2nostr_history")

    def _log(msg: str) -> None:
        print(msg)

    print("== PeerTube2Nostr Interactive ==")
    print("Type '/', '--help', or '?' for commands. 'quit' to exit.")
    _log(_interactive_dashboard(store, db_path))

    try:
        session = None
        if PromptSession is not None:
            try:
                style = Style.from_dict(
                    {"prompt": "ansicyan bold", "toolbar": "ansiblack bg:ansiwhite"}
                )
                session = PromptSession(
                    message=[("class:prompt", "[PT2N]> ")],
                    history=FileHistory(_history_path()),
                    auto_suggest=AutoSuggestFromHistory(),
                    completer=_InteractiveCompleter(),
                    style=style,
                    bottom_toolbar=lambda: _status_toolbar(store, db_path),
                )
            except Exception:
                session = None

        arg_prompts = _interactive_arg_prompts()
        while True:
            try:
                if session is not None:
                    line = session.prompt().strip()
                else:
                    line = input("[PT2N]> ").strip()
            except EOFError:
                line = "quit"
            if not line:
                continue
            parts = shlex.split(line)
            cmd = _normalize_cmd(parts[0])
            args = parts[1:]

            if cmd not in commands and not args and line.startswith(("http://", "https://")):
                if _maybe_add_url_as_source(store, n, line.strip(), _log):
                    continue

            if cmd == "edit-source" and len(args) < 3:
                try:
                    source_id = args[0] if args else input("Source id: ").strip()
                except EOFError:
                    source_id = ""
                if not source_id:
                    _log("Canceled.")
                    continue
                try:
                    choice = (input("Change what? (channel/rss/both): ").strip().lower() or "both")
                except EOFError:
                    choice = ""
                if choice not in ("channel", "rss", "both"):
                    _log("Choose: channel, rss, or both.")
                    continue
                channel_url = ""
                rss_url = ""
                if choice in ("channel", "both"):
                    try:
                        channel_url = input("Channel URL (blank to skip, 'none' to clear): ").strip()
                    except EOFError:
                        channel_url = ""
                if choice in ("rss", "both"):
                    try:
                        rss_url = input("RSS URL (blank to skip, 'none' to clear): ").strip()
                    except EOFError:
                        rss_url = ""
                _apply_edit_source(store, n, source_id, channel_url or None, rss_url or None, _log)
                continue

            if cmd == "set-rate" and len(args) < 3:
                try:
                    min_int = args[0] if len(args) > 0 else input("Min interval seconds: ").strip()
                except EOFError:
                    min_int = ""
                try:
                    max_per = args[1] if len(args) > 1 else input("Max posts per hour: ").strip()
                except EOFError:
                    max_per = ""
                try:
                    max_day = args[2] if len(args) > 2 else input("Max posts per day per source: ").strip()
                except EOFError:
                    max_day = ""
                parsed = _parse_set_rate_args(
                    ([f"--min-interval-seconds={min_int}"] if min_int else [])
                    + ([f"--max-posts-per-hour={max_per}"] if max_per else [])
                    + ([f"--max-posts-per-day-per-source={max_day}"] if max_day else [])
                )
                if isinstance(parsed, str):
                    _log(parsed)
                else:
                    if parsed.min_interval_seconds is not None:
                        store.set_setting("min_publish_interval_seconds", str(int(parsed.min_interval_seconds)))
                    if parsed.max_posts_per_hour is not None:
                        store.set_setting("max_posts_per_hour", str(int(parsed.max_posts_per_hour)))
                    if parsed.max_posts_per_day_per_source is not None:
                        store.set_setting(
                            "max_posts_per_day_per_source",
                            str(int(parsed.max_posts_per_day_per_source)),
                        )
                    min_interval, max_per_hour = store.get_publish_limits()
                    max_per_day_per_source = store.get_daily_source_limit()
                    _log(
                        "Rate limits: "
                        f"min_interval_seconds={min_interval}, "
                        f"max_posts_per_hour={max_per_hour}, "
                        f"max_posts_per_day_per_source={max_per_day_per_source}"
                    )
                continue

            if cmd == "resync-source" and len(args) < 1:
                try:
                    source_id = input("Source id: ").strip()
                except EOFError:
                    source_id = ""
                if not source_id:
                    _log("Canceled.")
                    continue
                _resync_source(store, n, int(source_id), _log)
                continue

            if cmd == "retry-failed" and len(args) < 1:
                try:
                    source_id = input("Source id (blank for all): ").strip()
                except EOFError:
                    source_id = ""
                if not source_id:
                    count = store.retry_failed(older_than_seconds=0)
                    _log(f"Re-queued failed items: {count}")
                else:
                    count = store.retry_failed_for_source(int(source_id), older_than_seconds=0)
                    _log(f"Re-queued failed items for source {source_id}: {count}")
                continue

            if cmd == "repair-db":
                repair_db(store, n, _log)
                continue

            if cmd in arg_prompts and len(args) < len(arg_prompts[cmd]):
                prompts = arg_prompts[cmd][len(args):]
                for prompt in prompts:
                    try:
                        val = input(f"{prompt}: ").strip()
                    except EOFError:
                        val = ""
                    if not val:
                        _log("Canceled.")
                        break
                    args.append(val)
                else:
                    pass
                if len(args) < len(arg_prompts[cmd]):
                    continue

            should_quit = _dispatch_command(store, n, db_path, cmd, args, _log)
            if should_quit:
                stop_event.set()
                return
    finally:
        store.close()


def _interactive_first_run(store: Store, db_path: str, n: UrlNormaliser) -> None:
    has_sources = store.count_sources() > 0
    has_nsec = bool(get_stored_nsec(db_path))
    if has_sources and has_nsec:
        return

    print("First run setup (press Enter to skip any step).")

    if store.count_relays() == 0:
        ans = input("Seed default relays? [Y/n]: ").strip().lower()
        if ans in ("", "y", "yes"):
            store.seed_default_relays_if_empty()
            print("Seeded default relays.")

    if not has_nsec:
        ans = input("Set nsec now? [Y/n]: ").strip().lower()
        if ans in ("", "y", "yes"):
            nsec = getpass.getpass("Enter nsec: ").strip()
            if nsec:
                store_type, path = set_stored_nsec(db_path, nsec)
                if store_type == "keyring":
                    print("Stored nsec in OS keyring for this DB path.")
                else:
                    print(f"Stored nsec in file: {path}")

    if not has_sources:
        channel_url = input("Add PeerTube channel URL (blank to skip): ").strip()
        if channel_url:
            try:
                sid = store.add_channel_source(channel_url)
                print(f"Added channel source id={sid}")
            except Exception as ex:
                print(f"Failed to add channel: {ex}")

            rss_url = input("Add RSS fallback URL (blank to skip): ").strip()
            if rss_url:
                try:
                    rss_norm = n.normalise_feed_url(rss_url)
                    if not n.looks_like_peertube_feed(rss_norm):
                        print("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
                    store.set_source_rss(sid, rss_url)
                    print(f"Set RSS fallback for source {sid} (canonical: {rss_norm})")
                except Exception as ex:
                    print(f"Failed to set RSS: {ex}")


def _interactive_commands() -> list[str]:
    return [
        "help", "status", "init", "refresh", "repair-db", "resync-source", "retry-failed", "sync-profile", "set-rate", "show-rate",
        "list-relays", "add-relay", "remove-relay", "edit-relay", "enable-relay", "disable-relay",
        "list-sources", "add-channel", "add-source", "add-rss", "set-rss", "set-channel", "edit-source", "set-source-lookback", "enable-source", "disable-source", "remove-source",
        "set-nsec", "clear-nsec",
        "quit", "exit",
    ]


def _interactive_arg_prompts() -> dict[str, list[str]]:
    return {
        "add-relay": ["Relay URL"],
        "remove-relay": ["Relay id or URL"],
        "edit-relay": ["Relay id or URL", "New relay URL"],
        "enable-relay": ["Relay id or URL"],
        "disable-relay": ["Relay id or URL"],
        "add-channel": ["Channel URL"],
        "add-source": ["Channel or RSS URL"],
        "add-rss": ["RSS URL"],
        "set-rss": ["Source id", "RSS URL"],
        "set-channel": ["Source id", "Channel URL"],
        "edit-source": [],
        "set-source-lookback": ["Source id", "Lookback days (or 'none')"],
        "set-rate": ["Min interval seconds", "Max posts per hour", "Max posts per day per source"],
        "enable-source": ["Source id"],
        "disable-source": ["Source id"],
        "remove-source": ["Source id"],
        "resync-source": ["Source id"],
        "retry-failed": ["Source id (blank for all)"],
    }


def _emit_help(log_fn) -> None:
    for line in _help_lines():
        log_fn(line)


def _help_lines() -> list[str]:
    return [
        "Commands:",
        "  help | / | ?                     Show this help",
        "  status                            Show counts + nsec status",
        "  init                              Init DB + seed relays (if empty)",
        "  refresh                           Ingest sources once (manual)",
        "  repair-db                         Repair/normalise DB fields",
        "  resync-source <id>                Clear pending + re-ingest one source",
        "  retry-failed [id]                 Requeue failed items (all or by source)",
        "  sync-profile [--relays ...]       Fetch kind 0 + 10002 for your pubkey",
        "  show-rate                         Show publish rate limits",
        "  set-rate [--min-interval-seconds N] [--max-posts-per-hour N]  Set limits",
        "  list-relays                       List relays",
        "  add-relay <url>                   Add relay",
        "  remove-relay <id|url>             Remove relay",
        "  edit-relay <id|url> <new_url>     Edit relay URL",
        "  enable-relay <id|url>             Enable relay",
        "  disable-relay <id|url>            Disable relay",
        "  list-sources                      List sources",
        "  add-channel <url>                 Add PeerTube channel",
        "  add-source <url>                  Add source (channel or RSS)",
        "  add-rss <url>                     Add RSS-only source",
        "  set-rss <id> <url>                Set RSS fallback",
        "  set-channel <id> <url>            Set channel URL (API primary)",
        "  edit-source <id> [--channel-url X] [--rss-url Y]  Edit source URLs",
        "  set-source-lookback <id> <days|none>  Set per-source lookback days",
        "  enable-source <id>                Enable source",
        "  disable-source <id>               Disable source",
        "  remove-source <id>                Remove source",
        "  set-nsec [nsec]                   Store nsec (prompt if omitted)",
        "  clear-nsec                        Remove stored nsec",
        "  quit | exit                       Stop",
    ]


# ---------------------------------------------------------------------------
# Command registry (interactive dispatch)
# ---------------------------------------------------------------------------

from dataclasses import dataclass


@dataclass
class CommandContext:
    store: Store
    n: UrlNormaliser
    db_path: str
    log_fn: callable


class CommandRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, tuple[callable, int, Optional[int]]] = {}

    def register(self, name: str, handler: callable, min_args: int = 0, max_args: Optional[int] = None) -> None:
        self._handlers[name] = (handler, min_args, max_args)

    def dispatch(self, ctx: CommandContext, cmd: str, args: list[str]) -> bool:
        cmd = _normalize_cmd(cmd)
        if cmd in ("/", "?", "help", "--help", "-h"):
            _emit_help(ctx.log_fn)
            return False
        if cmd in ("quit", "exit"):
            return True
        if not args and cmd.startswith(("http://", "https://")):
            if _maybe_add_url_as_source(ctx.store, ctx.n, cmd, ctx.log_fn):
                return False
        entry = self._handlers.get(cmd)
        if not entry:
            ctx.log_fn("Unknown or invalid command. Type '/' for usage.")
            return False
        handler, min_args, max_args = entry
        if len(args) < min_args:
            ctx.log_fn("Unknown or invalid command. Type '/' for usage.")
            return False
        if max_args is not None and len(args) > max_args:
            ctx.log_fn("Unknown or invalid command. Type '/' for usage.")
            return False
        return bool(handler(ctx, args))


def _cmd_status(ctx: CommandContext, _args: list[str]) -> bool:
    relays = ctx.store.get_enabled_relays()
    pending = ctx.store.count_pending()
    sources = ctx.store.count_sources()
    has_nsec = bool(get_stored_nsec(ctx.db_path))
    ctx.log_fn(f"Relays enabled: {len(relays)} | Sources: {sources} | Pending: {pending} | Nsec set: {has_nsec}")
    return False


def _cmd_init(ctx: CommandContext, _args: list[str]) -> bool:
    ctx.store.init_schema()
    ctx.store.seed_default_relays_if_empty()
    ctx.log_fn(f"Initialised DB: {ctx.db_path}")
    return False


def _cmd_sync_profile(ctx: CommandContext, args: list[str]) -> bool:
    parsed = _parse_sync_profile_args(args)
    if isinstance(parsed, str):
        ctx.log_fn(parsed)
        return False
    try:
        sync_profile(
            store=ctx.store,
            n=ctx.n,
            nsec_arg=parsed.nsec,
            relays_arg=parsed.relays,
            import_relays=parsed.import_relays,
            enable_imported=parsed.enable_imported,
            disable_missing=parsed.disable_missing,
            timeout_seconds=parsed.timeout_seconds,
            log_fn=ctx.log_fn,
        )
    except SystemExit as ex:
        msg = str(ex) or "sync-profile failed."
        ctx.log_fn(msg)
    except Exception as ex:
        ctx.log_fn(f"sync-profile error: {ex}")
    return False


def _cmd_refresh(ctx: CommandContext, _args: list[str]) -> bool:
    api_limit = int(os.environ.get("API_LIMIT_PER_SOURCE", "50"))
    lookback_days = int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30"))
    runner = Runner(ctx.store, PeerTubeClient(ctx.n), NostrPublisher(), ctx.n, log_fn=ctx.log_fn)
    runner.ingest_sources_once(api_limit, lookback_days)
    return False


def _cmd_repair_db(ctx: CommandContext, _args: list[str]) -> bool:
    repair_db(ctx.store, ctx.n, ctx.log_fn)
    return False


def _cmd_resync_source(ctx: CommandContext, args: list[str]) -> bool:
    try:
        sid = int(args[0])
    except ValueError:
        ctx.log_fn("source_id must be an integer")
        return False
    _resync_source(ctx.store, ctx.n, sid, ctx.log_fn)
    return False


def _cmd_retry_failed(ctx: CommandContext, args: list[str]) -> bool:
    if args:
        try:
            sid = int(args[0])
        except ValueError:
            ctx.log_fn("source_id must be an integer")
            return False
        count = ctx.store.retry_failed_for_source(sid, older_than_seconds=0)
        ctx.log_fn(f"Re-queued failed items for source {sid}: {count}")
    else:
        count = ctx.store.retry_failed(older_than_seconds=0)
        ctx.log_fn(f"Re-queued failed items: {count}")
    return False


def _cmd_show_rate(ctx: CommandContext, _args: list[str]) -> bool:
    min_interval, max_per_hour = ctx.store.get_publish_limits()
    ctx.log_fn(f"Rate limits: min_interval_seconds={min_interval}, max_posts_per_hour={max_per_hour}")
    return False


def _cmd_set_rate(ctx: CommandContext, args: list[str]) -> bool:
    parsed = _parse_set_rate_args(args)
    if isinstance(parsed, str):
        ctx.log_fn(parsed)
        return False
    if parsed.min_interval_seconds is not None:
        ctx.store.set_setting("min_publish_interval_seconds", str(int(parsed.min_interval_seconds)))
    if parsed.max_posts_per_hour is not None:
        ctx.store.set_setting("max_posts_per_hour", str(int(parsed.max_posts_per_hour)))
    if parsed.max_posts_per_day_per_source is not None:
        ctx.store.set_setting("max_posts_per_day_per_source", str(int(parsed.max_posts_per_day_per_source)))
    min_interval, max_per_hour = ctx.store.get_publish_limits()
    max_per_day_per_source = ctx.store.get_daily_source_limit()
    ctx.log_fn(
        "Rate limits: "
        f"min_interval_seconds={min_interval}, "
        f"max_posts_per_hour={max_per_hour}, "
        f"max_posts_per_day_per_source={max_per_day_per_source}"
    )
    return False


def _cmd_edit_source(ctx: CommandContext, args: list[str]) -> bool:
    parsed = _parse_edit_source_args(args)
    if isinstance(parsed, str):
        ctx.log_fn(parsed)
        return False
    _apply_edit_source(ctx.store, ctx.n, parsed.source_id, parsed.channel_url, parsed.rss_url, ctx.log_fn)
    return False


def _cmd_list_relays(ctx: CommandContext, _args: list[str]) -> bool:
    rows = ctx.store.list_relays()
    if not rows:
        ctx.log_fn("No relays.")
    else:
        table_rows: list[list[str]] = []
        for (rid, enabled, url, url_norm, last_used_ts, last_error, _latency_ms) in rows:
            lu = str(last_used_ts) if last_used_ts else "-"
            le = (last_error or "").replace("\n", " ")
            if len(le) > 80:
                le = le[:77] + "..."
            table_rows.append([str(rid), str(enabled), url, url_norm, lu, le])
        for line in _format_table(
            ["id", "enabled", "relay_url", "canonical", "last_used", "last_error"],
            table_rows,
        ):
            ctx.log_fn(line)
    return False


def _cmd_add_relay(ctx: CommandContext, args: list[str]) -> bool:
    rid = ctx.store.add_relay(args[0])
    ctx.log_fn(f"Added relay id={rid}")
    return False


def _cmd_add_source(ctx: CommandContext, args: list[str]) -> bool:
    if not _maybe_add_url_as_source(ctx.store, ctx.n, args[0], ctx.log_fn):
        ctx.log_fn("URL did not look like a PeerTube channel or RSS feed.")
    return False


def _cmd_remove_relay(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.remove_relay(args[0])
    ctx.log_fn(f"Removed: {c}")
    return False


def _cmd_edit_relay(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.update_relay_url(args[0], args[1])
    ctx.log_fn(f"Updated: {c}")
    return False


def _cmd_enable_relay(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.set_relay_enabled(args[0], True)
    ctx.log_fn(f"Enabled: {c}")
    return False


def _cmd_disable_relay(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.set_relay_enabled(args[0], False)
    ctx.log_fn(f"Disabled: {c}")
    return False


def _cmd_list_sources(ctx: CommandContext, _args: list[str]) -> bool:
    rows = ctx.store.list_sources()
    if not rows:
        ctx.log_fn("No sources.")
    else:
        table_rows: list[list[str]] = []
        for (sid, enabled, api_base, api_channel, _api_channel_url, rss_url, lookback_days, last_polled_ts, last_error) in rows:
            lp = str(last_polled_ts) if last_polled_ts else "-"
            lb = str(lookback_days) if lookback_days is not None else "-"
            if last_polled_ts:
                status = "ERR" if last_error else "OK"
            else:
                status = "NEVER"
            le = (last_error or "").replace("\n", " ")
            if len(le) > 80:
                le = le[:77] + "..."
            table_rows.append([str(sid), str(enabled), api_base or "", api_channel or "", rss_url or "", lb, status, lp, le])
        for line in _format_table(
            ["id", "enabled", "api_base", "api_channel", "rss_url", "lookback", "last_status", "last_polled", "last_error"],
            table_rows,
        ):
            ctx.log_fn(line)
    return False


def _cmd_add_channel(ctx: CommandContext, args: list[str]) -> bool:
    sid = ctx.store.add_channel_source(args[0])
    ctx.log_fn(f"Added channel source id={sid}")
    return False


def _cmd_add_rss(ctx: CommandContext, args: list[str]) -> bool:
    rss_norm = ctx.n.normalise_feed_url(args[0])
    if not ctx.n.looks_like_peertube_feed(rss_norm):
        ctx.log_fn("Warning: RSS URL does not look like a typical PeerTube feed (still adding).")
    sid = ctx.store.add_rss_source(args[0])
    ctx.log_fn(f"Added RSS source id={sid} (canonical: {rss_norm})")
    return False


def _cmd_set_rss(ctx: CommandContext, args: list[str]) -> bool:
    rss_norm = ctx.n.normalise_feed_url(args[1])
    if not ctx.n.looks_like_peertube_feed(rss_norm):
        ctx.log_fn("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
    c = ctx.store.set_source_rss(int(args[0]), args[1])
    if not c:
        ctx.log_fn(f"Source {args[0]} not found.")
        return False
    ctx.log_fn(f"Set RSS fallback for source {args[0]} (canonical: {rss_norm})")
    return False


def _cmd_set_channel(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.set_source_channel(int(args[0]), args[1])
    if not c:
        ctx.log_fn(f"Source {args[0]} not found.")
        return False
    ctx.log_fn(f"Set channel URL for source {args[0]}")
    return False


def _cmd_set_source_lookback(ctx: CommandContext, args: list[str]) -> bool:
    val = str(args[1]).strip().lower()
    if val in ("none", "null", "off"):
        c = ctx.store.set_source_lookback(int(args[0]), None)
        if not c:
            ctx.log_fn(f"Source {args[0]} not found.")
            return False
        ctx.log_fn(f"Cleared lookback for source {args[0]}")
        return False
    try:
        days = int(val)
    except ValueError:
        ctx.log_fn("lookback_days must be an integer or 'none'")
        return False
    c = ctx.store.set_source_lookback(int(args[0]), days)
    if not c:
        ctx.log_fn(f"Source {args[0]} not found.")
        return False
    ctx.log_fn(f"Set lookback_days={days} for source {args[0]}")
    return False


def _cmd_enable_source(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.set_source_enabled(int(args[0]), True)
    ctx.log_fn(f"Enabled: {c}")
    return False


def _cmd_disable_source(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.set_source_enabled(int(args[0]), False)
    ctx.log_fn(f"Disabled: {c}")
    return False


def _cmd_remove_source(ctx: CommandContext, args: list[str]) -> bool:
    c = ctx.store.remove_source(int(args[0]))
    ctx.log_fn(f"Removed: {c}")
    return False


def _cmd_set_nsec(ctx: CommandContext, args: list[str]) -> bool:
    nsec = args[0] if args else getpass.getpass("Enter nsec: ").strip()
    if not nsec:
        ctx.log_fn("nsec cannot be empty.")
        return False
    store_type, path = set_stored_nsec(ctx.db_path, nsec)
    if store_type == "keyring":
        ctx.log_fn("Stored nsec in OS keyring for this DB path.")
    else:
        ctx.log_fn(f"Stored nsec in file: {path}")
    return False


def _cmd_clear_nsec(ctx: CommandContext, _args: list[str]) -> bool:
    removed = clear_stored_nsec(ctx.db_path)
    ctx.log_fn("Removed stored nsec." if removed else "No stored nsec found.")
    return False


_COMMAND_REGISTRY: Optional[CommandRegistry] = None


def _get_command_registry() -> CommandRegistry:
    global _COMMAND_REGISTRY
    if _COMMAND_REGISTRY is not None:
        return _COMMAND_REGISTRY
    reg = CommandRegistry()
    reg.register("status", _cmd_status, min_args=0, max_args=0)
    reg.register("init", _cmd_init, min_args=0, max_args=0)
    reg.register("sync-profile", _cmd_sync_profile)
    reg.register("refresh", _cmd_refresh, min_args=0, max_args=0)
    reg.register("repair-db", _cmd_repair_db, min_args=0, max_args=0)
    reg.register("resync-source", _cmd_resync_source, min_args=1, max_args=1)
    reg.register("retry-failed", _cmd_retry_failed, min_args=0, max_args=1)
    reg.register("show-rate", _cmd_show_rate, min_args=0, max_args=0)
    reg.register("set-rate", _cmd_set_rate)
    reg.register("edit-source", _cmd_edit_source)
    reg.register("list-relays", _cmd_list_relays, min_args=0, max_args=0)
    reg.register("add-relay", _cmd_add_relay, min_args=1, max_args=1)
    reg.register("remove-relay", _cmd_remove_relay, min_args=1, max_args=1)
    reg.register("edit-relay", _cmd_edit_relay, min_args=2, max_args=2)
    reg.register("enable-relay", _cmd_enable_relay, min_args=1, max_args=1)
    reg.register("disable-relay", _cmd_disable_relay, min_args=1, max_args=1)
    reg.register("list-sources", _cmd_list_sources, min_args=0, max_args=0)
    reg.register("add-channel", _cmd_add_channel, min_args=1, max_args=1)
    reg.register("add-source", _cmd_add_source, min_args=1, max_args=1)
    reg.register("add-rss", _cmd_add_rss, min_args=1, max_args=1)
    reg.register("set-rss", _cmd_set_rss, min_args=2, max_args=2)
    reg.register("set-channel", _cmd_set_channel, min_args=2, max_args=2)
    reg.register("set-source-lookback", _cmd_set_source_lookback, min_args=2, max_args=2)
    reg.register("enable-source", _cmd_enable_source, min_args=1, max_args=1)
    reg.register("disable-source", _cmd_disable_source, min_args=1, max_args=1)
    reg.register("remove-source", _cmd_remove_source, min_args=1, max_args=1)
    reg.register("set-nsec", _cmd_set_nsec)
    reg.register("clear-nsec", _cmd_clear_nsec, min_args=0, max_args=0)
    _COMMAND_REGISTRY = reg
    return reg


def _dispatch_command(store: Store, n: UrlNormaliser, db_path: str, cmd: str, args: list[str], log_fn) -> bool:
    ctx = CommandContext(store=store, n=n, db_path=db_path, log_fn=log_fn)
    return _get_command_registry().dispatch(ctx, cmd, args)


def _status_toolbar(store: Store, db_path: str) -> str:
    metrics = DashboardMetrics.from_store(store, db_path)
    return metrics.status_toolbar()


def _interactive_dashboard(store: Store, db_path: str) -> str:
    metrics = DashboardMetrics.from_store(store, db_path)
    return "\n".join(metrics.dashboard_lines())


def _format_dashboard_panels(store: Store, db_path: str) -> dict[str, str]:
    metrics = DashboardMetrics.from_store(store, db_path)
    counts = metrics.counts_block()
    activity = metrics.activity_block()
    rate = metrics.rate_block()
    pending_lines: list[str] = []
    selector = PendingSelector(store)
    rows = selector.list_pending(limit=200)
    if not rows:
        pending_lines.append("(none)")
    else:
        now_ts = int(time.time())
        daily_counts = selector.daily_counts(now_ts) if metrics.max_per_day_per_source > 0 else {}
        eligible_lines: list[str] = []
        blocked_lines: list[str] = []
        for (vid, sid, title, watch_url, _first_seen_ts, published_ts, api_base, api_channel, _rss_url) in rows:
            label = title or watch_url or f"video {vid}"
            source_label = f"{api_base or ''} {api_channel or ''}".strip() or f"source {sid}"
            if len(label) > 70:
                label = label[:67] + "..."
            if published_ts:
                age = int(time.time()) - int(published_ts)
                age_txt = f"{age//3600}h" if age >= 3600 else f"{age//60}m"
            else:
                age_txt = "?"
            line = f"{label} ({age_txt}) [{source_label}]"
            if metrics.max_per_day_per_source > 0 and int(daily_counts.get(int(sid), 0)) >= metrics.max_per_day_per_source:
                blocked_lines.append(f"{line} (daily limit)")
            else:
                eligible_lines.append(line)
        if eligible_lines:
            pending_lines.extend(eligible_lines)
            pending_lines.extend(blocked_lines)
        else:
            pending_lines.append("(no eligible items; daily limits reached)")
            pending_lines.extend(blocked_lines)
    return {"counts": counts, "activity": activity, "rate": rate, "queue": pending_lines}


# ---------------------------------------------------------------------------
# Profile sync (uses pynostr directly)
# ---------------------------------------------------------------------------

def _npub_from_pubkey(pubkey_obj) -> Optional[str]:
    for attr in ("bech32", "to_bech32", "npub", "to_npub"):
        fn = getattr(pubkey_obj, attr, None)
        if callable(fn):
            try:
                val = fn()
                if isinstance(val, str) and val.startswith("npub"):
                    return val
            except Exception:
                continue
    return None


def _event_get(ev, key: str):
    if isinstance(ev, dict):
        return ev.get(key)
    return getattr(ev, key, None)


def _extract_event_from_msg(msg):
    if msg is None:
        return None
    if isinstance(msg, dict) and "event" in msg:
        return msg.get("event")
    ev = getattr(msg, "event", None)
    if ev is not None:
        return ev
    return msg


def _fetch_latest_profile_events(relays: list[str], pubkey_hex: str, timeout_seconds: int):
    rm = RelayManager(timeout=timeout_seconds)
    relay_errors = 0
    for r in relays:
        try:
            rm.add_relay(r)
        except Exception:
            relay_errors += 1

    filters = FiltersList([Filters(authors=[pubkey_hex], kinds=[0, 10002])])

    try:
        if hasattr(rm, "add_subscription"):
            sub_id = f"pt2n-sync-{int(time.time() * 1000)}"
            sub = rm.add_subscription(sub_id)
            if hasattr(sub, "add_filters"):
                sub.add_filters(filters)
            elif hasattr(sub, "set_filters"):
                sub.set_filters(filters)
        elif hasattr(rm, "add_subscription_on_all_relays"):
            rm.add_subscription_on_all_relays("pt2n-sync", filters)
    except Exception:
        relay_errors += 1

    try:
        if hasattr(rm, "open_connections"):
            rm.open_connections()
    except Exception:
        relay_errors += 1

    latest: dict[int, object] = {}
    start = time.time()
    mp = getattr(rm, "message_pool", None)
    while time.time() - start < timeout_seconds:
        got = False
        if mp is not None and hasattr(mp, "has_events") and hasattr(mp, "get_event"):
            while mp.has_events():
                got = True
                msg = mp.get_event()
                ev = _extract_event_from_msg(msg)
                if ev is None:
                    continue
                kind = int(_event_get(ev, "kind") or 0)
                created_at = int(_event_get(ev, "created_at") or 0)
                if kind not in (0, 10002):
                    continue
                prev = latest.get(kind)
                prev_ts = int(_event_get(prev, "created_at") or 0) if prev else 0
                if created_at > prev_ts:
                    latest[kind] = ev
        elif hasattr(rm, "run_sync"):
            try:
                rm.run_sync()
            except Exception:
                relay_errors += 1
                break
        if not got:
            time.sleep(0.1)

    try:
        if hasattr(rm, "close_connections"):
            rm.close_connections()
    except Exception:
        relay_errors += 1

    return latest, relay_errors


def _parse_profile_content(ev) -> dict:
    content = _event_get(ev, "content") or ""
    if isinstance(content, str):
        try:
            return json.loads(content) if content else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _parse_nip65_relays(ev) -> list[dict]:
    tags = _event_get(ev, "tags") or []
    out: dict[str, dict] = {}
    for tag in tags:
        if not isinstance(tag, (list, tuple)) or len(tag) < 2:
            continue
        if tag[0] != "r":
            continue
        url = str(tag[1]).strip()
        if not url:
            continue
        markers = {str(x).lower() for x in tag[2:]}
        if "read" in markers or "write" in markers:
            read = "read" in markers
            write = "write" in markers
        else:
            read = True
            write = True
        if url not in out:
            out[url] = {"url": url, "read": read, "write": write}
        else:
            out[url]["read"] = out[url]["read"] or read
            out[url]["write"] = out[url]["write"] or write
    return list(out.values())


def _format_rw(read: bool, write: bool) -> str:
    if read and write:
        return "read/write"
    if read:
        return "read"
    if write:
        return "write"
    return "none"


def sync_profile(
    store: Store,
    n: UrlNormaliser,
    nsec_arg: Optional[str],
    relays_arg: Optional[str],
    import_relays: bool,
    enable_imported: bool,
    disable_missing: bool,
    timeout_seconds: int,
    log_fn=print,
) -> None:
    nsec = os.environ.get("NOSTR_NSEC") or nsec_arg or get_stored_nsec(store.db_path)
    if not nsec:
        raise SystemExit("Provide nsec via --nsec or NOSTR_NSEC.")

    if relays_arg and relays_arg.strip():
        relays = [n.normalise_relay_url(x.strip()) for x in relays_arg.split(",") if x.strip()]
    else:
        relays_env = os.environ.get("NOSTR_RELAYS")
        if relays_env and relays_env.strip():
            relays = [n.normalise_relay_url(x.strip()) for x in relays_env.split(",") if x.strip()]
        else:
            relays = store.get_enabled_relays() or DEFAULT_RELAYS

    priv = PrivateKey.from_nsec(nsec)
    pub = priv.public_key
    pub_hex = pub.hex()
    npub = _npub_from_pubkey(pub) or "-"

    log_fn(f"Pubkey: {pub_hex} | npub: {npub}")
    log_fn(f"Bootstrap relays: {', '.join(relays)}")

    latest, relay_errors = _fetch_latest_profile_events(relays, pub_hex, timeout_seconds)

    profile_ev = latest.get(0)
    relays_ev = latest.get(10002)

    if not profile_ev:
        log_fn("No profile metadata found.")
    else:
        meta = _parse_profile_content(profile_ev)
        name = (meta.get("name") or meta.get("display_name") or "").strip()
        display = (meta.get("display_name") or meta.get("displayName") or "").strip()
        nip05 = (meta.get("nip05") or "").strip()
        website = (meta.get("website") or meta.get("url") or "").strip()
        picture = (meta.get("picture") or "").strip()

        log_fn("Profile:")
        if name or display:
            log_fn(f"  name: {name or display}")
        if display and display != name:
            log_fn(f"  display_name: {display}")
        if nip05:
            log_fn(f"  nip05: {nip05}")
        if website:
            log_fn(f"  website: {website}")
        if picture:
            log_fn(f"  picture: {picture}")

    if not relays_ev:
        log_fn("No NIP-65 relay list found.")
        if relay_errors:
            log_fn(f"Relay errors: {relay_errors}")
        return

    nip65 = _parse_nip65_relays(relays_ev)
    if not nip65:
        log_fn("No NIP-65 relay list found.")
        if relay_errors:
            log_fn(f"Relay errors: {relay_errors}")
        return

    log_fn("NIP-65 relays:")
    for r in nip65:
        log_fn(f"  {r['url']} ({_format_rw(r['read'], r['write'])})")

    if relay_errors:
        log_fn(f"Relay errors: {relay_errors}")

    if import_relays:
        imported_norms: set[str] = set()
        import_errors = 0
        for r in nip65:
            try:
                norm = n.normalise_relay_url(r["url"])
            except Exception:
                import_errors += 1
                continue
            try:
                store.add_relay_with_enabled(norm, enabled=enable_imported)
                if not enable_imported:
                    store.set_relay_enabled(norm, False)
                imported_norms.add(norm)
            except Exception:
                import_errors += 1

        log_fn(f"Imported relays: {len(imported_norms)}")
        if import_errors:
            log_fn(f"Import errors: {import_errors}")

        if disable_missing:
            rows = store.list_relays()
            disabled = 0
            for (rid, _enabled, _url, url_norm, _last_used_ts, _last_error, _latency_ms) in rows:
                if not url_norm:
                    continue
                if url_norm not in imported_norms:
                    store.set_relay_enabled(str(rid), False)
                    disabled += 1
            log_fn(f"Disabled missing relays: {disabled}")


# ---------------------------------------------------------------------------
# Shared helpers for CLI + interactive
# ---------------------------------------------------------------------------

def _parse_sync_profile_args(args: list[str]):
    p = argparse.ArgumentParser(prog="sync-profile", add_help=False)
    p.add_argument("--nsec", default=None)
    p.add_argument("--relays", default=None)
    p.add_argument("--import-relays", action="store_true")
    p.add_argument("--enable-imported", action="store_true")
    p.add_argument("--disable-missing", action="store_true")
    p.add_argument("--timeout-seconds", type=int, default=8)
    try:
        return p.parse_args(args)
    except SystemExit:
        return "Usage: sync-profile [--relays a,b] [--nsec nsec] [--import-relays] [--enable-imported] [--disable-missing] [--timeout-seconds N]"


def _parse_edit_source_args(args: list[str]):
    p = argparse.ArgumentParser(prog="edit-source", add_help=False)
    p.add_argument("source_id")
    p.add_argument("--channel-url", dest="channel_url", default=None)
    p.add_argument("--rss-url", dest="rss_url", default=None)
    try:
        ns = p.parse_args(args)
    except SystemExit:
        return "Usage: edit-source <id> [--channel-url URL] [--rss-url URL]"
    if not ns.channel_url and not ns.rss_url:
        return "Provide --channel-url and/or --rss-url."
    return ns


def _parse_set_rate_args(args: list[str]):
    p = argparse.ArgumentParser(prog="set-rate", add_help=False)
    p.add_argument("--min-interval-seconds", type=int, default=None)
    p.add_argument("--max-posts-per-hour", type=int, default=None)
    p.add_argument("--max-posts-per-day-per-source", type=int, default=None)
    try:
        ns = p.parse_args(args)
    except SystemExit:
        return "Usage: set-rate [--min-interval-seconds N] [--max-posts-per-hour N] [--max-posts-per-day-per-source N]"
    if ns.min_interval_seconds is None and ns.max_posts_per_hour is None and ns.max_posts_per_day_per_source is None:
        return "Provide --min-interval-seconds and/or --max-posts-per-hour and/or --max-posts-per-day-per-source."
    return ns


def _maybe_add_url_as_source(store: Store, n: UrlNormaliser, url: str, log_fn) -> bool:
    raw = (url or "").strip()
    if not raw:
        return False
    try:
        n.extract_channel_ref(raw)
        sid = store.add_channel_source(raw)
        log_fn(f"Added channel source id={sid}")
        return True
    except Exception:
        pass
    try:
        rss_norm = n.normalise_feed_url(raw)
        if n.looks_like_peertube_feed(rss_norm):
            sid = store.add_rss_source(raw)
            log_fn(f"Added RSS source id={sid} (canonical: {rss_norm})")
            return True
    except Exception:
        return False
    return False


def _normalize_cmd(cmd: str) -> str:
    if not cmd:
        return cmd
    raw = cmd.strip().lower()
    if raw.startswith("/"):
        raw = raw[1:]
    return raw.rstrip(".:,;")


def _apply_edit_source(store: Store, n: UrlNormaliser, source_id: str, channel_url: Optional[str], rss_url: Optional[str], log_fn) -> None:
    try:
        sid = int(str(source_id).strip())
    except ValueError:
        log_fn("Invalid source id.")
        return
    updates = []
    if channel_url:
        if str(channel_url).strip().lower() in ("none", "null", "off", "-"):
            c = store.clear_source_channel(sid)
            if c:
                updates.append("channel cleared")
        else:
            c = store.set_source_channel(sid, channel_url)
            if c:
                updates.append("channel")
    if rss_url:
        if str(rss_url).strip().lower() in ("none", "null", "off", "-"):
            c = store.clear_source_rss(sid)
            if c:
                updates.append("rss cleared")
        else:
            rss_norm = n.normalise_feed_url(rss_url)
            if not n.looks_like_peertube_feed(rss_norm):
                log_fn("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
            c = store.set_source_rss(sid, rss_url)
            if c:
                updates.append("rss")
    if not updates:
        log_fn("Source not found.")
        return
    log_fn(f"Updated source {sid}: {', '.join(updates)}")
    _resync_source(store, n, sid, log_fn)


def _resync_source(store: Store, n: UrlNormaliser, source_id: int, log_fn) -> None:
    cleared = store.clear_pending_for_source(source_id)
    if cleared:
        log_fn(f"Cleared pending items: {cleared}")
    api_limit = int(os.environ.get("API_LIMIT_PER_SOURCE", "50"))
    lookback_days = int(os.environ.get("NEW_SOURCE_LOOKBACK_DAYS", "30"))
    runner = Runner(store, PeerTubeClient(n), NostrPublisher(), n, log_fn=log_fn)
    runner.ingest_source_once(source_id, api_limit, lookback_days)


def repair_db(store: Store, n: UrlNormaliser, log_fn) -> None:
    counts = {"relays": 0, "sources": 0, "videos": 0, "published_ts": 0}

    rows = store.conn.execute("SELECT id, relay_url FROM relays").fetchall()
    for rid, relay_url in rows:
        try:
            norm = n.normalise_relay_url(relay_url)
        except Exception:
            continue
        store.conn.execute("UPDATE relays SET relay_url_norm=? WHERE id=?", (norm, rid))
        counts["relays"] += 1

    rows = store.conn.execute("SELECT id, api_base, api_channel_url, rss_url FROM sources").fetchall()
    for sid, api_base, api_channel_url, rss_url in rows:
        if api_base:
            try:
                base_norm = n.normalise_http_url(api_base)
                store.conn.execute("UPDATE sources SET api_base_norm=? WHERE id=?", (base_norm, sid))
            except Exception:
                pass
        if api_channel_url:
            try:
                chan_norm = n.normalise_http_url(api_channel_url)
                store.conn.execute("UPDATE sources SET api_channel_url_norm=? WHERE id=?", (chan_norm, sid))
            except Exception:
                pass
        if rss_url:
            try:
                rss_norm = n.normalise_feed_url(rss_url)
                store.conn.execute("UPDATE sources SET rss_url_norm=? WHERE id=?", (rss_norm, sid))
            except Exception:
                pass
        counts["sources"] += 1

    rows = store.conn.execute(
        "SELECT id, watch_url, published_ts, first_seen_ts FROM videos"
    ).fetchall()
    for vid, watch_url, published_ts, first_seen_ts in rows:
        try:
            norm = n.normalise_watch_url(watch_url)
            store.conn.execute("UPDATE videos SET watch_url_norm=? WHERE id=?", (norm, vid))
            counts["videos"] += 1
        except Exception:
            pass
        if published_ts is None and first_seen_ts is not None:
            store.conn.execute("UPDATE videos SET published_ts=? WHERE id=?", (int(first_seen_ts), vid))
            counts["published_ts"] += 1

    store.conn.commit()
    log_fn(
        "Repair complete: "
        f"relays={counts['relays']} sources={counts['sources']} "
        f"videos_normed={counts['videos']} published_ts_filled={counts['published_ts']}"
    )


# ---------------------------------------------------------------------------
# Interactive mode (TUI or shell)
# ---------------------------------------------------------------------------

def _run_interactive(
    args: argparse.Namespace,
    n: UrlNormaliser,
    nsec_env: Optional[str],
    relays: Optional[list[str]],
    retry: Optional[int],
) -> None:
    stop_event = threading.Event()
    log_queue: Optional[Queue] = Queue() if App is not None else None

    def _log_fn(msg: str) -> None:
        if log_queue is not None:
            log_queue.put(msg)
        else:
            print(msg)

    def _status_fn(msg: str) -> None:
        _set_runtime_status(msg)
        if log_queue is None:
            return

    def _runner_thread() -> None:
        thread_store = Store(args.db, n)
        thread_store.init_schema()
        try:
            thread_runner = Runner(
                thread_store,
                PeerTubeClient(n),
                NostrPublisher(),
                n,
                log_fn=_log_fn if log_queue else None,
                status_fn=_status_fn,
            )
            thread_runner.run(
                nsec=nsec_env,
                relays=relays,
                poll_seconds=args.poll_seconds,
                publish_interval_seconds=args.publish_interval_seconds,
                retry_failed_after_seconds=retry,
                api_limit_per_source=args.api_limit_per_source,
                new_source_lookback_days=args.new_source_lookback_days,
                stop_event=stop_event,
            )
        finally:
            thread_store.close()

    t = threading.Thread(target=_runner_thread, daemon=True)
    t.start()

    if App is not None and log_queue is not None:
        _interactive_tui(args.db, n, stop_event, log_queue)
    else:
        _interactive_shell(args.db, n, stop_event)
    t.join()


def _interactive_tui(db_path: str, n: UrlNormaliser, stop_event: threading.Event, log_queue: Queue) -> None:
    class PeerTubeTUI(App):
        CSS = """
        Screen { layout: vertical; }
        #body { height: 1fr; }
        #log { height: 1fr; }
        #status { height: 1; display: none; }
        #input_row { height: 3; }
        #prompt { width: 6; content-align: right middle; color: #aaaaaa; }
        #input { height: 3; border: round #4c9aff; }
        #palette { height: 6; border: round #666666; display: none; }
        #panels { height: auto; }
        #panel_counts { height: auto; }
        #panel_activity { height: auto; }
        #panel_rate { height: auto; }
        #panel_queue { height: 10; }
        #queue_title { height: 1; }
        #queue_list { height: 1fr; }
        .panel {
            border: round #666666;
            padding: 0 1;
            width: 1fr;
        }
        #panel_counts { border: round #4c9aff; }
        #panel_activity { border: round #67b26f; }
        #panel_rate { border: round #ffb347; }
        #panel_queue { border: round #b39ddb; }
        #status { background: #222222; color: #b0bec5; }
        """
        BINDINGS = [
            ("/", "palette", "Commands"),
            ("?", "help", "Help"),
            ("tab", "complete", "Complete"),
            ("down", "palette_down", "Next"),
            ("up", "palette_up", "Prev"),
            ("d", "toggle_dashboard", "Dashboard"),
            ("pageup", "queue_up", "Queue Up"),
            ("pagedown", "queue_down", "Queue Down"),
            ("ctrl+l", "clear", "Clear"),
            ("ctrl+c", "quit", "Quit"),
        ]
        TAB_FOCUS_NEXT = False
        ENABLE_TAB_FOCUS = False

        def __init__(self) -> None:
            super().__init__()
            self.store = Store(db_path, n)
            self.store.init_schema()
            self._wizard_queue: list[tuple[str, callable, bool]] = []
            self._wizard_active = False
            self._pending_secret = False
            self._palette_visible = False
            self._commands = _interactive_commands()
            self._palette_map: dict[str, str] = {}
            self._palette_gen = 0
            self._palette_force = False
            self._pending_cmd: Optional[str] = None
            self._pending_args: list[str] = []
            self._pending_prompts: list[tuple[str, bool]] = []
            self._pending_allow_blank = False
            self._last_prompt = ""
            self._pending_edit_choice = ""
            self._palette_mode = "commands"
            self._dashboard_visible = True
            self._queue_cache: list[str] = []

        class CommandInput(Input):
            def key_tab(self) -> None:
                self.app.action_complete()
                self.focus()

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            with Vertical(id="body"):
                with Horizontal(id="panels"):
                    yield Static(id="panel_counts", classes="panel")
                    yield Static(id="panel_activity", classes="panel")
                    yield Static(id="panel_rate", classes="panel")
                with Vertical(id="panel_queue", classes="panel"):
                    yield Static("Next Posts", id="queue_title")
                    yield ListView(id="queue_list")
                yield RichLog(id="log", wrap=True, highlight=True, markup=False)
                yield Static(id="status")
                with Horizontal(id="input_row"):
                    yield Static("cmd>", id="prompt")
                    yield self.CommandInput(id="input", placeholder="Type / for commands")
                yield ListView(id="palette")
            yield Footer()

        def on_mount(self) -> None:
            self.set_interval(0.25, self._drain_logs)
            self.set_interval(1.0, self._refresh_status)
            self._log("== PeerTube2Nostr Interactive ==")
            self._log("Type '/' for commands. 'quit' to exit.")
            self._start_wizard_if_needed()
            self._apply_dashboard_visibility()

        def on_unmount(self) -> None:
            stop_event.set()
            self.store.close()

        def _log(self, msg: str) -> None:
            self.query_one("#log", RichLog).write(msg)

        def _emit_help(self) -> None:
            for line in _help_lines():
                self._log(line)

        def _drain_logs(self) -> None:
            while True:
                try:
                    msg = log_queue.get_nowait()
                except Empty:
                    break
                self._log(msg)

        def _refresh_status(self) -> None:
            if self._dashboard_visible:
                panels = _format_dashboard_panels(self.store, db_path)
                self.query_one("#panel_counts", Static).update(panels["counts"])
                self.query_one("#panel_activity", Static).update(panels["activity"])
                self.query_one("#panel_rate", Static).update(panels["rate"])
                self._update_queue_list(panels["queue"])

        def action_help(self) -> None:
            self._emit_help()

        def action_palette(self) -> None:
            inp = self.query_one("#input", Input)
            if not inp.value:
                inp.value = "/"
            inp.focus()
            self._palette_mode = "commands"
            self._show_palette(True)
            self._update_palette(inp.value)

        def action_toggle_dashboard(self) -> None:
            self._dashboard_visible = not self._dashboard_visible
            self._apply_dashboard_visibility()

        def action_complete(self) -> None:
            inp = self.query_one("#input", Input)
            text = inp.value
            matches = self._palette_matches(text)
            if not matches:
                return
            choice = matches[0]
            inp.value = choice + " "
            inp.focus()
            self._show_palette(False)
            self._palette_force = True

        def action_palette_down(self) -> None:
            if not self._palette_visible:
                self.action_palette()
            palette = self.query_one("#palette", ListView)
            palette.focus()
            palette.action_cursor_down()

        def action_palette_up(self) -> None:
            if not self._palette_visible:
                self.action_palette()
            palette = self.query_one("#palette", ListView)
            palette.focus()
            palette.action_cursor_up()

        def action_queue_up(self) -> None:
            if not self._dashboard_visible:
                return
            queue = self.query_one("#queue_list", ListView)
            queue.focus()
            for _ in range(5):
                queue.action_cursor_up()

        def action_queue_down(self) -> None:
            if not self._dashboard_visible:
                return
            queue = self.query_one("#queue_list", ListView)
            queue.focus()
            for _ in range(5):
                queue.action_cursor_down()

        def action_clear(self) -> None:
            self.query_one("#log", RichLog).clear()

        def action_quit(self) -> None:
            stop_event.set()
            self.exit()

        def on_input_changed(self, event: Input.Changed) -> None:
            if self._wizard_active:
                return
            if getattr(self, "_palette_force", False):
                self._palette_force = False
                return
            text = event.value
            if self._palette_mode != "commands":
                return
            if text.startswith("/") or self._palette_visible:
                self._show_palette(True)
                self._update_palette(text)
            else:
                self._show_palette(False)

        def on_input_submitted(self, event: Input.Submitted) -> None:
            line = event.value.strip()
            event.input.value = ""
            if not line:
                return

            if self._wizard_active:
                self._handle_wizard_input(line)
                return

            pending_result = self._handle_pending_input(line, event.input)
            if pending_result is None:
                return
            cmd, args = pending_result

            if cmd is None:
                new_result = self._handle_new_command_input(line, event.input)
                if new_result is None:
                    return
                cmd, args = new_result

            should_quit = _dispatch_command(self.store, n, db_path, cmd, args, self._log)
            if should_quit:
                self.action_quit()

        def _handle_pending_input(self, line: str, inp: Input):
            if self._pending_secret:
                self._pending_secret = False
                inp.password = False
                inp.placeholder = "Type / for commands"
                return "set-nsec", [line]
            if not self._pending_cmd:
                return None, []
            if line.lower() in ("cancel", "exit"):
                self._reset_pending(inp, canceled=True)
                return None
            if not line and not self._pending_allow_blank:
                self._reset_pending(inp, canceled=True)
                return None
            self._pending_args.append(line)
            if self._pending_prompts:
                self._prompt_next_pending(inp)
                return None
            cmd = self._pending_cmd
            args = self._pending_args
            if cmd == "edit-source":
                if len(args) == 1 and not self._pending_edit_choice:
                    self._pending_prompts = [("Change what? (channel/rss/both)", False)]
                    self._prompt_next_pending(inp)
                    return None
                if self._pending_edit_choice == "":
                    self._pending_edit_choice = args[1].strip().lower() if len(args) > 1 else ""
                    if self._pending_edit_choice not in ("channel", "rss", "both"):
                        self._log("Choose: channel, rss, or both.")
                        self._pending_args = [args[0]]
                        self._pending_prompts = [("Change what? (channel/rss/both)", False)]
                        self._prompt_next_pending(inp)
                        return None
                    prompts = []
                    if self._pending_edit_choice in ("channel", "both"):
                        prompts.append(("Channel URL (blank to skip, 'none' to clear)", True))
                    if self._pending_edit_choice in ("rss", "both"):
                        prompts.append(("RSS URL (blank to skip, 'none' to clear)", True))
                    self._pending_prompts = prompts
                    self._pending_args = [args[0], self._pending_edit_choice]
                    self._prompt_next_pending(inp)
                    return None
                source_id = args[0] if len(args) > 0 else ""
                choice = args[1] if len(args) > 1 else ""
                channel_url = None
                rss_url = None
                cursor = 2
                if choice in ("channel", "both"):
                    channel_url = args[cursor] if len(args) > cursor and args[cursor] else None
                    cursor += 1
                if choice in ("rss", "both"):
                    rss_url = args[cursor] if len(args) > cursor and args[cursor] else None
                _apply_edit_source(self.store, n, source_id, channel_url, rss_url, self._log)
                self._reset_pending(inp)
                return None
            if cmd == "add-channel":
                self._wiz_add_channel(args[0] if args else "")
                self._reset_pending(inp)
                return None
            if cmd == "add-rss":
                if not _maybe_add_url_as_source(self.store, n, args[0] if args else "", self._log):
                    self._log("Invalid RSS URL.")
                self._reset_pending(inp)
                return None
            if cmd == "add-source":
                self._wiz_add_source(args[0] if args else "")
                self._reset_pending(inp)
                return None
            if cmd == "set-rate":
                min_int = args[0] if len(args) > 0 else ""
                max_per = args[1] if len(args) > 1 else ""
                max_day = args[2] if len(args) > 2 else ""
                parsed = _parse_set_rate_args(
                    ([f"--min-interval-seconds={min_int}"] if min_int else [])
                    + ([f"--max-posts-per-hour={max_per}"] if max_per else [])
                    + ([f"--max-posts-per-day-per-source={max_day}"] if max_day else [])
                )
                if isinstance(parsed, str):
                    self._log(parsed)
                else:
                    if parsed.min_interval_seconds is not None:
                        self.store.set_setting("min_publish_interval_seconds", str(int(parsed.min_interval_seconds)))
                    if parsed.max_posts_per_hour is not None:
                        self.store.set_setting("max_posts_per_hour", str(int(parsed.max_posts_per_hour)))
                    if parsed.max_posts_per_day_per_source is not None:
                        self.store.set_setting(
                            "max_posts_per_day_per_source",
                            str(int(parsed.max_posts_per_day_per_source)),
                        )
                    min_interval, max_per_hour = self.store.get_publish_limits()
                    max_per_day_per_source = self.store.get_daily_source_limit()
                    self._log(
                        "Rate limits: "
                        f"min_interval_seconds={min_interval}, "
                        f"max_posts_per_hour={max_per_hour}, "
                        f"max_posts_per_day_per_source={max_per_day_per_source}"
                    )
                self._reset_pending(inp)
                return None
            if cmd == "resync-source":
                source_id = args[0] if args else ""
                if not source_id:
                    self._log("Canceled.")
                else:
                    _resync_source(self.store, n, int(source_id), self._log)
                self._reset_pending(inp)
                return None
            if cmd == "retry-failed":
                if args:
                    count = self.store.retry_failed_for_source(int(args[0]), older_than_seconds=0)
                    self._log(f"Re-queued failed items for source {args[0]}: {count}")
                else:
                    count = self.store.retry_failed(older_than_seconds=0)
                    self._log(f"Re-queued failed items: {count}")
                self._reset_pending(inp)
                return None
            self._reset_pending(inp)
            return cmd, args

        def _handle_new_command_input(self, line: str, inp: Input):
            if line.startswith(("http://", "https://")) and self._last_prompt:
                lp = self._last_prompt.lower()
                if "channel or rss" in lp:
                    self._wiz_add_source(line)
                    return None
                if "channel url" in lp:
                    self._wiz_add_channel(line)
                    return None
                if "rss url" in lp:
                    if not _maybe_add_url_as_source(self.store, n, line, self._log):
                        self._log("Invalid RSS URL.")
                    return None

            parts = shlex.split(line)
            cmd = _normalize_cmd(parts[0])
            args = parts[1:]

            if cmd not in self._commands and not args and line.startswith(("http://", "https://")):
                if _maybe_add_url_as_source(self.store, n, line.strip(), self._log):
                    return None

            if cmd == "set-nsec" and not args:
                self._pending_secret = True
                inp.password = True
                inp.placeholder = "Enter nsec:"
                self._log("Enter nsec:")
                return None

            if cmd == "/":
                self._show_palette(True)
                self._update_palette("/")
                return None

            arg_prompts = _interactive_arg_prompts()
            if cmd in arg_prompts and len(args) < len(arg_prompts[cmd]):
                self._pending_cmd = cmd
                self._pending_args = args
                self._pending_prompts = [(p, False) for p in arg_prompts[cmd][len(args):]]
                self._prompt_next_pending(inp)
                return None
            if cmd == "edit-source" and len(args) < 1:
                self._pending_cmd = cmd
                self._pending_args = args
                prompts = [("Source id", False)]
                self._pending_prompts = prompts[len(args):]
                self._prompt_next_pending(inp)
                return None
            if cmd == "set-rate" and len(args) < 3:
                self._pending_cmd = cmd
                self._pending_args = args
                prompts = [
                    ("Min interval seconds", False),
                    ("Max posts per hour", False),
                    ("Max posts per day per source", False),
                ]
                self._pending_prompts = prompts[len(args):]
                self._prompt_next_pending(inp)
                return None
            if cmd == "resync-source" and len(args) < 1:
                self._pending_cmd = cmd
                self._pending_args = args
                prompts = [("Source id", False)]
                self._pending_prompts = prompts[len(args):]
                self._prompt_next_pending(inp)
                return None
            if cmd == "repair-db":
                repair_db(self.store, n, self._log)
                return None
            if cmd == "retry-failed" and not args:
                self._pending_cmd = cmd
                self._pending_args = args
                prompts = [("Source id (blank for all)", True)]
                self._pending_prompts = prompts[len(args):]
                self._prompt_next_pending(inp)
                return None

            return cmd, args

        def on_list_view_selected(self, event: ListView.Selected) -> None:
            if not self._palette_visible:
                return
            value = ""
            item_id = event.item.id or ""
            if item_id in self._palette_map:
                value = self._palette_map[item_id]
            if not value:
                label = event.item.query_one(Label)
                if hasattr(label, "text"):
                    value = str(label.text).strip()
                else:
                    value = str(label).strip()
            inp = self.query_one("#input", Input)
            if value:
                inp.value = value + " "
            inp.focus()
            self._show_palette(False)
            self._palette_mode = "commands"

        def _start_wizard_if_needed(self) -> None:
            has_sources = self.store.count_sources() > 0
            has_nsec = bool(get_stored_nsec(db_path))

            if self.store.count_relays() == 0:
                self._wizard_queue.append(("Seed default relays? [Y/n]:", self._wiz_seed_relays, False))
            if not has_nsec:
                self._wizard_queue.append(("Set nsec now? [Y/n]:", self._wiz_ask_nsec, False))
            if not has_sources:
                self._wizard_queue.append(("Add PeerTube channel URL (blank to skip):", self._wiz_add_channel, False))

            if self._wizard_queue:
                self._wizard_active = True
                self._advance_wizard()

        def _show_palette(self, show: bool) -> None:
            self._palette_visible = show
            palette = self.query_one("#palette", ListView)
            palette.styles.display = "block" if show else "none"

        def _palette_matches(self, query: str) -> list[str]:
            q = query.lstrip("/").strip().lower()
            matches = []
            for cmd in self._commands:
                if not q:
                    matches.append(cmd)
                elif cmd.startswith(q) or q in cmd:
                    matches.append(cmd)
            return matches

        def _update_palette(self, query: str) -> None:
            if not self._palette_visible:
                return
            if self._palette_mode != "commands":
                return
            matches = self._palette_matches(query)
            palette = self.query_one("#palette", ListView)
            palette.clear()
            self._palette_gen += 1
            self._palette_map = {}
            for i, cmd in enumerate(matches[:50]):
                item_id = f"cmd_{self._palette_gen}_{i}"
                self._palette_map[item_id] = cmd
                palette.append(ListItem(Label(cmd), id=item_id))

        def _set_palette_items(self, items: list[tuple[str, str]], mode: str) -> None:
            self._palette_mode = mode
            palette = self.query_one("#palette", ListView)
            palette.clear()
            self._palette_gen += 1
            self._palette_map = {}
            for i, (label, value) in enumerate(items[:50]):
                item_id = f"pick_{self._palette_gen}_{i}"
                self._palette_map[item_id] = value
                palette.append(ListItem(Label(label), id=item_id))
            self._show_palette(True)

        def _apply_dashboard_visibility(self) -> None:
            panels = self.query_one("#panels", Horizontal)
            queue = self.query_one("#panel_queue", Vertical)
            if self._dashboard_visible:
                panels.styles.display = "block"
                queue.styles.display = "block"
            else:
                panels.styles.display = "none"
                queue.styles.display = "none"

        def _update_queue_list(self, items: list[str]) -> None:
            if items == self._queue_cache:
                return
            self._queue_cache = list(items)
            lv = self.query_one("#queue_list", ListView)
            lv.clear()
            for text in items:
                lv.append(ListItem(Label(text)))

        def _advance_wizard(self) -> None:
            if not self._wizard_queue:
                self._wizard_active = False
                inp = self.query_one("#input", Input)
                inp.password = False
                inp.placeholder = "Type / for commands"
                self._log("Wizard complete.")
                return
            prompt, _handler, password = self._wizard_queue[0]
            inp = self.query_one("#input", Input)
            inp.password = password
            inp.placeholder = prompt
            self._log(prompt)

        def _prompt_next_pending(self, inp: Input) -> None:
            if not self._pending_prompts:
                return
            prompt, allow_blank = self._pending_prompts.pop(0)
            self._last_prompt = prompt
            self._pending_allow_blank = allow_blank
            inp.placeholder = prompt
            self._log(prompt)
            pl = prompt.lower()
            if "source id" in pl:
                rows = self.store.list_sources()
                items = []
                for (sid, _enabled, api_base, api_channel, _api_channel_url, rss_url, _lookback_days, _last_polled_ts, _last_error) in rows:
                    label = f"{sid}: {api_base or ''} {api_channel or ''}".strip()
                    if rss_url:
                        label = f"{label} | rss"
                    items.append((label, str(sid)))
                if items:
                    self._set_palette_items(items, "sources")
            elif "relay id" in pl:
                rows = self.store.list_relays()
                items = []
                for (rid, _enabled, url, _url_norm, _last_used_ts, _last_error, _latency_ms) in rows:
                    items.append((f"{rid}: {url}", str(rid)))
                if items:
                    self._set_palette_items(items, "relays")

        def _reset_pending(self, inp: Input, canceled: bool = False) -> None:
            self._pending_cmd = None
            self._pending_args = []
            self._pending_prompts = []
            self._pending_allow_blank = False
            self._last_prompt = ""
            self._pending_edit_choice = ""
            inp.placeholder = "Type / for commands"
            if canceled:
                self._log("Canceled.")

        def _handle_wizard_input(self, value: str) -> None:
            prompt, handler, _password = self._wizard_queue.pop(0)
            new_steps = handler(value.strip()) or []
            if new_steps:
                self._wizard_queue = new_steps + self._wizard_queue
            self._advance_wizard()

        def _wiz_seed_relays(self, value: str) -> list[tuple[str, callable, bool]]:
            ans = value.lower()
            if ans in ("", "y", "yes"):
                self.store.seed_default_relays_if_empty()
                self._log("Seeded default relays.")
            return []

        def _wiz_ask_nsec(self, value: str) -> list[tuple[str, callable, bool]]:
            ans = value.lower()
            if ans in ("", "y", "yes"):
                return [("Enter nsec:", self._wiz_set_nsec, True)]
            return []

        def _wiz_set_nsec(self, value: str) -> list[tuple[str, callable, bool]]:
            if not value:
                self._log("nsec cannot be empty.")
                return []
            store_type, path = set_stored_nsec(db_path, value)
            if store_type == "keyring":
                self._log("Stored nsec in OS keyring for this DB path.")
            else:
                self._log(f"Stored nsec in file: {path}")
            return []

        def _wiz_add_channel(self, value: str) -> list[tuple[str, callable, bool]]:
            if not value:
                return []
            try:
                sid = self.store.add_channel_source(value)
                self._log(f"Added channel source id={sid}")
            except Exception as ex:
                self._log(f"Failed to add channel: {ex}")
                return []
            return [("Add RSS fallback URL (blank to skip):", lambda v: self._wiz_set_rss(v, sid), False)]

        def _wiz_set_rss(self, value: str, sid: int) -> list[tuple[str, callable, bool]]:
            if not value:
                return []
            try:
                rss_norm = n.normalise_feed_url(value)
                if not n.looks_like_peertube_feed(rss_norm):
                    self._log("Warning: RSS URL does not look like a typical PeerTube feed (still setting).")
                self.store.set_source_rss(sid, value)
                self._log(f"Set RSS fallback for source {sid} (canonical: {rss_norm})")
            except Exception as ex:
                self._log(f"Failed to set RSS: {ex}")
            return []

        def _wiz_add_source(self, value: str) -> list[tuple[str, callable, bool]]:
            if not value:
                return []
            try:
                sid = self.store.add_channel_source(value)
                self._log(f"Added channel source id={sid}")
                return [("Add RSS fallback URL (blank to skip):", lambda v: self._wiz_set_rss(v, sid), False)]
            except Exception:
                pass
            try:
                rss_norm = n.normalise_feed_url(value)
                if not n.looks_like_peertube_feed(rss_norm):
                    self._log("Warning: RSS URL does not look like a typical PeerTube feed (still adding).")
                sid = self.store.add_rss_source(value)
                self._log(f"Added RSS source id={sid} (canonical: {rss_norm})")
            except Exception as ex:
                self._log(f"Failed to add source: {ex}")
            return []

    PeerTubeTUI().run()


if __name__ == "__main__":
    main()
