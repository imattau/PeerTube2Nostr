"""Relay selection and signed-event validation for publishing endpoints."""

import time
from typing import Optional

from pynostr.event import Event

from core.database import Store
from core.utils import RELAY_POLICY_LOCKED, UrlNormaliser, get_relay_policy

MAX_PUBLISH_RELAYS = 20
ALLOWED_EVENT_KINDS = {1, 21, 22}
MAX_CLOCK_SKEW_SECONDS = 600


class RelaySelectionError(ValueError):
    """Raised when a requested relay selection cannot be used."""


class SignedEventError(ValueError):
    """Raised when a client-supplied signed event is not acceptable."""


def resolve_publish_relays(store: Store, requested: Optional[list[str]] = None) -> list[str]:
    """Return the normalised relay list to publish to.

    ``requested`` comes from the browser (per-user selection). It must be a
    subset of the admin defaults when the server policy is ``locked``.
    """
    normaliser = UrlNormaliser()
    policy = get_relay_policy()

    requested = [str(r).strip() for r in (requested or []) if str(r or "").strip()]
    if requested:
        candidates = requested
    elif policy == RELAY_POLICY_LOCKED:
        candidates = store.get_default_relays()
    else:
        candidates = list(store.get_enabled_relays())

    out: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        try:
            norm = normaliser.normalise_relay_url(raw)
        except Exception:
            if requested:
                raise RelaySelectionError(f"Invalid relay URL: {raw}")
            continue
        if norm not in seen:
            seen.add(norm)
            out.append(norm)

    if not out:
        raise RelaySelectionError("No relays available to publish to")
    if len(out) > MAX_PUBLISH_RELAYS:
        raise RelaySelectionError(f"At most {MAX_PUBLISH_RELAYS} relays can be used per post")

    if policy == RELAY_POLICY_LOCKED:
        allowed = set(store.get_default_relays())
        blocked = [r for r in out if r not in allowed]
        if blocked:
            raise RelaySelectionError(
                "This server only allows publishing to its default relays: "
                + ", ".join(blocked)
            )

    return out


def verify_signed_event(event_json: dict) -> Event:
    """Validate a client-signed event before republishing it.

    This endpoint is protected by the API key, but the server must not become
    an open relay for arbitrary events.
    """
    if not isinstance(event_json, dict):
        raise SignedEventError("Event must be a JSON object")

    for field in ("id", "pubkey", "sig", "kind", "created_at"):
        if event_json.get(field) in (None, ""):
            raise SignedEventError(f"Signed event is missing '{field}'")

    try:
        ev = Event.from_dict(event_json)
    except Exception as ex:
        raise SignedEventError(f"Could not parse signed event: {ex}") from ex

    try:
        kind = int(ev.kind)
    except Exception:
        raise SignedEventError("Event kind is not a number") from None
    if kind not in ALLOWED_EVENT_KINDS:
        raise SignedEventError(f"Event kind {kind} is not allowed")

    created_at = int(ev.created_at or 0)
    if abs(int(time.time()) - created_at) > MAX_CLOCK_SKEW_SECONDS:
        raise SignedEventError("Event timestamp is too far from the server clock")

    try:
        valid = ev.verify()
    except Exception:
        valid = False
    if not valid:
        raise SignedEventError("Event signature is invalid")

    return ev
