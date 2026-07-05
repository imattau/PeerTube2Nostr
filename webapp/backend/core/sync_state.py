import json, os, hmac, hashlib, base64, struct, math
from typing import Optional

from pynostr.event import Event
from pynostr.filters import Filters, FiltersList
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager

SYNC_KIND = 30000
SYNC_DTAG = "pt2n:sync"
SYNC_VERSION = 1


# ── NIP-44 ──────────────────────────────────────────────────────────────

def calc_padded_len(unpadded_len: int) -> int:
    if unpadded_len <= 32:
        return 32
    next_power = 1 << (math.floor(math.log2(unpadded_len - 1)) + 1)
    chunk = 32 if next_power <= 256 else next_power // 8
    return chunk * (math.floor((unpadded_len - 1) / chunk) + 1)


def pad(plaintext: str) -> bytes:
    unpadded = plaintext.encode("utf-8")
    unpadded_len = len(unpadded)
    if unpadded_len < 1 or unpadded_len > 4294967295:
        raise ValueError("invalid plaintext length")
    if unpadded_len >= 65536:
        prefix = b"\x00\x00" + struct.pack(">I", unpadded_len)
    else:
        prefix = struct.pack(">H", unpadded_len)
    padded_len = calc_padded_len(unpadded_len)
    return prefix + unpadded + b"\x00" * (padded_len - unpadded_len)


def unpad(padded: bytes) -> str:
    first_two = struct.unpack(">H", padded[0:2])[0]
    if first_two == 0:
        unpadded_len = struct.unpack(">I", padded[2:6])[0]
        prefix_len = 6
    else:
        unpadded_len = first_two
        prefix_len = 2
    unpadded = padded[prefix_len:prefix_len + unpadded_len]
    if unpadded_len == 0 or len(unpadded) != unpadded_len or len(padded) != prefix_len + calc_padded_len(unpadded_len):
        raise ValueError("invalid padding")
    return unpadded.decode("utf-8")


def conversation_key(private_key_hex: str, public_key_hex: str) -> bytes:
    priv = PrivateKey.from_hex(private_key_hex)
    shared_x = priv.ecdh(public_key_hex)  # raw 32-byte x coordinate (unhashed)
    return hmac.new(key=b"nip44-v2", msg=shared_x, digestmod=hashlib.sha256).digest()


def message_keys(conv_key: bytes, nonce: bytes) -> tuple[bytes, bytes, bytes]:
    okm = _hkdf_expand(conv_key, nonce, 76)
    return okm[0:32], okm[32:44], okm[44:76]  # chacha_key, chacha_nonce, hmac_key


def _hkdf_expand(prk: bytes, info: bytes, length: int) -> bytes:
    result = b""
    t = b""
    i = 1
    while len(result) < length:
        t = hmac.new(prk, t + info + bytes([i]), hashlib.sha256).digest()
        result += t
        i += 1
    return result[:length]


def encrypt_nip44(plaintext: str, conv_key: bytes) -> str:
    nonce = os.urandom(32)
    chacha_key, chacha_nonce, hmac_key = message_keys(conv_key, nonce)
    padded = pad(plaintext)
    ciphertext = chacha20_encrypt(chacha_key, chacha_nonce, padded)
    mac = hmac.new(hmac_key, nonce + ciphertext, hashlib.sha256).digest()
    payload = bytes([2]) + nonce + ciphertext + mac
    return base64.b64encode(payload).decode()


def decrypt_nip44(payload: str, conv_key: bytes) -> str:
    data = base64.b64decode(payload)
    if data[0] != 2:
        raise ValueError(f"unknown version {data[0]}")
    nonce = data[1:33]
    ciphertext = data[33:-32]
    mac = data[-32:]
    chacha_key, chacha_nonce, hmac_key = message_keys(conv_key, nonce)
    expected_mac = hmac.new(hmac_key, nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(expected_mac, mac):
        raise ValueError("invalid MAC")
    padded = chacha20_encrypt(chacha_key, chacha_nonce, ciphertext)  # ChaCha20 decrypt = encrypt (xor)
    return unpad(padded)


def chacha20_encrypt(key: bytes, nonce12: bytes, data: bytes) -> bytes:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms
    counter_nonce = b"\x00\x00\x00\x00" + nonce12  # RFC 8439: 4-byte counter + 12-byte nonce
    cipher = Cipher(algorithms.ChaCha20(key, counter_nonce), mode=None)
    return cipher.encryptor().update(data)


# ── NIP-59 Gift Wrap ────────────────────────────────────────────────────

def _conv_key_for_pair(priv_hex: str, pub_hex: str) -> bytes:
    return conversation_key(priv_hex, pub_hex)


def _sign_event(ev: Event, priv_hex: str) -> None:
    ev.sign(priv_hex)


def build_rumor(data: dict, pubkey_hex: str) -> Event:
    """Create an unsigned rumor event."""
    content = json.dumps(data, default=str)
    ev = Event(
        kind=SYNC_KIND,
        pubkey=pubkey_hex,
        content=content,
        tags=[["d", SYNC_DTAG]],
    )
    return ev


def build_seal(rumor: Event, sender_priv_hex: str, sender_pub_hex: str) -> Event:
    """Wrap a rumor in a kind-13 seal."""
    conv_key = _conv_key_for_pair(sender_priv_hex, sender_pub_hex)
    rumor_json = json.dumps(rumor.to_dict(), default=str)
    encrypted = encrypt_nip44(rumor_json, conv_key)
    ev = Event(
        kind=13,
        pubkey=sender_pub_hex,
        content=encrypted,
        tags=[],
    )
    _sign_event(ev, sender_priv_hex)
    return ev


def build_gift_wrap(seal: Event, recipient_pub_hex: str) -> Event:
    """Wrap a seal in a kind-1059 gift wrap with a random ephemeral key."""
    random_priv = PrivateKey()
    random_pub_hex = random_priv.public_key.hex()
    random_priv_hex = random_priv.hex()
    conv_key = _conv_key_for_pair(random_priv_hex, recipient_pub_hex)
    seal_json = json.dumps(seal.to_dict(), default=str)
    encrypted = encrypt_nip44(seal_json, conv_key)
    ev = Event(
        kind=1059,
        pubkey=random_pub_hex,
        content=encrypted,
        tags=[["p", recipient_pub_hex]],
    )
    _sign_event(ev, random_priv_hex)
    return ev


def unwrap_gift_wrap(wrap_event: Event, recipient_priv_hex: str) -> Optional[Event]:
    """Decrypt a gift wrap event to reveal the seal."""
    wrap_pub_hex = wrap_event.pubkey
    conv_key = _conv_key_for_pair(recipient_priv_hex, wrap_pub_hex)
    try:
        seal_json = decrypt_nip44(wrap_event.content, conv_key)
        seal_dict = json.loads(seal_json)
        return Event.from_dict(seal_dict)
    except Exception:
        return None


def unseal(seal_event: Event, recipient_priv_hex: str) -> Optional[Event]:
    """Decrypt a seal event to reveal the rumor."""
    seal_pub_hex = seal_event.pubkey
    conv_key = _conv_key_for_pair(recipient_priv_hex, seal_pub_hex)
    try:
        rumor_json = decrypt_nip44(seal_event.content, conv_key)
        rumor_dict = json.loads(rumor_json)
        return Event.from_dict(rumor_dict)
    except Exception:
        return None


# ── StateSyncer ─────────────────────────────────────────────────────────

class StateSyncer:
    def __init__(self, store, nsec: str, relays: list[str]):
        self.store = store
        self.nsec = nsec
        self.relays = relays
        self._priv: Optional[PrivateKey] = None
        self._pub_hex: Optional[str] = None
        self._priv_hex: Optional[str] = None

    def _ensure_key(self) -> None:
        if self._priv is not None:
            return
        if not self.nsec:
            raise RuntimeError("nsec not configured")
        self._priv = PrivateKey.from_nsec(self.nsec)
        self._pub_hex = self._priv.public_key.hex()
        self._priv_hex = self._priv.hex()

    def sync_queue(self) -> Optional[str]:
        return self._sync_all()

    def sync_sources(self) -> Optional[str]:
        return self._sync_all()

    def _sync_all(self) -> Optional[str]:
        if not self.nsec or not self.relays:
            return None
        try:
            self._ensure_key()
            data = self._collect_state()
            rumor = build_rumor(data, self._pub_hex)
            seal = build_seal(rumor, self._priv_hex, self._pub_hex)
            wrap = build_gift_wrap(seal, self._pub_hex)
            return self._publish_event(wrap)
        except Exception:
            return None

    def _collect_state(self) -> dict:
        videos = self.store.list_videos(limit=10000)
        rows = self.store.list_sources()
        sources = []
        for (sid, enabled, api_base, api_channel, api_channel_url, rss_url,
             lookback_days, last_polled_ts, last_error) in rows:
            sources.append({
                "id": sid,
                "enabled": bool(enabled),
                "api_base": api_base,
                "api_channel": api_channel,
                "api_channel_url": api_channel_url,
                "rss_url": rss_url,
                "lookback_days": lookback_days,
            })
        return {
            "version": SYNC_VERSION,
            "ts": int(time.time()),
            "videos": videos,
            "sources": sources,
        }

    def _publish_event(self, ev: Event) -> str:
        rm = RelayManager(timeout=6)
        for r in self.relays:
            try:
                rm.add_relay(r)
            except Exception:
                pass
        try:
            rm.publish_event(ev)
            rm.run_sync()
        finally:
            try:
                rm.close_connections()
            except Exception:
                pass
        return ev.id

    def fetch_state(self) -> Optional[dict]:
        if not self.nsec or not self.relays:
            return None
        self._ensure_key()
        rm = RelayManager(timeout=8)
        for r in self.relays:
            try:
                rm.add_relay(r)
            except Exception:
                pass
        filters = FiltersList([Filters(
            kinds=[1059],
            pubkey_refs=[self._pub_hex],
            limit=10,
        )])
        try:
            rm.add_subscription_on_all_relays("pt2n-fetch", filters)
        except Exception:
            pass
        try:
            rm.run_sync()
        finally:
            pass

        result: Optional[Event] = None
        mp = getattr(rm, "message_pool", None)
        if mp is not None:
            while mp.has_events():
                msg = mp.get_event()
                ev = getattr(msg, "event", None) or msg
                seal = unwrap_gift_wrap(ev, self._priv_hex)
                if seal is None:
                    continue
                rumor = unseal(seal, self._priv_hex)
                if rumor is None:
                    continue
                if rumor.kind != SYNC_KIND:
                    continue
                tags = getattr(rumor, "tags", []) or []
                d_val = None
                for t in tags:
                    if isinstance(t, (list, tuple)) and len(t) >= 2 and t[0] == "d":
                        d_val = str(t[1])
                        break
                if d_val != SYNC_DTAG:
                    continue
                try:
                    parsed = json.loads(rumor.content)
                    if isinstance(parsed, dict):
                        if result is None or (parsed.get("version", 0) or 0) > (result.get("version", 0) or 0):
                            result = parsed
                except Exception:
                    continue

        try:
            rm.close_connections()
        except Exception:
            pass
        return result

    def sync_all(self) -> Optional[str]:
        return self._sync_all()
