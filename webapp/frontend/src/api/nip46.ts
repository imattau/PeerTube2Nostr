import { nip04, generateSecretKey, getPublicKey, SimplePool, finalizeEvent, type NostrEvent, type Filter, utils } from 'nostr-tools'

interface BunkerComponents {
  remotePubkey: string
  relays: string[]
  secret?: string
}

interface SavedState {
  localSecret: string
  remotePubkey: string
  relays: string[]
  secret?: string
  userPubkey: string | null
}

interface PendingRequest {
  resolve: (value: any) => void
  reject: (reason: any) => void
  timer: ReturnType<typeof setTimeout>
}

const STORAGE_KEY = 'pt2n:nip46'
const REQUEST_TIMEOUT = 30_000

let _localSecret: Uint8Array | null = null
let _localPubkey: string | null = null
let _remotePubkey: string | null = null
let _relays: string[] = []
let _secret: string | undefined
let _pool: SimplePool | null = null
let _connected = false
let _connecting = false
let _userPubkey: string | null = null
let _pendingRequests = new Map<string, PendingRequest>()
let _closer: { close: () => void } | null = null
let _onStatusChange: (() => void) | null = null
let _requestId = 0

function parseBunkerUrl(raw: string): BunkerComponents {
  if (!raw.startsWith('bunker://'))
    throw new Error('Invalid bunker URL: must start with bunker://')
  const body = raw.slice('bunker://'.length)
  const q = body.indexOf('?')
  const remotePubkey = q === -1 ? body : body.slice(0, q)
  if (!remotePubkey || remotePubkey.length !== 64 || !/^[0-9a-f]{64}$/i.test(remotePubkey))
    throw new Error('Invalid remote signer pubkey in bunker URL')
  const params = q === -1 ? new URLSearchParams() : new URLSearchParams(body.slice(q + 1))
  const relays = params.getAll('relay')
  if (!relays.length) throw new Error('No relay specified in bunker URL')
  return { remotePubkey, relays, secret: params.get('secret') || undefined }
}

async function sendRequest(method: string, params: any[]): Promise<any> {
  if (!_localSecret || !_remotePubkey || !_pool || !_relays.length)
    throw new Error('NIP-46 not connected')

  const id = String(++_requestId)
  const payload = JSON.stringify({ id, method, params })

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      _pendingRequests.delete(id)
      reject(new Error(`NIP-46 request "${method}" timed out`))
    }, REQUEST_TIMEOUT)

    _pendingRequests.set(id, { resolve, reject, timer })

    try {
      const ciphertext = nip04.encrypt(_localSecret as Uint8Array, _remotePubkey as string, payload)
      const event = finalizeEvent({
        kind: 4,
        content: ciphertext,
        tags: [['p', _remotePubkey as string]],
        created_at: Math.floor(Date.now() / 1000),
      }, _localSecret as Uint8Array)

      const promises = _pool!.publish(_relays, event)
      Promise.allSettled(promises).catch(() => {})
    } catch (e) {
      reject(e)
    }
  })
}

function handleIncomingEvent(event: NostrEvent) {
  if (!_localSecret) return

  try {
    const plaintext = nip04.decrypt(_localSecret, event.pubkey, event.content)
    try {
      const msg = JSON.parse(plaintext)
      if (msg.id && _pendingRequests.has(msg.id)) {
        const pending = _pendingRequests.get(msg.id)!
        clearTimeout(pending.timer)
        _pendingRequests.delete(msg.id)
        if (msg.error) {
          pending.reject(new Error(msg.error))
        } else {
          pending.resolve(msg.result)
        }
      }
    } catch { }
  } catch { }
}

function saveState() {
  if (_localSecret && _remotePubkey && _relays.length) {
    const state: SavedState = {
      localSecret: utils.bytesToHex(_localSecret),
      remotePubkey: _remotePubkey,
      relays: _relays,
      secret: _secret,
      userPubkey: _userPubkey,
    }
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state))
  }
}

function clearState() {
  sessionStorage.removeItem(STORAGE_KEY)
}

async function cleanup() {
  _connected = false
  _connecting = false
  if (_closer) {
    try { _closer.close() } catch { }
    _closer = null
  }
  if (_pool) {
    try { _pool.destroy() } catch { }
    _pool = null
  }
  for (const [, pending] of _pendingRequests) {
    clearTimeout(pending.timer)
    pending.reject(new Error('NIP-46 disconnected'))
  }
  _pendingRequests.clear()
}

function notify() {
  if (_onStatusChange) _onStatusChange()
}

export function getStatus() {
  return {
    connected: _connected,
    connecting: _connecting,
    pubkey: _userPubkey,
    remotePubkey: _remotePubkey,
    relays: _relays,
  }
}

export function onStatusChange(cb: (() => void) | null) {
  _onStatusChange = cb
}

export async function connect(bunkerUrl: string): Promise<string> {
  if (_connecting) throw new Error('Already connecting')
  if (_connected) await disconnect()

  _connecting = true
  notify()

  try {
    const components = parseBunkerUrl(bunkerUrl)
    _remotePubkey = components.remotePubkey
    _relays = components.relays
    _secret = components.secret
    _localSecret = generateSecretKey()
    _localPubkey = getPublicKey(_localSecret)

    _pool = new SimplePool()

    const filter: Filter = {
      kinds: [4],
      authors: [_remotePubkey],
    }
    filter['#p'] = [_localPubkey]

    _closer = _pool.subscribe(_relays, filter, {
      onevent: handleIncomingEvent,
    })

    const connectParams: any[] = [_localPubkey]
    if (_secret) connectParams.push(_secret)
    await sendRequest('connect', connectParams)

    _userPubkey = await sendRequest('get_public_key', [])

    _connected = true
    _connecting = false
    saveState()
    notify()

    return _userPubkey as string
  } catch (e) {
    _connecting = false
    _connected = false
    await cleanup()
    clearState()
    notify()
    throw e
  }
}

export async function disconnect() {
  _userPubkey = null
  _localSecret = null
  _localPubkey = null
  _remotePubkey = null
  _relays = []
  _secret = undefined
  await cleanup()
  clearState()
  notify()
}

export async function getPubkey(): Promise<string> {
  if (!_connected || !_userPubkey) throw new Error('NIP-46 not connected')
  return _userPubkey
}

export async function signEvent(event: {
  kind: number
  content: string
  tags: string[][]
  created_at: number
  pubkey?: string
}): Promise<NostrEvent> {
  if (!_connected) throw new Error('NIP-46 not connected')
  const result = await sendRequest('sign_event', [event])
  if (result && typeof result === 'object' && result.id && result.sig) {
    return result as NostrEvent
  }
  throw new Error('Invalid sign_event response from remote signer')
}

export async function tryRestore(): Promise<boolean> {
  const raw = sessionStorage.getItem(STORAGE_KEY)
  if (!raw) return false

  try {
    const state: SavedState = JSON.parse(raw)
    if (!state.localSecret || !state.remotePubkey || !state.relays?.length) {
      clearState()
      return false
    }

    _localSecret = utils.hexToBytes(state.localSecret)
    _localPubkey = getPublicKey(_localSecret)
    _remotePubkey = state.remotePubkey
    _relays = state.relays
    _secret = state.secret
    _userPubkey = state.userPubkey || null

    _pool = new SimplePool()

    const filter: Filter = {
      kinds: [4],
      authors: [_remotePubkey],
    }
    filter['#p'] = [_localPubkey]

    _closer = _pool.subscribe(_relays, filter, {
      onevent: handleIncomingEvent,
    })

    _connected = true
    notify()
    return true
  } catch {
    clearState()
    return false
  }
}
