import { SimplePool, nip59, nip44, type NostrEvent, type EventTemplate } from 'nostr-tools'
import { getPublicKey, generateSecretKey, finalizeEvent, utils } from 'nostr-tools'
import { api } from './client'

const STORAGE_KEY_HEX = 'pt2n:identity_hex'
const STORAGE_KEY_NPUB = 'pt2n:identity_npub'
const SYNC_KIND = 30000
const SYNC_DTAG = 'pt2n:sync'

function nip07(): any {
  return (typeof window !== 'undefined' && (window as any).nostr) || null
}

function nip07HasNip44(): boolean {
  const n = nip07()
  return !!(n && n.nip44 && typeof n.nip44.encrypt === 'function' && typeof n.nip44.decrypt === 'function')
}

function isTauri(): boolean {
  return typeof window !== 'undefined' && !!window.__TAURI__
}

export function storeIdentityHex(hex: string) {
  sessionStorage.setItem(STORAGE_KEY_HEX, hex)
}

export function getIdentityHex(): string | null {
  return sessionStorage.getItem(STORAGE_KEY_HEX)
}

export function clearIdentityHex() {
  sessionStorage.removeItem(STORAGE_KEY_HEX)
  sessionStorage.removeItem(STORAGE_KEY_NPUB)
}

function getIdentityBytes(): Uint8Array | null {
  const hex = getIdentityHex()
  if (!hex) return null
  return utils.hexToBytes(hex)
}

export function storeIdentityNpub(npub: string) {
  sessionStorage.setItem(STORAGE_KEY_NPUB, npub)
}

export function getIdentityNpub(): string | null {
  return sessionStorage.getItem(STORAGE_KEY_NPUB)
}

export async function getNip07Pubkey(): Promise<string | null> {
  try {
    const n = nip07()
    if (!n || !n.getPublicKey) return null
    return await n.getPublicKey()
  } catch {
    return null
  }
}

export function isSyncAvailable(): boolean {
  if (isTauri()) return false
  return !!getIdentityHex() || nip07HasNip44()
}

async function getRelays(): Promise<string[]> {
  try {
    const res = await api.listRelays()
    return res.relays.filter(r => r.enabled).map(r => r.relay_url)
  } catch {
    return []
  }
}

async function getOwnPubkey(): Promise<string | null> {
  const bytes = getIdentityBytes()
  if (bytes) {
    try {
      return getPublicKey(bytes)
    } catch {}
  }
  return getNip07Pubkey()
}

async function nip07UnwrapEvent(wrapEvent: NostrEvent): Promise<any | null> {
  try {
    const n = nip07()
    if (!n || !n.nip44) return null
    const sealJson = await n.nip44.decrypt(wrapEvent.pubkey, wrapEvent.content)
    const seal = JSON.parse(sealJson)
    const rumorJson = await n.nip44.decrypt(seal.pubkey, seal.content)
    return JSON.parse(rumorJson)
  } catch {
    return null
  }
}

async function nip07WrapEvent(event: EventTemplate, ownPubkey: string): Promise<NostrEvent | null> {
  try {
    const n = nip07()
    if (!n || !n.nip44 || !n.signEvent) return null
    const created_at = Math.floor(Date.now() / 1000)
    const rumor = { ...event, pubkey: ownPubkey, created_at }
    const sealContent = await n.nip44.encrypt(ownPubkey, JSON.stringify(rumor))
    const sealEvent = {
      kind: 13, pubkey: ownPubkey, content: sealContent, tags: [] as string[][], created_at,
    }
    const signedSeal = await n.signEvent(sealEvent)
    const randomKey = generateSecretKey()
    const randomPubkey = getPublicKey(randomKey)
    const convKey = nip44.getConversationKey(randomKey, ownPubkey)
    const wrapContent = nip44.encrypt(JSON.stringify(signedSeal), convKey)
    const wrap = {
      kind: 1059, pubkey: randomPubkey, content: wrapContent,
      tags: [['p', ownPubkey]] as string[][], created_at,
    }
    return finalizeEvent(wrap, randomKey)
  } catch {
    return null
  }
}

export async function restoreState(): Promise<{
  found: boolean
  version?: number
  ts?: number
  video_count?: number
  source_count?: number
}> {
  const relays = await getRelays()
  if (!relays.length) return { found: false }

  const pubkey = await getOwnPubkey()
  if (!pubkey) return { found: false }

  const identityBytes = getIdentityBytes()

  const pool = new SimplePool()
  try {
    const events = await pool.querySync(relays, { kinds: [1059], '#p': [pubkey], limit: 10 })

    let best: any = null
    for (const ev of events) {
      try {
        let rumor: any
        if (identityBytes) {
          rumor = nip59.unwrapEvent(ev, identityBytes)
        } else {
          rumor = await nip07UnwrapEvent(ev)
        }
        if (!rumor || rumor.kind !== SYNC_KIND) continue
        const dTag = (rumor.tags || []).find((t: string[]) => t[0] === 'd')
        if (!dTag || dTag[1] !== SYNC_DTAG) continue
        const data = typeof rumor.content === 'string' ? JSON.parse(rumor.content) : rumor.content
        if (data && typeof data === 'object') {
          if (!best || (data.version || 0) > (best.version || 0)) {
            best = data
          }
        }
      } catch {}
    }

    if (best) {
      return {
        found: true,
        version: best.version,
        ts: best.ts,
        video_count: (best.videos || []).length,
        source_count: (best.sources || []).length,
      }
    }
    return { found: false }
  } finally {
    pool.destroy()
  }
}

export async function pushState(): Promise<{ event_id: string }> {
  const relays = await getRelays()
  if (!relays.length) return { event_id: '' }

  const identityBytes = getIdentityBytes()
  const ownPubkey = await getOwnPubkey()
  if (!ownPubkey) return { event_id: '' }

  let sources: any[] = []
  let videos: any[] = []
  try {
    const [sourcesRes, videosRes] = await Promise.all([
      api.listSources(),
      api.listVideos('pending', 10000),
    ])
    sources = sourcesRes.sources
    videos = videosRes.videos
  } catch {}

  const data = {
    version: 1,
    ts: Math.floor(Date.now() / 1000),
    videos,
    sources: sources.map(s => ({
      id: s.id, enabled: s.enabled,
      api_base: s.api_base, api_channel: s.api_channel,
      api_channel_url: s.api_channel_url, rss_url: s.rss_url,
      lookback_days: s.lookback_days,
    })),
  }

  const pool = new SimplePool()
  try {
    let wrap: NostrEvent
    const eventTemplate = { kind: SYNC_KIND, content: JSON.stringify(data), tags: [['d', SYNC_DTAG]], created_at: Math.floor(Date.now() / 1000) }
    if (identityBytes) {
      wrap = nip59.wrapEvent(eventTemplate, identityBytes, ownPubkey) as NostrEvent
    } else {
      const result = await nip07WrapEvent(eventTemplate, ownPubkey)
      if (!result) return { event_id: '' }
      wrap = result
    }

    await Promise.all(pool.publish(relays, wrap))
    return { event_id: wrap.id || '' }
  } finally {
    pool.destroy()
  }
}
