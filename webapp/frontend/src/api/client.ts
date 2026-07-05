import { tauriInvoke, isTauri } from './adapter'

const BASE = '/api'
let _useTauri = isTauri()

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  })
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`)
  return res.json()
}

async function httpCall<T>(method: string, path: string): Promise<T> {
  return request<T>(path, { method })
}

export interface Source {
  id: number; enabled: boolean
  api_base: string | null; api_channel: string | null
  api_channel_url: string | null; rss_url: string | null
  lookback_days: number | null; last_polled_ts: number | null
  last_error: string | null
}

export interface Relay {
  id: number; enabled: boolean; relay_url: string
  relay_url_norm: string | null; last_used_ts: number | null
  last_error: string | null; latency_ms: number | null
}

export interface Video {
  id: number; source_id: number; watch_url: string
  title: string | null; summary: string | null
  channel_name: string | null; status: string
  thumbnail_url: string | null; published_ts: number | null
  first_seen_ts: number; error: string | null
}

export interface Metrics {
  relays: number; sources: number; pending: number
  posted: number; failed: number; has_nsec: boolean
  status: string; next_post: string; poll_age: string
  post_age: string; last_poll_ts: number | null
  last_posted_ts: number | null; min_interval: number
  max_per_hour: number; max_per_day_per_source: number
}

export interface Settings {
  min_publish_interval_seconds: number; max_posts_per_hour: number
  max_posts_per_day_per_source: number; has_nsec: boolean
}

async function tauri<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  return tauriInvoke<T>(cmd, args)
}

function pick<T>(http: () => Promise<T>, tauriFn: () => Promise<T>): () => Promise<T> {
  return () => _useTauri ? tauriFn() : http()
}

export const api = {

  // Sources
  listSources: pick(
    () => httpCall<{ sources: Source[] }>('GET', '/sources'),
    () => tauri<Source[]>('list_sources').then(r => ({ sources: r })),
  ),

  addSource: (url: string) => pick(
    () => httpCall<{ id: number; type: string }>('POST', `/sources?url=${encodeURIComponent(url)}`),
    () => tauri<{ id: number }>('add_source', { url }).then(r => ({ ...r, type: 'channel' })),
  )(),

  deleteSource: (id: number) => pick(
    () => httpCall<{ ok: boolean }>('DELETE', `/sources/${id}`),
    () => tauri<{ ok: boolean }>('remove_source', { id }),
  )(),

  enableSource: (id: number) => pick(
    () => httpCall<{ ok: boolean }>('POST', `/sources/${id}/enable`),
    () => tauri<{ ok: boolean }>('enable_source', { id }),
  )(),

  disableSource: (id: number) => pick(
    () => httpCall<{ ok: boolean }>('POST', `/sources/${id}/disable`),
    () => tauri<{ ok: boolean }>('disable_source', { id }),
  )(),

  resyncSource: (id: number) => pick(
    () => httpCall<{ cleared: number; inserted: number }>('POST', `/sources/${id}/resync`),
    () => Promise.resolve({ cleared: 0, inserted: 0 }),
  )(),

  // Relays
  listRelays: pick(
    () => httpCall<{ relays: Relay[] }>('GET', '/relays'),
    () => tauri<Relay[]>('list_relays').then(r => ({ relays: r })),
  ),

  addRelay: (url: string) => pick(
    () => request<{ id: number }>('/relays', { method: 'POST', body: JSON.stringify({ relay_url: url }) }),
    () => tauri<{ id: number }>('add_relay', { relayUrl: url }),
  )(),

  deleteRelay: (id: number) => pick(
    () => httpCall<{ ok: boolean }>('DELETE', `/relays/${id}`),
    () => tauri<{ ok: boolean }>('remove_relay', { id }),
  )(),

  enableRelay: (id: number) => pick(
    () => httpCall<{ ok: boolean }>('POST', `/relays/${id}/enable`),
    () => tauri<{ ok: boolean }>('enable_relay', { id }),
  )(),

  disableRelay: (id: number) => pick(
    () => httpCall<{ ok: boolean }>('POST', `/relays/${id}/disable`),
    () => tauri<{ ok: boolean }>('disable_relay', { id }),
  )(),

  checkRelays: pick(
    () => httpCall<{ results: { relay_url: string; latency_ms: number | null; error: string | null }[] }>('POST', '/relays/check'),
    () => Promise.resolve({ results: [] }),
  ),
  importNip65: () => pick(
    () => httpCall<{ imported: number }>('POST', '/relays/import-nip65'),
    () => tauriInvoke<{ imported: number }>('import_nip65_relays'),
  )(),

  // Queue
  listVideos: (status = 'pending', limit = 200) => pick(
    () => httpCall<{ videos: Video[]; count: number }>('GET', `/queue?status=${status}&limit=${limit}`),
    () => tauri<Video[]>('list_videos', { status, limit }).then(r => ({ videos: r, count: r.length })),
  )(),

  retryFailed: () => pick(
    () => httpCall<{ requeued: number }>('POST', '/queue/retry-failed'),
    () => tauri<{ count: number }>('retry_failed').then(r => ({ requeued: r.count })),
  )(),

  nextPending: () => pick(
    () => httpCall<{ video: Video | null; wait_seconds: number; eligible: boolean }>('GET', '/queue/next'),
    () => Promise.resolve({ video: null, wait_seconds: 0, eligible: false }),
  )(),

  // Metrics
  getMetrics: pick(
    () => httpCall<Metrics>('GET', '/metrics'),
    () => tauri<Metrics>('get_metrics'),
  ),

  // Settings
  getSettings: pick(
    () => httpCall<Settings>('GET', '/settings'),
    () => tauri<Settings>('get_settings'),
  ),

  updateSettings: (data: Partial<Settings>) => {
    const params = new URLSearchParams()
    if (data.min_publish_interval_seconds !== undefined) params.set('min_publish_interval_seconds', String(data.min_publish_interval_seconds))
    if (data.max_posts_per_hour !== undefined) params.set('max_posts_per_hour', String(data.max_posts_per_hour))
    if (data.max_posts_per_day_per_source !== undefined) params.set('max_posts_per_day_per_source', String(data.max_posts_per_day_per_source))
    return pick(
      () => httpCall<Settings>('PUT', `/settings?${params}`),
      () => tauri<Settings>('update_settings', {
        min_publish_interval_seconds: data.min_publish_interval_seconds ?? null,
        max_posts_per_hour: data.max_posts_per_hour ?? null,
        max_posts_per_day_per_source: data.max_posts_per_day_per_source ?? null,
      }),
    )()
  },

  setNsec: (nsec: string) => pick(
    () => request<{ stored_in: string }>('/settings/nsec', { method: 'PUT', body: JSON.stringify({ nsec }) }),
    () => tauriInvoke<{ configured: boolean }>('set_nsec', { nsecKey: nsec }).then(r => ({ stored_in: r.configured ? 'keyring' : '' })),
  )(),

  deleteNsec: () => pick(
    () => httpCall<{ removed: boolean }>('DELETE', '/settings/nsec'),
    () => tauriInvoke<{ configured: boolean }>('delete_nsec').then(r => ({ removed: !r.configured })),
  )(),

  // Setup
  setupStatus: pick(
    () => httpCall<{ complete: boolean; relays: number; sources: number; needs_onboarding: boolean }>('GET', '/setup/status'),
    () => Promise.resolve({ complete: false, relays: 0, sources: 0, needs_onboarding: true }),
  ),
  markSetupComplete: pick(
    () => httpCall<{ ok: boolean }>('POST', '/setup/complete'),
    () => Promise.resolve({ ok: true }),
  ),

  // Sync
  syncState: pick(
    () => httpCall<{ event_id: string }>('POST', '/sync'),
    () => Promise.resolve({ event_id: '' }),
  ),
  syncStatus: pick(
    () => httpCall<{ available: boolean; pubkey?: string; relay_count?: number }>('GET', '/sync/status'),
    () => Promise.resolve({ available: false }),
  ),
  syncRestore: pick(
    () => httpCall<{ found: boolean; version?: number; ts?: number; video_count?: number; source_count?: number }>('GET', '/sync/restore'),
    () => Promise.resolve({ found: false }),
  ),
}
