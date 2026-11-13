import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { api, type Relay } from '@/api/client'

const STORAGE_PREFIX = 'pt2n:publish_relays:'
const IDENTITY_NPUB_KEY = 'pt2n:identity_npub'
const IDENTITY_HEX_KEY = 'pt2n:identity_hex'

function identityKey(): string {
  try {
    return sessionStorage.getItem(IDENTITY_NPUB_KEY)
      || sessionStorage.getItem(IDENTITY_HEX_KEY)
      || 'local'
  } catch {
    return 'local'
  }
}

function relayKey(relay: Relay): string {
  return relay.relay_url_norm || relay.relay_url
}

function sameRelay(a: string, b: string): boolean {
  return a.replace(/\/+$/, '') === b.replace(/\/+$/, '')
}

function unique(values: string[]): string[] {
  const out: string[] = []
  for (const value of values) {
    if (!value) continue
    if (!out.some(existing => sameRelay(existing, value))) out.push(value)
  }
  return out
}

export const useRelaysStore = defineStore('relays', () => {
  const relays = ref<Relay[]>([])
  const defaultRelays = ref<string[]>([])
  const policy = ref('open')
  const selected = ref<string[]>([])
  const loading = ref(false)
  const loaded = ref(false)
  const error = ref<string | null>(null)

  const isLocked = computed(() => policy.value === 'locked')
  const enabledRelays = computed(() => relays.value.filter(r => r.enabled))
  const enabledKeys = computed(() => enabledRelays.value.map(relayKey))

  const defaultKeys = computed(() => {
    const keys: string[] = []
    for (const raw of defaultRelays.value) {
      const match = relays.value.find(r => sameRelay(relayKey(r), raw))
      keys.push(match ? relayKey(match) : raw)
    }
    return unique(keys)
  })

  function canonical(value: string): string | null {
    const match = relays.value.find(r => sameRelay(relayKey(r), value))
    return match ? relayKey(match) : null
  }

  function readStored(): string[] {
    try {
      const raw = localStorage.getItem(STORAGE_PREFIX + identityKey())
      if (!raw) return []
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) ? parsed.filter(v => typeof v === 'string') : []
    } catch {
      return []
    }
  }

  function persist() {
    try {
      localStorage.setItem(STORAGE_PREFIX + identityKey(), JSON.stringify(selected.value))
    } catch {
      // localStorage can be unavailable (private mode); selection then lasts for the session only.
    }
  }

  function resolveSelection(stored: string[]) {
    const mapped = unique(
      stored
        .map(canonical)
        .filter((v): v is string => !!v),
    )
    if (mapped.length) return mapped
    if (defaultKeys.value.length) return [...defaultKeys.value]
    return [...enabledKeys.value]
  }

  async function load(force = false) {
    if (loaded.value && !force) return
    loading.value = true
    try {
      const res = await api.listRelays()
      relays.value = res.relays
      defaultRelays.value = res.default_relays || []
      policy.value = res.relay_policy || 'open'
      selected.value = resolveSelection(readStored())
      if (selected.value.length) persist()
      error.value = null
      loaded.value = true
    } catch (e: any) {
      error.value = e.message
    } finally {
      loading.value = false
    }
  }

  async function refreshIdentity() {
    if (!loaded.value) return
    selected.value = resolveSelection(readStored())
    if (selected.value.length) persist()
  }

  function isSelected(url: string): boolean {
    return selected.value.some(value => sameRelay(value, url))
  }

  function toggle(url: string) {
    const key = canonical(url) || url
    if (isSelected(key)) {
      selected.value = selected.value.filter(value => !sameRelay(value, key))
    } else {
      selected.value = unique([...selected.value, key])
    }
    persist()
  }

  function selectAll() {
    selected.value = unique([...selected.value, ...enabledKeys.value])
    persist()
  }

  function selectDefaults() {
    selected.value = [...defaultKeys.value]
    persist()
  }

  function selectUrls(urls: string[]) {
    const mapped = unique(
      urls
        .map(canonical)
        .filter((v): v is string => !!v),
    )
    if (!mapped.length) return
    selected.value = unique([...selected.value, ...mapped])
    persist()
  }

  /** Relays to publish to right now. Empty user selection falls back to enabled relays. */
  const publishRelays = computed(() => {
    const chosen = selected.value.filter(url => relays.value.some(r => sameRelay(relayKey(r), url)))
    if (chosen.length) return chosen
    if (enabledKeys.value.length) return [...enabledKeys.value]
    return [...defaultKeys.value]
  })

  return {
    relays,
    defaultRelays,
    policy,
    selected,
    loading,
    loaded,
    error,
    isLocked,
    enabledRelays,
    defaultKeys,
    publishRelays,
    load,
    refreshIdentity,
    isSelected,
    toggle,
    selectAll,
    selectDefaults,
    selectUrls,
    persist,
  }
})
