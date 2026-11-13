<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api, type Relay } from '@/api/client'
import { useRelaysStore } from '@/stores/relays'

const relaysStore = useRelaysStore()
const loading = ref(true)
const checking = ref(false)
const showAdd = ref(false)
const addUrl = ref('')
const adding = ref(false)
const importing = ref(false)
const nip07Pubkey = ref('')
const notice = ref('')

function keyOf(r: Relay): string {
  return r.relay_url_norm || r.relay_url
}

async function checkHealth() {
  checking.value = true
  try {
    await api.checkRelays()
    await relaysStore.load(true)
  } catch { }
  checking.value = false
}

async function load() {
  loading.value = true
  await relaysStore.load(true)
  loading.value = false
}

async function addRelay() {
  if (!addUrl.value) return
  adding.value = true
  try {
    await api.addRelay(addUrl.value)
    const added = addUrl.value
    addUrl.value = ''
    showAdd.value = false
    await relaysStore.load(true)
    const match = relaysStore.relays.find(r => r.relay_url_norm?.replace(/\/+$/, '') === added.replace(/\/+$/, ''))
    if (match) relaysStore.toggle(keyOf(match))
  } catch (e: any) { alert(e.message) }
  adding.value = false
}

async function toggleRelay(r: Relay) {
  if (r.enabled) await api.disableRelay(r.id)
  else await api.enableRelay(r.id)
  await relaysStore.load(true)
}

async function removeRelay(id: number, url: string) {
  if (!confirm(`Remove ${url}?`)) return
  try {
    await api.deleteRelay(id)
    await relaysStore.load(true)
  } catch (e: any) { alert(e.message) }
}

async function importNip65() {
  importing.value = true
  notice.value = ''
  try {
    const pubkey = nip07Pubkey.value || undefined
    const res = await api.importNip65(pubkey)
    await relaysStore.load(true)
    if (!relaysStore.isLocked && res.relays?.length) {
      relaysStore.selectUrls(res.relays)
      notice.value = `Imported ${res.imported} relay(s) from your NIP-65 profile and added them to your publishing selection.`
    } else {
      notice.value = `Imported ${res.imported} relay(s) from your NIP-65 profile.`
    }
  } catch (e: any) {
    alert(e.message)
  }
  importing.value = false
}

function latencyBadgeClass(latency: number | null): string {
  if (latency === null) return 'badge-error'
  if (latency < 200) return 'badge-success'
  return 'badge-warning'
}

function latencyLabel(latency: number | null): string {
  if (latency === null) return 'Offline'
  return `${latency}ms`
}

onMounted(async () => {
  await load()
  checkHealth()
  if (typeof window !== 'undefined' && 'nostr' in window) {
    try {
      const win = window as any
      nip07Pubkey.value = await win.nostr.getPublicKey()
    } catch { }
  }
})
</script>

<template>
  <div class="px-40 py-32">
    <div class="flex items-center mb-16">
      <div class="flex-col" style="flex:1">
        <div class="heading-1">Relays</div>
        <div class="body mt-8">Nostr relay connectivity and publishing health</div>
      </div>
      <button class="button-default mr-8" :disabled="checking" @click="checkHealth">{{ checking ? 'Checking...' : 'Check health' }}</button>
      <button class="button-default mr-8" :disabled="importing" @click="importNip65">{{ importing ? 'Importing...' : 'Import from NIP-65' }}</button>
      <button v-if="!relaysStore.isLocked" class="button-primary" @click="showAdd = true">+ Add relay</button>
    </div>

    <div v-if="relaysStore.isLocked" class="banner banner-info mb-16">
      <div class="banner-title">Locked relay policy</div>
      <div class="banner-body">
        This server only publishes to the administrator's default relays. You can still choose which of them to use.
      </div>
    </div>

    <div v-if="notice" class="banner banner-info mb-16">
      <div class="banner-body">{{ notice }}</div>
    </div>

    <div v-if="loading" class="body">Loading...</div>

    <div class="heading-4 mb-8">Publishing relays</div>
    <div class="body mb-8">
      Choose which relays your posts are published to. The selection is stored in this browser for your Nostr identity.
    </div>
    <div class="flex gap-8 mb-16">
      <button class="button-default" @click="relaysStore.selectDefaults()">Use server defaults</button>
      <button class="button-default" @click="relaysStore.selectAll()">Select all enabled</button>
      <span class="body-small" style="align-self:center;color:#666">
        {{ relaysStore.publishRelays.length }} selected
      </span>
    </div>

    <div
      v-for="r in relaysStore.relays"
      :key="'select-' + r.id"
      class="action-row"
    >
      <div class="action-info">
        <label class="action-title" style="cursor:pointer">
          <input
            type="checkbox"
            :checked="relaysStore.isSelected(keyOf(r))"
            style="margin-right:8px"
            @change="relaysStore.toggle(keyOf(r))"
          />
          {{ r.relay_url }}
        </label>
        <div class="action-subtitle" v-if="r.last_error">Last error: {{ r.last_error }}</div>
      </div>
      <div class="action-right flex items-center gap-8">
        <span v-if="r.is_default" class="badge badge-accent">Default</span>
        <span v-if="!r.enabled" class="badge badge-warning">Disabled</span>
        <span class="badge" :class="latencyBadgeClass(r.latency_ms)">{{ latencyLabel(r.latency_ms) }}</span>
      </div>
    </div>

    <div v-if="!loading && relaysStore.relays.length === 0" class="empty-state">
      <div class="empty-icon">&#9744;</div>
      <div class="empty-title">No relays configured</div>
      <div class="empty-body">Add a Nostr relay to start publishing. You can also import relays from your NIP-65 profile.</div>
    </div>

    <template v-if="relaysStore.enabledRelays.length > 0">
      <div class="heading-4 mb-8 mt-24">Configured on this server</div>
      <div v-for="r in relaysStore.enabledRelays" :key="'server-' + r.id" class="action-row">
        <div class="action-info">
          <div class="action-title">{{ r.relay_url }}</div>
          <div class="action-subtitle" v-if="r.last_error">Last error: {{ r.last_error }}</div>
        </div>
        <div class="action-right flex items-center gap-8">
          <span v-if="r.is_default" class="badge badge-accent">Default</span>
          <span class="badge" :class="latencyBadgeClass(r.latency_ms)">{{ latencyLabel(r.latency_ms) }}</span>
          <button v-if="!relaysStore.isLocked" class="button-default" @click="removeRelay(r.id, r.relay_url)">Remove</button>
          <label class="switch" v-if="!relaysStore.isLocked">
            <input type="checkbox" :checked="r.enabled" @change="toggleRelay(r)" />
            <span class="slider"></span>
          </label>
        </div>
      </div>
    </template>

    <template v-if="relaysStore.relays.filter(r => !r.enabled).length > 0">
      <div class="heading-4 mb-8 mt-24">Disabled</div>
      <div v-for="r in relaysStore.relays.filter(r => !r.enabled)" :key="'disabled-' + r.id" class="action-row">
        <div class="action-info">
          <div class="action-title">{{ r.relay_url }}</div>
          <div class="action-subtitle" v-if="!relaysStore.isSelected(keyOf(r))">Not in your publishing selection</div>
        </div>
        <div class="action-right flex items-center gap-8">
          <span v-if="r.is_default" class="badge badge-accent">Default</span>
          <button v-if="!relaysStore.isLocked" class="button-default" @click="removeRelay(r.id, r.relay_url)">Remove</button>
          <label class="switch" v-if="!relaysStore.isLocked">
            <input type="checkbox" :checked="r.enabled" @change="toggleRelay(r)" />
            <span class="slider"></span>
          </label>
        </div>
      </div>
    </template>

    <div v-if="showAdd" class="dialog-overlay" @click.self="showAdd = false">
      <div class="dialog">
        <div class="heading-3 mb-16">Add relay</div>
        <input v-model="addUrl" placeholder="wss://relay.example.com" class="w-full" @keyup.enter="addRelay" />
        <div class="flex gap-8 mt-16">
          <button class="button-default" @click="showAdd = false">Cancel</button>
          <button class="button-primary ml-auto" :disabled="adding || !addUrl" @click="addRelay">
            {{ adding ? 'Adding...' : 'Add' }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
