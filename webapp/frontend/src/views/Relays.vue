<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api, type Relay } from '@/api/client'

const relays = ref<Relay[]>([])
const loading = ref(true)
const checking = ref(false)
const showAdd = ref(false)
const addUrl = ref('')
const adding = ref(false)
const importing = ref(false)

async function checkHealth() {
  checking.value = true
  try {
    await api.checkRelays()
  } catch { }
  checking.value = false
}

async function load() {
  loading.value = true
  try {
    const res = await api.listRelays()
    relays.value = res.relays
  } catch { }
  loading.value = false
}

async function addRelay() {
  if (!addUrl.value) return
  adding.value = true
  try {
    await api.addRelay(addUrl.value)
    addUrl.value = ''
    showAdd.value = false
    await load()
  } catch (e: any) { alert(e.message) }
  adding.value = false
}

async function toggleRelay(r: Relay) {
  if (r.enabled) await api.disableRelay(r.id)
  else await api.enableRelay(r.id)
  await load()
}

async function removeRelay(id: number, url: string) {
  if (!confirm(`Remove ${url}?`)) return
  await api.deleteRelay(id)
  await load()
}

async function importNip65() {
  importing.value = true
  try {
    const res = await api.importNip65()
    alert(`Imported ${res.imported} relay(s) from NIP-65 profile.`)
    await load()
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
  if (latency < 200) return `${latency}ms`
  return `${latency}ms`
}

onMounted(async () => {
  await load()
  checkHealth()
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
      <button class="button-primary" @click="showAdd = true">+ Add relay</button>
    </div>

    <div v-if="loading" class="body">Loading...</div>

    <div class="heading-4 mb-8 mt-24" v-if="relays.filter(r => r.enabled).length > 0">Configured relays</div>
    <div v-for="r in relays.filter(r => r.enabled)" :key="r.id" class="action-row">
      <div class="action-info">
        <div class="action-title">{{ r.relay_url }}</div>
        <div class="action-subtitle" v-if="r.last_error">Last error: {{ r.last_error }}</div>
      </div>
      <div class="action-right flex items-center gap-8">
        <span class="badge" :class="latencyBadgeClass(r.latency_ms)">{{ latencyLabel(r.latency_ms) }}</span>
        <button class="button-default" @click="removeRelay(r.id, r.relay_url)">Remove</button>
        <label class="switch">
          <input type="checkbox" :checked="r.enabled" @change="toggleRelay(r)" />
          <span class="slider"></span>
        </label>
      </div>
    </div>

    <div class="heading-4 mb-8 mt-24" v-if="relays.filter(r => !r.enabled).length > 0">Disabled</div>
    <div v-for="r in relays.filter(r => !r.enabled)" :key="r.id" class="action-row">
      <div class="action-info">
        <div class="action-title">{{ r.relay_url }}</div>
      </div>
      <div class="action-right flex items-center gap-8">
        <button class="button-default" @click="removeRelay(r.id, r.relay_url)">Remove</button>
        <label class="switch">
          <input type="checkbox" :checked="r.enabled" @change="toggleRelay(r)" />
          <span class="slider"></span>
        </label>
      </div>
    </div>

    <div v-if="!loading && relays.length === 0" class="empty-state">
      <div class="empty-icon">&#9744;</div>
      <div class="empty-title">No relays configured</div>
      <div class="empty-body">Add a Nostr relay to start publishing. You can also import relays from your NIP-65 profile.</div>
    </div>

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
