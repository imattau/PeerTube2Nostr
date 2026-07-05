<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api, type Settings } from '@/api/client'

const settings = ref<Settings | null>(null)
const loading = ref(true)
const saving = ref(false)
const showNsec = ref(false)
const nsecValue = ref('')
const showBunker = ref(false)
const bunkerUrl = ref('')
const statusMsg = ref('')

async function load() {
  loading.value = true
  try {
    settings.value = await api.getSettings()
  } catch { }
  loading.value = false
}

async function saveInterval() {
  if (!settings.value) return
  saving.value = true
  try {
    settings.value = await api.updateSettings({
      min_publish_interval_seconds: settings.value.min_publish_interval_seconds,
      max_posts_per_hour: settings.value.max_posts_per_hour,
      max_posts_per_day_per_source: settings.value.max_posts_per_day_per_source,
    })
  } catch (e: any) { alert(e.message) }
  saving.value = false
}

async function setNsec() {
  if (!nsecValue.value) return
  try {
    const res = await api.setNsec(nsecValue.value)
    statusMsg.value = `Stored in ${res.stored_in}`
    nsecValue.value = ''
    showNsec.value = false
    await load()
  } catch (e: any) { alert(e.message) }
}

async function clearNsec() {
  if (!confirm('Remove stored NSEC?')) return
  await api.deleteNsec()
  statusMsg.value = 'NSEC removed — returning to setup'
  setTimeout(() => window.location.reload(), 1500)
}

onMounted(load)
</script>

<template>
  <div class="px-40 py-32">
    <div class="heading-1 mb-8">Preferences</div>
    <div class="body mb-16">Publishing, identity, security and maintenance</div>

    <div v-if="loading" class="body">Loading...</div>

    <div v-if="settings" class="card mb-16">
      <div class="heading-4 mb-8">Publishing</div>

      <div class="action-row">
        <div class="action-info">
          <div class="action-title">Minimum interval</div>
          <div class="action-subtitle">Minimum time between posts (seconds)</div>
        </div>
        <div class="action-right">
          <input type="number" v-model.number="settings.min_publish_interval_seconds" style="width:80px" @change="saveInterval" />
        </div>
      </div>

      <div class="action-row">
        <div class="action-info">
          <div class="action-title">Maximum posts per hour</div>
          <div class="action-subtitle">Hourly publishing cap</div>
        </div>
        <div class="action-right">
          <input type="number" v-model.number="settings.max_posts_per_hour" style="width:80px" @change="saveInterval" />
        </div>
      </div>

      <div class="action-row">
        <div class="action-info">
          <div class="action-title">Daily source limit</div>
          <div class="action-subtitle">Max posts per source per day</div>
        </div>
        <div class="action-right">
          <input type="number" v-model.number="settings.max_posts_per_day_per_source" style="width:80px" @change="saveInterval" />
        </div>
      </div>
    </div>

    <div class="card mb-16">
      <div class="heading-4 mb-8">Nostr identity</div>

      <div class="action-row">
        <div class="action-info">
          <div class="action-title">Local NSEC (OS keyring)</div>
          <div class="action-subtitle">Nostr secret key stored in OS keychain or encrypted file</div>
        </div>
        <div class="action-right flex items-center gap-8">
          <span v-if="settings?.has_nsec" class="badge badge-success">Configured</span>
          <span v-else class="badge badge-warning">Not configured</span>
          <button class="button-default" @click="showNsec = !showNsec">{{ settings?.has_nsec ? 'Change' : 'Configure' }}</button>
          <button class="button-default" v-if="settings?.has_nsec" @click="clearNsec">Remove</button>
        </div>
      </div>

      <div v-if="showNsec" class="p-16">
        <div class="body mb-8">Enter your Nostr secret key (nsec):</div>
        <input v-model="nsecValue" type="password" placeholder="nsec1..." class="w-full" @keyup.enter="setNsec" />
        <div class="flex gap-8 mt-8">
          <button class="button-primary" :disabled="!nsecValue" @click="setNsec">Save</button>
          <button class="button-default" @click="showNsec = false; nsecValue = ''">Cancel</button>
        </div>
      </div>

      <div class="action-row">
        <div class="action-info">
          <div class="action-title">NIP-46 Bunker (remote signer)</div>
          <div class="action-subtitle">Connect to a Nostr Connect bunker for remote signing</div>
        </div>
        <div class="action-right">
          <button class="button-default" @click="showBunker = !showBunker">Configure</button>
        </div>
      </div>

      <div v-if="showBunker" class="p-16">
        <div class="body mb-8">Enter your bunker URL (e.g. <code>bunker://...</code>):</div>
        <input v-model="bunkerUrl" type="text" placeholder="bunker://..." class="w-full" />
        <div class="body-small mt-8" style="color:#999">
          NIP-46 bunker support is planned. Your nsec stays on the remote signer.
        </div>
        <div class="flex gap-8 mt-8">
          <button class="button-default" @click="showBunker = false; bunkerUrl = ''">Close</button>
        </div>
      </div>

      <div class="action-row" v-if="settings?.has_nsec">
        <div class="action-info">
          <div class="action-title">Synchronise profile</div>
          <div class="action-subtitle">Fetch metadata and NIP-65 relay list from relays</div>
        </div>
        <div class="action-right">
          <button class="button-default">Sync</button>
        </div>
      </div>

      <div v-if="statusMsg" class="body mt-8" style="color:#1B6D3F">{{ statusMsg }}</div>
    </div>

    <div class="card mb-16">
      <div class="heading-4 mb-8">Maintenance</div>

      <div class="action-row">
        <div class="action-info">
          <div class="action-title">Repair database</div>
          <div class="action-subtitle">Normalise and repair stored records</div>
        </div>
        <div class="action-right">
          <button class="button-default">Repair</button>
        </div>
      </div>
    </div>
  </div>
</template>
