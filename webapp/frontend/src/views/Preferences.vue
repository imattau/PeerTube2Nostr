<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api, type Settings } from '@/api/client'
import { isPRFSupported, registerPasskeyIdentity, importPasskeyIdentityFromNsec, hasStoredPasskeyIdentity, getStoredPasskeyPubkey, exportPasskeyIdentityAsNsec } from 'nostr-passkey'
import { nip19 } from 'nostr-tools'

const settings = ref<Settings | null>(null)
const loading = ref(true)
const saving = ref(false)
const showNsec = ref(false)
const nsecValue = ref('')
const showBunker = ref(false)
const bunkerUrl = ref('')
const statusMsg = ref('')

const passkeySupported = ref(false)
const storedPasskeyPubkey = ref('')
const showPasskeyImport = ref(false)
const passkeyImportNsec = ref('')
const syncing = ref(false)

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
  if (!confirm('Remove NSEC and clear ALL data (sources, relays, queue, settings)?')) return
  await api.deleteNsec()
  statusMsg.value = 'All data cleared — returning to setup'
  setTimeout(() => window.location.reload(), 1500)
}

async function syncProfile() {
  syncing.value = true
  statusMsg.value = ''
  try {
    const res = await api.importNip65()
    statusMsg.value = `Imported ${res.imported} relay(s) from NIP-65 profile`
  } catch (e: any) {
    statusMsg.value = `Sync failed: ${e.message}`
  }
  syncing.value = false
}

const pkOpts = { rpName: 'PeerTube2Nostr', storage: sessionStorage }

async function setupPasskey() {
  try {
    const result = await registerPasskeyIdentity(pkOpts)
    const nsec = nip19.nsecEncode(result.secretKey as Uint8Array)
    await api.setNsec(nsec)
    statusMsg.value = 'New passkey created and stored in keyring'
    storedPasskeyPubkey.value = getStoredPasskeyPubkey(pkOpts) || ''
  } catch (e: any) { alert(e.message) }
}

async function importPasskey() {
  if (!passkeyImportNsec.value) return
  try {
    await importPasskeyIdentityFromNsec(passkeyImportNsec.value, pkOpts)
    await api.setNsec(passkeyImportNsec.value)
    statusMsg.value = 'NSEC imported into passkey and stored in keyring'
    passkeyImportNsec.value = ''
    showPasskeyImport.value = false
    storedPasskeyPubkey.value = getStoredPasskeyPubkey(pkOpts) || ''
  } catch (e: any) { alert(e.message) }
}

async function exportPasskeyNsec() {
  try {
    const nsec = await exportPasskeyIdentityAsNsec(undefined, pkOpts)
    nsecValue.value = nsec
    showNsec.value = true
  } catch (e: any) { alert(e.message) }
}

onMounted(async () => {
  await load()
  passkeySupported.value = await isPRFSupported()
  storedPasskeyPubkey.value = getStoredPasskeyPubkey(pkOpts) || ''
})

function npub(hex: string): string {
  try { return nip19.npubEncode(hex) } catch { return hex }
}
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

      <!-- Passkey -->
      <div v-if="passkeySupported" class="action-row">
        <div class="action-info">
          <div class="action-title">Passkey (biometric / hardware key)</div>
          <div class="action-subtitle" v-if="storedPasskeyPubkey">Ready: {{ npub(storedPasskeyPubkey) }}</div>
          <div class="action-subtitle" v-else>Protected by FaceID, TouchID, Windows Hello, or YubiKey</div>
        </div>
        <div class="action-right flex items-center gap-8">
          <span v-if="storedPasskeyPubkey" class="badge badge-success">Configured</span>
          <button class="button-default" @click="setupPasskey">{{ storedPasskeyPubkey ? 'New key' : 'Create passkey' }}</button>
          <button class="button-default" v-if="storedPasskeyPubkey" @click="showPasskeyImport = !showPasskeyImport">Import nsec</button>
          <button class="button-default" v-if="storedPasskeyPubkey" @click="exportPasskeyNsec">Export nsec</button>
        </div>
      </div>

      <div v-if="showPasskeyImport" class="p-16">
        <div class="body mb-8">Import an existing nsec into your passkey:</div>
        <input v-model="passkeyImportNsec" type="password" placeholder="nsec1..." class="w-full" @keyup.enter="importPasskey" />
        <div class="flex gap-8 mt-8">
          <button class="button-primary" :disabled="!passkeyImportNsec" @click="importPasskey">Import</button>
          <button class="button-default" @click="showPasskeyImport = false; passkeyImportNsec = ''">Cancel</button>
        </div>
      </div>

      <!-- Direct NSEC (OS keyring) -->
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

      <!-- NIP-46 Bunker -->
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
          <button class="button-default" :disabled="syncing" @click="syncProfile">{{ syncing ? 'Syncing...' : 'Sync' }}</button>
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
