<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { api } from '@/api/client'
import { isPRFSupported, registerPasskeyIdentity, importPasskeyIdentityFromNsec, hasStoredPasskeyIdentity, getStoredPasskeyPubkey } from 'nostr-passkey'
import { nip19 } from 'nostr-tools'

const router = useRouter()

const step = ref(1)
const identityMethod = ref<'passkey-create' | 'passkey-import' | 'nsec' | 'nip07' | null>(null)

const nsecInput = ref('')
const importNsecInput = ref('')
const passkeyPubkey = ref('')
const passkeyNsecHex = ref('')
const nip07Pubkey = ref('')
const nip07Available = ref(false)

const relayUrl = ref('')
const channelUrl = ref('')
const rssUrl = ref('')
const importNip65 = ref(false)
const loading = ref(false)
const errorMsg = ref('')

const passkeySupported = ref(false)
const storedPasskeyPubkey = ref('')

const showingPasskeyImport = ref(false)
const showingNsecInput = ref(false)
const pkOpts = { rpName: 'PeerTube2Nostr', storage: sessionStorage }

onMounted(async () => {
  passkeySupported.value = await isPRFSupported()
  storedPasskeyPubkey.value = getStoredPasskeyPubkey(pkOpts) || ''
  if (typeof window !== 'undefined' && 'nostr' in window) {
    nip07Available.value = true
    try {
      const win = window as any
      nip07Pubkey.value = await win.nostr.getPublicKey()
    } catch { }
  }
})

async function createPasskey() {
  errorMsg.value = ''
  try {
    const result = await registerPasskeyIdentity(pkOpts)
    passkeyPubkey.value = result.pubkey
    passkeyNsecHex.value = bytesToHex(result.secretKey as Uint8Array)
    identityMethod.value = 'passkey-create'
    showingPasskeyImport.value = false
    showingNsecInput.value = false
  } catch (e: any) {
    errorMsg.value = e.message
  }
}

async function importPasskey() {
  if (!importNsecInput.value) return
  errorMsg.value = ''
  try {
    const result = await importPasskeyIdentityFromNsec(importNsecInput.value, pkOpts)
    passkeyPubkey.value = result.pubkey
    passkeyNsecHex.value = bytesToHex(result.secretKey as Uint8Array)
    identityMethod.value = 'passkey-import'
    showingPasskeyImport.value = false
    showingNsecInput.value = false
  } catch (e: any) {
    errorMsg.value = e.message
  }
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
}

function useNsecDirect() {
  if (!nsecInput.value) return
  identityMethod.value = 'nsec'
  showingNsecInput.value = false
  showingPasskeyImport.value = false
}

function useNip07() {
  identityMethod.value = 'nip07'
  showingNsecInput.value = false
  showingPasskeyImport.value = false
}

function decodedNsec(): string {
  if (identityMethod.value === 'passkey-create' || identityMethod.value === 'passkey-import') {
    if (passkeyNsecHex.value) {
      const bytes = new Uint8Array(passkeyNsecHex.value.match(/.{1,2}/g)!.map(b => parseInt(b, 16)))
      return nip19.nsecEncode(bytes)
    }
    return ''
  }
  if (identityMethod.value === 'nsec') {
    return nsecInput.value
  }
  return ''
}

function pubkeyBech32(hex: string): string {
  try {
    return nip19.npubEncode(hex)
  } catch {
    return hex
  }
}

async function finish() {
  loading.value = true
  errorMsg.value = ''
  try {
    const nsec = decodedNsec()
    if (nsec) {
      await api.setNsec(nsec)
    }
    if (relayUrl.value) {
      await api.addRelay(relayUrl.value)
    }
    if (channelUrl.value) {
      await api.addSource(channelUrl.value)
    }
    await api.markSetupComplete()
    router.replace('/overview')
  } catch (e: any) {
    errorMsg.value = e.message
  }
  loading.value = false
}
</script>

<template>
  <div class="px-40 py-32" style="max-width:640px;margin:0 auto;padding-top:60px">
    <div class="text-center mb-24">
      <div class="heading-1">PeerTube2Nostr</div>
      <div class="body mt-8">Welcome! Let's get you set up.</div>
    </div>

    <div v-if="errorMsg" class="banner banner-error mb-16">
      <div class="banner-title">Error</div>
      <div class="banner-body">{{ errorMsg }}</div>
    </div>

    <div class="card">
      <div v-if="step === 1">
        <div class="heading-3 mb-16">Nostr Identity</div>
        <div class="body mb-16">Choose how to manage your Nostr secret key.</div>

        <div v-if="nip07Available" class="card card-selectable" :class="{ 'card-selected': identityMethod === 'nip07' }" @click="useNip07">
          <div class="flex items-center gap-8">
            <div class="radio-circle" :class="{ 'radio-selected': identityMethod === 'nip07' }" />
            <div>
              <div class="heading-4">NIP-07 Browser Extension</div>
              <div class="body-small">Sign via browser extension (Alby, nos2x, etc.)</div>
              <div v-if="nip07Pubkey" class="body-small mt-4" style="color:#3B82F6;word-break:break-all">
                Pubkey: {{ pubkeyBech32(nip07Pubkey) }}
              </div>
              <div v-else class="body-small mt-4" style="color:#E66100">Extension detected but could not read public key</div>
            </div>
          </div>
        </div>

        <div v-if="passkeySupported" class="card card-selectable" :class="{ 'card-selected': identityMethod?.startsWith('passkey') }">
          <div class="flex items-center gap-8 mb-8">
            <div class="radio-circle" :class="{ 'radio-selected': identityMethod?.startsWith('passkey') }" />
            <div>
              <div class="heading-4">Passkey (biometric / hardware key)</div>
              <div class="body-small">Protected by FaceID, TouchID, Windows Hello, or YubiKey</div>
            </div>
          </div>
          <div v-if="!identityMethod?.startsWith('passkey')" class="flex gap-8 mt-8" style="margin-left:28px">
            <button class="button-default" @click="createPasskey">
              {{ storedPasskeyPubkey ? 'Unlock existing' : 'Create new key' }}
            </button>
            <button class="button-default" @click="showingPasskeyImport = true">Import existing nsec</button>
          </div>
          <div v-if="passkeyPubkey" class="mt-8 body-small" style="margin-left:28px;color:#2EC27E;word-break:break-all">
            Passkey ready: {{ pubkeyBech32(passkeyPubkey) }}
          </div>
        </div>

        <div v-if="showingPasskeyImport" class="mt-8">
          <div class="heading-4 mb-8">Import existing nsec into passkey:</div>
          <input v-model="importNsecInput" type="password" placeholder="nsec1..." class="w-full" @keyup.enter="importPasskey" />
          <div class="flex gap-8 mt-8">
            <button class="button-primary" :disabled="!importNsecInput" @click="importPasskey">Import</button>
            <button class="button-default" @click="showingPasskeyImport = false; importNsecInput = ''">Cancel</button>
          </div>
        </div>

        <div class="card card-selectable" :class="{ 'card-selected': identityMethod === 'nsec' }">
          <div class="flex items-center gap-8">
            <div class="radio-circle" :class="{ 'radio-selected': identityMethod === 'nsec' }" />
            <div>
              <div class="heading-4">Direct NSEC (OS keyring)</div>
              <div class="body-small">Store your secret key in the OS keychain</div>
            </div>
          </div>
          <div v-if="identityMethod !== 'nsec' && !showingNsecInput" class="mt-8" style="margin-left:28px">
            <button class="button-default" @click="showingNsecInput = true">Paste nsec</button>
          </div>
        </div>

        <div v-if="showingNsecInput" class="mt-8">
          <div class="heading-4 mb-8">Enter your Nostr secret key:</div>
          <input v-model="nsecInput" type="password" placeholder="nsec1..." class="w-full" @keyup.enter="useNsecDirect" />
          <div class="flex gap-8 mt-8">
            <button class="button-primary" :disabled="!nsecInput" @click="useNsecDirect">Use this key</button>
            <button class="button-default" @click="showingNsecInput = false; nsecInput = ''">Cancel</button>
          </div>
        </div>

        <div v-if="identityMethod === 'nsec' && nsecInput" class="body-small mt-8" style="color:#2EC27E">
          Key selected
        </div>

        <div v-if="!passkeySupported && !nip07Available" class="body-small mt-8">
          Tip: Passkeys (WebAuthn PRF) are not supported in this browser. You can paste an nsec directly.
        </div>

        <div class="flex gap-8 mt-16" v-if="identityMethod && step === 1">
          <button class="button-default" @click="identityMethod = null; showingNsecInput = false; showingPasskeyImport = false">Change</button>
          <button class="button-primary ml-auto" @click="step = 2">Next</button>
        </div>
      </div>

      <div v-if="step === 2">
        <div class="heading-3 mb-16">Nostr Relay</div>
        <div class="body mb-16">Add a relay to publish to. You can also import relays from your NIP-65 profile later.</div>
        <input v-model="relayUrl" placeholder="wss://relay.damus.io" class="w-full" />
        <div class="flex items-center gap-8 mt-8">
          <input type="checkbox" v-model="importNip65" id="nip65" />
          <label for="nip65" class="body">Import relays from NIP-65 profile</label>
        </div>
        <div class="flex gap-8 mt-16">
          <button class="button-default" @click="step = 1">Back</button>
          <button class="button-primary ml-auto" @click="step = 3">Next</button>
        </div>
      </div>

      <div v-if="step === 3">
        <div class="heading-3 mb-16">PeerTube Source</div>
        <div class="body mb-16">Add a PeerTube channel to start discovering videos.</div>
        <input v-model="channelUrl" placeholder="https://example.tube/c/mychannel" class="w-full" />
        <div class="body mt-8 mb-8">Optional RSS fallback:</div>
        <input v-model="rssUrl" placeholder="https://example.tube/feeds/videos.xml" class="w-full" />
        <div class="flex gap-8 mt-16">
          <button class="button-default" @click="step = 2">Back</button>
          <button class="button-primary ml-auto" :disabled="loading" @click="finish">
            {{ loading ? 'Setting up...' : 'Finish' }}
          </button>
        </div>
      </div>
    </div>

    <div class="text-center mt-16">
      <div class="body-small">Step {{ step }} of 3</div>
    </div>
  </div>
</template>

<style scoped>
.card-selectable {
  cursor: pointer;
  border: 2px solid transparent;
  transition: border-color 0.15s;
  margin-bottom: 8px;
}
.card-selectable:hover {
  border-color: #CDE0F9;
}
.card-selected {
  border-color: #3584E4;
  background: #F0F6FF;
}
.radio-circle {
  width: 18px;
  height: 18px;
  border-radius: 50%;
  border: 2px solid #AAA;
  flex-shrink: 0;
  margin-top: 2px;
}
.radio-selected {
  border-color: #3584E4;
  background: #3584E4;
  box-shadow: inset 0 0 0 3px white;
}
</style>
