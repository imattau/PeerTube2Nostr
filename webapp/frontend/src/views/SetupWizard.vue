<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { api } from '@/api/client'

const router = useRouter()

const step = ref(1)
const nsec = ref('')
const relayUrl = ref('')
const channelUrl = ref('')
const rssUrl = ref('')
const importNip65 = ref(false)
const loading = ref(false)

async function finish() {
  loading.value = true
  try {
    if (nsec.value) {
      await api.setNsec(nsec.value)
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
    alert(e.message)
  }
  loading.value = false
}
</script>

<template>
  <div class="px-40 py-32" style="max-width:600px;margin:0 auto;padding-top:60px">
    <div class="text-center mb-24">
      <div class="heading-1">PeerTube2Nostr</div>
      <div class="body mt-8">Welcome! Let's get you set up.</div>
    </div>

    <div class="card">
      <div v-if="step === 1">
        <div class="heading-3 mb-16">Nostr Identity</div>
        <div class="body mb-16">
          Choose how to sign events. NIP-07 (browser extension) is not available in the desktop app.
        </div>
        <div class="heading-4 mb-8">Local NSEC (OS keyring)</div>
        <div class="body mb-8">Store your secret key in the OS keychain:</div>
        <input v-model="nsec" type="password" placeholder="nsec1..." class="w-full" />
        <div class="body mb-8 mt-16">
          <strong>NIP-46 Bunker</strong> (remote signing via Nostr Connect) is planned for a future release.
        </div>
        <div class="body-small mt-8">You can skip this and configure signing later in Preferences.</div>
        <div class="flex gap-8 mt-16">
          <button class="button-default" @click="step = 3">Skip</button>
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
