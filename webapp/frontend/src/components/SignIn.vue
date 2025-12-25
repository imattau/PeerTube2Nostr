<script setup lang="ts">
import { reactive } from 'vue'
import { useAppStore } from '../store/app'
import Button from './ui/Button.vue'
import { ArrowRight } from 'lucide-vue-next'

const store = useAppStore()
const form = reactive({
  nsec: '',
  bunkerUrl: '',
  pubkey: '',
  signingMethod: 'extension',
})

const detectExtension = async () => {
  form.signingMethod = 'extension'
  const nostr = (window as any).nostr
  if (nostr) {
    try {
      form.pubkey = await nostr.getPublicKey()
    } catch (e) { console.error(e) }
  }
}

const handleSignIn = async () => {
  try {
    await store.signIn({
      method: form.signingMethod,
      nsec: form.signingMethod === 'nsec' ? form.nsec : undefined,
      bunkerUrl: form.signingMethod === 'bunker' ? form.bunkerUrl : undefined,
    })
  } catch (e) {
    alert('Sign-in failed. Please check your credentials.')
  }
}
</script>

<template>
  <div class="fixed inset-0 z-[100] bg-bg flex items-center justify-center p-4">
    <div class="w-full max-w-sm space-y-6 animate-in fade-in zoom-in-95 duration-300">
      <div class="text-center">
        <h1 class="text-2xl font-bold text-text-primary tracking-tight">Sign In to PeerTube2Nostr</h1>
        <p class="text-sm text-text-muted mt-2">Choose your preferred sign-in method.</p>
      </div>

      <div class="bg-surface-1 p-2 rounded-lg flex gap-1">
        <button v-for="m in [
          { id: 'extension', label: 'Extension' },
          { id: 'nsec', label: 'NSEC' },
          { id: 'bunker', label: 'Bunker' }
        ]" :key="m.id" @click="form.signingMethod = m.id"
          :class="[
            'flex-1 py-2 text-xs font-semibold rounded-md transition-colors',
            form.signingMethod === m.id ? 'bg-accent text-white' : 'text-text-muted hover:bg-surface-2'
          ]">
          {{ m.label }}
        </button>
      </div>

      <div v-if="form.signingMethod === 'nsec'">
        <label class="block text-xs font-medium text-text-muted mb-2">Private Key (nsec)</label>
        <input v-model="form.nsec" type="password" placeholder="nsec1..."
          class="w-full bg-surface-2 border border-border-subtle rounded-md px-3 py-2 text-sm" />
      </div>
      <div v-if="form.signingMethod === 'bunker'">
        <label class="block text-xs font-medium text-text-muted mb-2">Bunker URL</label>
        <input v-model="form.bunkerUrl" placeholder="bunker://..."
          class="w-full bg-surface-2 border border-border-subtle rounded-md px-3 py-2 text-sm" />
      </div>
      <div v-if="form.signingMethod === 'extension'">
        <Button variant="secondary" class="w-full" @click="detectExtension">
          Connect Browser Extension
        </Button>
        <div v-if="form.pubkey" class="text-xs text-text-muted mt-2 text-center truncate">
          <span class="font-bold text-emerald-500">Connected:</span> {{ form.pubkey }}
        </div>
      </div>

      <Button @click="handleSignIn" class="w-full">
        Sign In <ArrowRight class="w-4 h-4 ml-2" />
      </Button>
    </div>
  </div>
</template>
