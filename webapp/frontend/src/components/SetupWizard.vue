<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { useAppStore } from '../store/app'
import Button from './ui/Button.vue'
import { 
  CheckCircle2
} from 'lucide-vue-next'

const store = useAppStore()
const step = ref(1)
const setupToken = ref('')

const form = reactive({
  nsec: '',
  bunkerUrl: '',
  pubkey: '',
  signingMethod: 'nsec',
  firstChannel: ''
})

onMounted(async () => {
  const status = await store.fetchSetupStatus()
  if (status.setup_token) {
    setupToken.value = status.setup_token
  }
})

onMounted(async () => {
  const status = await store.fetchSetupStatus()
  if (status.setup_token) {
    setupToken.value = status.setup_token
  }
})

const detectExtension = async () => {
  const nostr = (window as any).nostr
  if (nostr) {
    try {
      form.pubkey = await nostr.getPublicKey()
      form.signingMethod = 'extension'
    } catch (e) { console.error(e) }
  }
}

const nextStep = () => step.value++
const prevStep = () => step.value--

const finish = async () => {
  try {
    await store.updateSigningConfig({
      method: form.signingMethod,
      nsec: form.signingMethod === 'nsec' ? form.nsec : null,
      bunker_url: form.signingMethod === 'bunker' ? form.bunkerUrl : null,
      pubkey: form.signingMethod === 'extension' ? form.pubkey : null
    })
    if (form.firstChannel) {
      await store.addSource(form.firstChannel)
    }
    await store.finishSetup(setupToken.value)
  } catch (e) {
    alert('Setup failed. Please check your inputs.')
  }
}
</script>

<template>
  <div class="fixed inset-0 z-[100] bg-black flex items-center justify-center p-4">
    <div class="max-w-lg w-full bg-[#0a0a0a] border border-white/[0.05] rounded-xl p-10 shadow-2xl">
      
      <div class="flex gap-1.5 mb-10">
        <div v-for="i in 2" :key="i" :class="['h-1 flex-1 rounded-full transition-all', step >= i ? 'bg-accent' : 'bg-surface-1']"></div>
      </div>

      <div v-if="step === 1" class="animate-in fade-in duration-500">
        <h2 class="text-2xl font-bold text-white mb-2">Signing Method</h2>
        <p class="text-xs text-text-muted mb-8">Choose how to sign Nostr events.</p>
        
        <div class="grid grid-cols-1 gap-2 mb-8">
          <button v-for="m in [
            { id: 'nsec', label: 'Local NSEC', desc: 'Secure server-side storage' },
            { id: 'bunker', label: 'Nostr Connect', desc: 'Remote bunker (NIP-46)' },
            { id: 'extension', label: 'Browser Extension', desc: 'Manual signing (NIP-07)', action: detectExtension }
          ]" :key="m.id" @click="m.action ? m.action() : (form.signingMethod = m.id)"
            :class="[
              'p-3.5 rounded-lg border text-left',
              form.signingMethod === m.id ? 'border-accent bg-accent/[0.02]' : 'border-border-subtle bg-surface-1 hover:bg-surface-2'
            ]">
            <div class="flex justify-between items-center"><span class="text-xs font-bold text-text-primary">{{ m.label }}</span>
              <CheckCircle2 v-if="form.signingMethod === m.id" class="w-3.5 h-3.5 text-accent" />
            </div>
            <p class="text-[10px] text-text-muted mt-0.5">{{ m.desc }}</p>
          </button>
        </div>

        <div v-if="form.signingMethod === 'nsec'" class="space-y-4">
          <label class="block text-[10px] font-bold text-text-muted uppercase mb-2">Private Key (nsec...)</label>
          <input v-model="form.nsec" type="password" class="w-full bg-black border border-border-subtle rounded-md px-4 py-2.5" />
        </div>

        <Button class="w-full mt-10" @click="nextStep">Continue</Button>
      </div>

      <div v-if="step === 2" class="animate-in fade-in duration-500">
        <h2 class="text-2xl font-bold text-white mb-2">Initialize</h2>
        <p class="text-xs text-text-muted mb-8">Add your first PeerTube channel.</p>
        
        <div class="space-y-4">
          <label class="block text-[10px] font-bold text-text-muted uppercase mb-2">Channel or RSS URL</label>
          <input v-model="form.firstChannel" class="w-full bg-black border border-border-subtle rounded-md px-4 py-2.5" />
        </div>

        <div class="flex gap-2 mt-10">
          <Button variant="secondary" class="flex-1" @click="prevStep">Back</Button>
          <Button class="flex-[2]" @click="finish">Finish Setup</Button>
        </div>
      </div>
    </div>
  </div>
</template>
