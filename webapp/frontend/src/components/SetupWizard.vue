<script setup lang="ts">
import { ref, reactive } from 'vue'
import { useAppStore } from '../store/app'
import Button from './ui/Button.vue'
import { 
  CheckCircle2,
  Info,
  ArrowRight
} from 'lucide-vue-next'

const store = useAppStore()
const step = ref(1)

const form = reactive({
  apiKey: '',
  nsec: '',
  bunkerUrl: '',
  pubkey: '',
  signingMethod: 'nsec', // 'nsec', 'bunker', or 'extension'
  firstChannel: ''
})

const detectExtension = async () => {
  const nostr = (window as any).nostr
  if (nostr) {
    try {
      const pubkey = await nostr.getPublicKey()
      form.pubkey = pubkey
      form.signingMethod = 'extension'
      return true
    } catch (e) {
      console.error('Extension access denied', e)
    }
  }
  return false
}

const nextStep = () => step.value++
const prevStep = () => step.value--

const finish = async () => {
  try {
    store.setApiKey(form.apiKey)
    await store.updateSigningConfig({
      method: form.signingMethod,
      nsec: form.signingMethod === 'nsec' ? form.nsec : null,
      bunker_url: form.signingMethod === 'bunker' ? form.bunkerUrl : null,
      pubkey: form.signingMethod === 'extension' ? form.pubkey : null
    })
    if (form.firstChannel) {
      await store.addSource(form.firstChannel)
    }
    await store.finishSetup()
  } catch (e) {
    alert('Setup failed. Please check your API key and try again.')
  }
}
</script>

<template>
  <div class="fixed inset-0 z-[100] bg-black flex items-center justify-center p-4">
    <div class="max-w-lg w-full bg-[#0a0a0a] border border-white/[0.05] rounded-xl p-10 shadow-2xl">
      
      <!-- Stepper -->
      <div class="flex gap-1.5 mb-10">
        <div v-for="i in 3" :key="i" :class="[
          'h-1 flex-1 rounded-full transition-all duration-500',
          step >= i ? 'bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,0.4)]' : 'bg-white/[0.05]'
        ]"></div>
      </div>

      <!-- Step 1: Security -->
      <div v-if="step === 1" class="animate-in fade-in duration-500">
        <h2 class="text-2xl font-bold text-white tracking-tight mb-2">Welcome</h2>
        <p class="text-xs text-slate-500 mb-8 font-medium">Verify your administrative access to continue.</p>
        
        <div class="space-y-4">
          <div>
            <label class="block text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2">Security API Key</label>
            <input v-model="form.apiKey" type="password" placeholder="Paste your API key"
              class="w-full bg-black border border-white/10 rounded-md px-4 py-2.5 text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500 font-mono text-xs" />
          </div>
          <div class="flex items-start gap-3 bg-white/[0.02] p-4 rounded-lg border border-white/[0.05]">
            <Info class="w-4 h-4 text-indigo-500 shrink-0" />
            <p class="text-[11px] text-slate-500 leading-relaxed italic">The key was generated on first run. Check your server logs or <code>API_KEY</code> env variable.</p>
          </div>
        </div>

        <Button class="w-full mt-10" :disabled="!form.apiKey" @click="nextStep">
          Continue Setup <ArrowRight class="w-4 h-4 ml-2" />
        </Button>
      </div>

      <!-- Step 2: Signing -->
      <div v-if="step === 2" class="animate-in fade-in duration-500">
        <h2 class="text-2xl font-bold text-white tracking-tight mb-2">Signing Method</h2>
        <p class="text-xs text-slate-500 mb-8 font-medium">Choose how you want to sign Nostr events.</p>
        
        <div class="grid grid-cols-1 gap-2 mb-8">
          <button v-for="m in [
            { id: 'nsec', label: 'Local NSEC', desc: 'Secure server-side storage' },
            { id: 'bunker', label: 'Nostr Connect', desc: 'Sign via remote bunker (NIP-46)' },
            { id: 'extension', label: 'Browser Extension', desc: 'Manual signing (NIP-07)', action: detectExtension }
          ]" :key="m.id" @click="m.action ? m.action() : (form.signingMethod = m.id)"
            :class="[
              'p-3.5 rounded-lg border text-left transition-all',
              form.signingMethod === m.id ? 'border-indigo-500 bg-indigo-500/[0.02]' : 'border-white/[0.05] bg-white/[0.01] hover:bg-white/[0.03]'
            ]">
            <div class="flex items-center justify-between">
              <span class="text-xs font-bold text-slate-200">{{ m.label }}</span>
              <CheckCircle2 v-if="form.signingMethod === m.id" class="w-3.5 h-3.5 text-indigo-500" />
            </div>
            <p class="text-[10px] text-slate-600 mt-0.5">{{ m.desc }}</p>
          </button>
        </div>

        <div v-if="form.signingMethod === 'nsec'" class="space-y-4">
          <label class="block text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2">Private Key (nsec...)</label>
          <input v-model="form.nsec" type="password" placeholder="nsec1..."
            class="w-full bg-black border border-white/10 rounded-md px-4 py-2.5 text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500 font-mono text-xs" />
        </div>

        <div v-if="form.signingMethod === 'extension' && form.pubkey" class="p-3 bg-emerald-500/5 border border-emerald-500/10 rounded-md">
          <p class="text-[10px] font-bold text-emerald-500 uppercase tracking-tight">Active Identity Detected</p>
          <p class="text-[9px] text-slate-500 truncate mt-0.5 font-mono">{{ form.pubkey }}</p>
        </div>

        <div class="flex gap-2 mt-10">
          <Button variant="secondary" class="flex-1" @click="prevStep">Back</Button>
          <Button class="flex-[2]" @click="nextStep">Continue</Button>
        </div>
      </div>

      <!-- Step 3: Source -->
      <div v-if="step === 3" class="animate-in fade-in duration-500">
        <h2 class="text-2xl font-bold text-white tracking-tight mb-2">Initialize</h2>
        <p class="text-xs text-slate-500 mb-8 font-medium">Add your first PeerTube channel URL.</p>
        
        <div class="space-y-4">
          <label class="block text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2">Channel or RSS URL</label>
          <input v-model="form.firstChannel" placeholder="https://example.tube/c/channel"
            class="w-full bg-black border border-white/10 rounded-md px-4 py-2.5 text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500 text-xs" />
        </div>

        <div class="flex gap-2 mt-10">
          <Button variant="secondary" class="flex-1" @click="prevStep">Back</Button>
          <Button class="flex-[2]" @click="finish">Finish Setup</Button>
        </div>
      </div>

    </div>
  </div>
</template>
