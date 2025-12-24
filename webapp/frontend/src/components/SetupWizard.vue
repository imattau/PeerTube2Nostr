<script setup lang="ts">
import { ref, reactive } from 'vue'
import { useAppStore } from '../store/app'
import { 
  Key, 
  ShieldCheck, 
  Rss, 
  Zap,
  ArrowRight,
  CheckCircle2,
  Info
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
    
    // Save signing config
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
  <div class="fixed inset-0 z-[100] bg-slate-950 flex items-center justify-center p-4 overflow-y-auto">
    <div class="max-w-2xl w-full bg-slate-900 border border-slate-800 rounded-[2rem] p-8 md:p-12 shadow-2xl my-8">
      
      <!-- Progress Bar -->
      <div class="flex gap-2 mb-12">
        <div v-for="i in 3" :key="i" :class="[
          'h-1.5 flex-1 rounded-full transition-all duration-500',
          step >= i ? 'bg-indigo-500 shadow-[0_0_10px_rgba(99,102,241,0.5)]' : 'bg-slate-800'
        ]"></div>
      </div>

      <!-- Step 1: Authentication -->
      <div v-if="step === 1" class="animate-in fade-in slide-in-from-bottom-4 duration-500">
        <div class="bg-indigo-500/10 w-16 h-16 rounded-2xl flex items-center justify-center mb-6">
          <Key class="w-8 h-8 text-indigo-500" />
        </div>
        <h2 class="text-3xl font-bold mb-4">Welcome to PeerTube2Nostr</h2>
        <p class="text-slate-400 mb-8">First, enter your API Key. If you are running in Docker, check your logs for the generated key.</p>
        
        <div class="space-y-4">
          <div>
            <label class="block text-sm font-medium text-slate-300 mb-2">API Security Key</label>
            <input 
              v-model="form.apiKey"
              type="password"
              class="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-3 focus:ring-2 focus:ring-indigo-500 focus:outline-none"
              placeholder="Paste your API key here"
            />
          </div>
          <div class="flex items-start gap-3 bg-slate-800/50 p-4 rounded-xl border border-slate-700">
            <Info class="w-5 h-5 text-indigo-400 shrink-0 mt-0.5" />
            <p class="text-xs text-slate-400">This key protects your dashboard. You can set it via the <code>API_KEY</code> environment variable or use the one generated on first run.</p>
          </div>
        </div>

        <button @click="nextStep" :disabled="!form.apiKey" class="w-full mt-10 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white font-bold py-4 rounded-xl flex items-center justify-center gap-2 transition-all">
          Next Step <ArrowRight class="w-5 h-5" />
        </button>
      </div>

      <!-- Step 2: Signing -->
      <div v-if="step === 2" class="animate-in fade-in slide-in-from-right-4 duration-500">
        <div class="bg-amber-500/10 w-16 h-16 rounded-2xl flex items-center justify-center mb-6">
          <ShieldCheck class="w-8 h-8 text-amber-500" />
        </div>
        <h2 class="text-3xl font-bold mb-4">Nostr Signing</h2>
        <p class="text-slate-400 mb-8">Choose how you want to sign your Nostr events.</p>
        
        <div class="grid grid-cols-1 gap-4 mb-8">
          <button 
            @click="form.signingMethod = 'nsec'"
            :class="[
              'p-4 rounded-2xl border-2 text-left transition-all',
              form.signingMethod === 'nsec' ? 'border-indigo-500 bg-indigo-500/5' : 'border-slate-800 bg-slate-800/30 hover:border-slate-700'
            ]"
          >
            <div class="font-bold flex items-center justify-between">
              Local NSEC
              <CheckCircle2 v-if="form.signingMethod === 'nsec'" class="w-5 h-5 text-indigo-500" />
            </div>
            <p class="text-xs text-slate-500 mt-1">Store your private key locally on the server (Encrypted at rest if possible).</p>
          </button>

          <button 
            @click="form.signingMethod = 'bunker'"
            :class="[
              'p-4 rounded-2xl border-2 text-left transition-all',
              form.signingMethod === 'bunker' ? 'border-indigo-500 bg-indigo-500/5' : 'border-slate-800 bg-slate-800/30 hover:border-slate-700'
            ]"
          >
            <div class="font-bold flex items-center justify-between">
              Nostr Connect (Bunker)
              <CheckCircle2 v-if="form.signingMethod === 'bunker'" class="w-5 h-5 text-indigo-500" />
            </div>
            <p class="text-xs text-slate-500 mt-1">Sign remotely using NIP-46. Your key never touches our server.</p>
          </button>

          <button 
            @click="detectExtension"
            :class="[
              'p-4 rounded-2xl border-2 text-left transition-all',
              form.signingMethod === 'extension' ? 'border-indigo-500 bg-indigo-500/5' : 'border-slate-800 bg-slate-800/30 hover:border-slate-700'
            ]"
          >
            <div class="font-bold flex items-center justify-between">
              Browser Extension (NIP-07)
              <CheckCircle2 v-if="form.signingMethod === 'extension'" class="w-5 h-5 text-indigo-500" />
            </div>
            <p class="text-xs text-slate-500 mt-1">Use Alby, Nos2X, etc. Identity detected from your browser.</p>
          </button>
        </div>

        <div class="space-y-4">
          <div v-if="form.signingMethod === 'nsec'">
            <label class="block text-sm font-medium text-slate-300 mb-2">Private Key (nsec...)</label>
            <input 
              v-model="form.nsec"
              type="password"
              class="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-3 focus:ring-2 focus:ring-indigo-500 focus:outline-none"
              placeholder="nsec1..."
            />
          </div>
          <div v-else-if="form.signingMethod === 'bunker'">
            <label class="block text-sm font-medium text-slate-300 mb-2">Bunker Connection String</label>
            <input 
              v-model="form.bunkerUrl"
              class="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-3 focus:ring-2 focus:ring-indigo-500 focus:outline-none"
              placeholder="bunker://... or npub..."
            />
          </div>
          <div v-else-if="form.signingMethod === 'extension'" class="bg-indigo-500/10 border border-indigo-500/20 p-4 rounded-xl">
            <div class="flex items-center gap-2 mb-2">
              <CheckCircle2 class="w-4 h-4 text-indigo-500" />
              <span class="text-sm font-bold text-indigo-400">Extension Detected</span>
            </div>
            <p class="text-[10px] font-mono break-all text-slate-400">{{ form.pubkey }}</p>
            <p class="text-[10px] text-slate-500 mt-2 italic">Note: Background automation requires NIP-46 or NSEC. Extension mode is for manual dashboard actions.</p>
          </div>
        </div>

        <div class="flex gap-4 mt-10">
          <button @click="prevStep" class="flex-1 bg-slate-800 hover:bg-slate-700 text-white font-bold py-4 rounded-xl transition-all">
            Back
          </button>
          <button @click="nextStep" class="flex-[2] bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-4 rounded-xl flex items-center justify-center gap-2 transition-all">
            Continue <ArrowRight class="w-5 h-5" />
          </button>
        </div>
      </div>

      <!-- Step 3: Content -->
      <div v-if="step === 3" class="animate-in fade-in slide-in-from-right-4 duration-500">
        <div class="bg-emerald-500/10 w-16 h-16 rounded-2xl flex items-center justify-center mb-6">
          <Rss class="w-8 h-8 text-emerald-500" />
        </div>
        <h2 class="text-3xl font-bold mb-4">First Source</h2>
        <p class="text-slate-400 mb-8">Almost there! Add your first PeerTube channel to start publishing.</p>
        
        <div class="space-y-4">
          <div>
            <label class="block text-sm font-medium text-slate-300 mb-2">Channel URL</label>
            <input 
              v-model="form.firstChannel"
              class="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-3 focus:ring-2 focus:ring-indigo-500 focus:outline-none"
              placeholder="https://peertube.instance/c/my_channel"
            />
          </div>
        </div>

        <div class="bg-indigo-500/10 border border-indigo-500/20 p-6 rounded-3xl mt-10">
          <h4 class="font-bold flex items-center gap-2 mb-2">
            <Zap class="w-4 h-4 text-indigo-400" /> Fast Forward
          </h4>
          <p class="text-sm text-slate-400">Once you finish, the runner will start polling automatically and your first posts will appear in the queue shortly.</p>
        </div>

        <div class="flex gap-4 mt-10">
          <button @click="prevStep" class="flex-1 bg-slate-800 hover:bg-slate-700 text-white font-bold py-4 rounded-xl transition-all">
            Back
          </button>
          <button @click="finish" class="flex-[2] bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-4 rounded-xl flex items-center justify-center gap-2 transition-all">
            Finish Setup <CheckCircle2 class="w-5 h-5" />
          </button>
        </div>
      </div>

    </div>
  </div>
</template>

<style scoped>
.animate-in {
  animation-duration: 0.5s;
}
</style>
