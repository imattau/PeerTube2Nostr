<script setup lang="ts">
import { onMounted, onUnmounted, reactive, ref } from 'vue'
import { useAppStore } from './store/app'
import SetupWizard from './components/SetupWizard.vue'
import { 
  Database, 
  Rss, 
  Key, 
  PlusCircle,
  Trash2,
  CheckCircle2,
  XCircle,
  Play,
  Square,
  AlertCircle,
  LayoutDashboard,
  ShieldCheck,
  Zap,
  Terminal,
  ExternalLink,
  RefreshCw,
  Info
} from 'lucide-vue-next'

const store = useAppStore()
const activeTab = ref('dashboard')
const showSetupBanner = ref(true)

const modal = reactive({
  show: false,
  title: '',
  label: '',
  value: '',
  action: null as Function | null,
  error: ''
})

const openModal = (title: string, label: string, action: Function) => {
  modal.title = title
  modal.label = label
  modal.action = action
  modal.value = ''
  modal.error = ''
  modal.show = true
}

const handleModalSubmit = async () => {
  if (!modal.value) {
    modal.error = 'Field cannot be empty'
    return
  }
  try {
    if (modal.action) await modal.action(modal.value)
    modal.show = false
  } catch (e: any) {
    modal.error = e.response?.data?.detail || 'An error occurred'
  }
}

let pollInterval: any
onMounted(() => {
  store.fetchAll()
  store.fetchLogs()
  pollInterval = setInterval(() => {
    store.fetchAll()
    store.fetchLogs()
  }, 5000)
})

onUnmounted(() => {
  clearInterval(pollInterval)
})

const addSource = () => openModal('Add Source', 'PeerTube Channel or RSS URL', store.addSource)
const addRelay = () => openModal('Add Relay', 'Relay URL (wss://...)', store.addRelay)
const setNsec = () => openModal('Set NSEC', 'nsec1...', store.updateNsec)

const promptKey = () => {
  const key = window.prompt('Enter API Key')
  if (key) store.setApiKey(key)
}

const formatTs = (ts: number | null) => {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}
</script>

<template>
  <div class="min-h-screen bg-[#050505] text-slate-300 font-sans antialiased pb-20">
    
    <!-- API Key Onboarding Banner -->
    <div v-if="!store.apiKey && showSetupBanner" class="bg-indigo-600/10 border-b border-indigo-500/20 px-4 py-2">
      <div class="max-w-[1100px] mx-auto flex items-center justify-between">
        <div class="flex items-center gap-2 text-xs font-bold text-indigo-400 uppercase tracking-tighter">
          <Info class="w-3.5 h-3.5" />
          API Access Required for data updates
        </div>
        <div class="flex items-center gap-4">
          <button @click="promptKey" class="text-[10px] font-black text-white bg-indigo-600 px-3 py-1 rounded hover:bg-indigo-500 transition-colors">ENTER KEY</button>
          <button @click="showSetupBanner = false" class="text-indigo-400 hover:text-white transition-colors"><XCircle class="w-4 h-4" /></button>
        </div>
      </div>
    </div>

    <!-- Centralized Container -->
    <div class="max-w-[1100px] mx-auto px-6 py-8">
      
      <!-- Operational Header -->
      <header class="flex items-center justify-between mb-8 border-b border-white/[0.05] pb-6">
        <div class="flex items-center gap-4">
          <div class="bg-indigo-600 p-1.5 rounded">
            <Zap class="w-5 h-5 text-white fill-white" />
          </div>
          <div>
            <h1 class="text-lg font-bold text-white tracking-tight">PeerTube2Nostr</h1>
            <p class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Control Panel</p>
          </div>
        </div>

        <div class="flex items-center gap-3">
          <div class="flex items-center gap-2 px-2.5 py-1 bg-white/[0.03] rounded border border-white/[0.05]">
            <div :class="['w-1.5 h-1.5 rounded-full', store.metrics.status === 'idle' ? 'bg-emerald-500' : 'bg-blue-500 animate-pulse']"></div>
            <span class="text-[10px] font-black uppercase text-slate-400">{{ store.metrics.status || 'OFFLINE' }}</span>
          </div>
          <div class="flex bg-white/[0.03] rounded border border-white/[0.05] overflow-hidden">
            <button @click="store.startRunner" :disabled="store.metrics.status !== 'stopped'" class="p-1.5 hover:bg-white/5 disabled:opacity-20 transition-colors">
              <Play class="w-3.5 h-3.5 text-emerald-500 fill-emerald-500" />
            </button>
            <button @click="store.stopRunner" :disabled="store.metrics.status === 'stopped'" class="p-1.5 hover:bg-white/5 disabled:opacity-20 transition-colors border-l border-white/[0.05]">
              <Square class="w-3.5 h-3.5 text-red-500 fill-red-500" />
            </button>
          </div>
        </div>
      </header>

      <!-- Dashboard Grid -->
      <div v-if="activeTab === 'dashboard'" class="grid grid-cols-1 md:grid-cols-12 gap-6 animate-in fade-in duration-500">
        
        <!-- Top Metrics (4-wide) -->
        <div class="md:col-span-12 grid grid-cols-2 md:grid-cols-4 gap-4 mb-2">
          <div v-for="(val, label) in { 'Queue': store.metrics.pending, 'Published': store.metrics.posted, 'Failed': store.metrics.failed, 'Sources': store.metrics.sources }" :key="label"
            class="bg-white/[0.02] p-4 rounded-md">
            <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{{ label }}</p>
            <p class="text-xl font-bold text-white mt-0.5">{{ val ?? 0 }}</p>
          </div>
        </div>

        <!-- Primary Column -->
        <div class="md:col-span-8 space-y-6">
          
          <!-- Compact Queue -->
          <section>
            <h3 class="text-[11px] font-black text-slate-500 uppercase tracking-widest mb-3 flex items-center gap-2">
              <LayoutDashboard class="w-3.5 h-3.5" /> Pending Queue
            </h3>
            <div class="bg-white/[0.02] rounded border border-white/[0.03] overflow-hidden">
              <div v-if="store.queue.length > 0" class="divide-y divide-white/[0.03]">
                <div v-for="item in store.queue" :key="item.id" class="p-3 flex items-center gap-4 hover:bg-white/[0.01] group">
                  <div class="w-16 aspect-video rounded-sm overflow-hidden bg-slate-900 shrink-0 border border-white/5">
                    <img v-if="item.thumbnail_url" :src="item.thumbnail_url" class="w-full h-full object-cover grayscale-[0.3] group-hover:grayscale-0 transition-all" />
                    <div v-else class="w-full h-full flex items-center justify-center"><Play class="w-3 h-3 text-slate-700" /></div>
                  </div>
                  <div class="flex-1 min-w-0">
                    <p class="text-[9px] text-indigo-400 font-bold uppercase">{{ item.channel_name }}</p>
                    <h4 class="text-xs font-medium text-slate-200 truncate">{{ item.title || item.watch_url }}</h4>
                  </div>
                  <a :href="item.watch_url" target="_blank" class="p-1.5 text-slate-600 hover:text-white transition-colors">
                    <ExternalLink class="w-3.5 h-3.5" />
                  </a>
                </div>
              </div>
              <div v-else class="h-24 flex flex-col items-center justify-center gap-1">
                <CheckCircle2 class="w-4 h-4 text-slate-700" />
                <p class="text-[10px] text-slate-600 font-bold uppercase tracking-tight">Queue is clean</p>
              </div>
            </div>
          </section>

          <!-- Source Management (Simplified Table) -->
          <section>
            <div class="flex justify-between items-center mb-3">
              <h3 class="text-[11px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
                <Rss class="w-3.5 h-3.5" /> Active Channels
              </h3>
              <button @click="addSource" class="text-indigo-400 hover:text-white text-[10px] font-black uppercase transition-colors flex items-center gap-1.5">
                <PlusCircle class="w-3 h-3" /> Add Source
              </button>
            </div>
            <div class="bg-white/[0.02] rounded border border-white/[0.03] overflow-hidden">
              <table class="w-full text-left text-xs">
                <tbody class="divide-y divide-white/[0.03]">
                  <tr v-for="source in store.sources" :key="source.id" class="hover:bg-white/[0.01]">
                    <td class="px-4 py-3 min-w-0">
                      <div class="font-bold text-slate-200 truncate max-w-[250px]">{{ source.api_channel_url || source.rss_url }}</div>
                      <div class="text-[9px] text-slate-600 uppercase font-black mt-0.5">Polled: {{ formatTs(source.last_polled_ts) }}</div>
                    </td>
                    <td class="px-4 py-3 text-right">
                      <div class="flex items-center justify-end gap-1">
                        <button @click="store.toggleSource(source.id, !source.enabled)" :class="source.enabled ? 'text-emerald-500' : 'text-slate-600'" class="p-1.5 hover:bg-white/5 rounded">
                          <RefreshCw class="w-3.5 h-3.5" />
                        </button>
                        <button @click="store.deleteSource(source.id)" class="p-1.5 hover:bg-red-500/10 text-slate-600 hover:text-red-500 rounded">
                          <Trash2 class="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </section>
        </div>

        <!-- Secondary Column -->
        <div class="md:col-span-4 space-y-6">
          
          <!-- Relay Health -->
          <section>
            <div class="flex justify-between items-center mb-3">
              <h3 class="text-[11px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
                <Database class="w-3.5 h-3.5" /> Relays
              </h3>
              <button @click="addRelay" class="text-indigo-400 hover:text-white transition-colors"><PlusCircle class="w-3.5 h-3.5" /></button>
            </div>
            <div class="space-y-2">
              <div v-for="relay in store.relays" :key="relay.id" class="bg-white/[0.02] p-3 rounded flex items-center justify-between group border border-white/[0.03]">
                <div class="min-w-0 flex-1">
                  <p class="text-xs font-bold text-slate-300 truncate">{{ relay.relay_url.replace('wss://', '') }}</p>
                  <span v-if="relay.latency_ms" class="text-[9px] text-indigo-500 font-black uppercase">{{ relay.latency_ms }}ms latency</span>
                </div>
                <div class="flex items-center opacity-0 group-hover:opacity-100 transition-opacity">
                  <button @click="store.toggleRelay(relay.id, !relay.enabled)" :class="relay.enabled ? 'text-emerald-500' : 'text-slate-600'" class="p-1 hover:bg-white/5 rounded"><CheckCircle2 class="w-3.5 h-3.5" /></button>
                  <button @click="store.deleteRelay(relay.id)" class="p-1 text-slate-600 hover:text-red-500 rounded"><Trash2 class="w-3.5 h-3.5" /></button>
                </div>
              </div>
            </div>
          </section>

          <!-- System Activity (Monospace) -->
          <section>
            <h3 class="text-[11px] font-black text-slate-500 uppercase tracking-widest mb-3 flex items-center gap-2">
              <Terminal class="w-3.5 h-3.5" /> Activity
            </h3>
            <div class="bg-black/50 rounded p-3 font-mono text-[10px] h-[300px] overflow-y-auto custom-scrollbar border border-white/[0.03]">
              <div v-for="(log, i) in store.logs.slice().reverse()" :key="i" class="py-1 text-slate-500 leading-relaxed border-b border-white/[0.02] last:border-0">
                <span class="text-indigo-500/40 mr-1.5">[{{ store.logs.length - i }}]</span>{{ log }}
              </div>
            </div>
          </section>

          <!-- Configuration & Auth -->
          <section class="pt-4 border-t border-white/[0.05]">
            <div class="flex flex-col gap-2">
              <button @click="setNsec" class="w-full bg-white/[0.03] hover:bg-white/[0.06] text-slate-300 py-2.5 rounded text-[10px] font-black uppercase tracking-widest transition-colors flex items-center justify-center gap-2">
                <ShieldCheck class="w-3.5 h-3.5 text-emerald-500" v-if="store.metrics.has_nsec" />
                <Key class="w-3.5 h-3.5 text-indigo-500" v-else />
                Update NSEC
              </button>
              <button @click="promptKey" class="w-full bg-white/[0.03] hover:bg-white/[0.06] text-slate-500 py-2.5 rounded text-[10px] font-black uppercase tracking-widest transition-colors">
                Change API Key
              </button>
            </div>
          </section>
        </div>

      </div>
    </div>

    <!-- Standard Minimal Modal -->
    <div v-if="modal.show" class="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/90 backdrop-blur-sm animate-in fade-in duration-200">
      <div class="bg-[#0f0f0f] border border-white/10 w-full max-w-sm rounded shadow-2xl p-6">
        <h3 class="text-xs font-black text-white uppercase tracking-widest mb-1">{{ modal.title }}</h3>
        <p class="text-[10px] text-slate-500 mb-4 font-bold uppercase">{{ modal.label }}</p>
        
        <input v-model="modal.value" @keyup.enter="handleModalSubmit" autofocus
          class="w-full bg-black border border-white/10 rounded px-3 py-2 text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-xs mb-4" />
        
        <div v-if="modal.error" class="flex items-center gap-2 text-red-500 text-[9px] mb-4 font-black uppercase">
          <AlertCircle class="w-3 h-3" /> {{ modal.error }}
        </div>

        <div class="flex gap-2">
          <button @click="modal.show = false" class="flex-1 py-2 rounded bg-white/5 hover:bg-white/10 text-slate-500 font-black text-[10px] uppercase transition-all">Cancel</button>
          <button @click="handleModalSubmit" class="flex-1 py-2 rounded bg-indigo-600 hover:bg-indigo-500 text-white font-black text-[10px] uppercase transition-all">Confirm</button>
        </div>
      </div>
    </div>

    <!-- Full Setup Wizard (Hidden if Key exists) -->
    <SetupWizard v-if="!store.setupComplete && !store.apiKey" />
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap');

:root {
  font-family: 'Plus Jakarta Sans', sans-serif;
}

.custom-scrollbar::-webkit-scrollbar {
  width: 3px;
}
.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}
.custom-scrollbar::-webkit-scrollbar-thumb {
  background: rgba(255,255,255,0.05);
  border-radius: 10px;
}

.animate-in {
  animation: animate-in 0.2s ease-out;
}

@keyframes animate-in {
  from { opacity: 0; transform: translateY(4px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>
