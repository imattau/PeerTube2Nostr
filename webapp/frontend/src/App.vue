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
  Settings as SettingsIcon,
  ShieldCheck,
  Zap,
  Terminal,
  Clock,
  ExternalLink,
  RefreshCw
} from 'lucide-vue-next'

const store = useAppStore()
const activeTab = ref('dashboard')

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
  return new Date(ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}
</script>

<template>
  <div class="min-h-screen bg-[#0a0a0a] text-slate-300 font-sans antialiased flex">
    
    <!-- Sidebar -->
    <nav class="w-64 border-r border-white/5 bg-[#0f0f0f] hidden lg:flex flex-col shrink-0">
      <div class="p-6 border-b border-white/5 flex items-center gap-3">
        <div class="bg-indigo-600 p-1.5 rounded shadow-lg shadow-indigo-600/20">
          <Zap class="w-5 h-5 text-white fill-white" />
        </div>
        <span class="font-bold text-white tracking-tight">PeerTube2Nostr</span>
      </div>

      <div class="flex-1 p-4 space-y-1">
        <button v-for="item in [
          { id: 'dashboard', icon: LayoutDashboard, label: 'Overview' },
          { id: 'sources', icon: Rss, label: 'Sources' },
          { id: 'relays', icon: Database, label: 'Relays' },
          { id: 'settings', icon: SettingsIcon, label: 'Settings' }
        ]" :key="item.id" 
        @click="activeTab = item.id"
        :class="[
          'w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors',
          activeTab === item.id ? 'bg-white/5 text-white' : 'text-slate-500 hover:text-slate-200 hover:bg-white/[0.02]'
        ]">
          <component :is="item.icon" class="w-4 h-4" />
          {{ item.label }}
        </button>
      </div>

      <div class="p-4 border-t border-white/5">
        <button @click="promptKey" class="flex items-center gap-3 px-3 py-2 text-slate-500 hover:text-slate-200 transition-colors w-full text-sm">
          <Key class="w-4 h-4" />
          <span>Security Key</span>
        </button>
      </div>
    </nav>

    <!-- Main Content -->
    <div class="flex-1 flex flex-col min-w-0">
      
      <!-- Top Bar -->
      <header class="h-16 border-b border-white/5 bg-[#0f0f0f]/50 backdrop-blur flex items-center justify-between px-8 sticky top-0 z-30">
        <h2 class="text-sm font-semibold text-white capitalize">{{ activeTab }}</h2>
        
        <div class="flex items-center gap-4">
          <div class="flex items-center gap-2 px-3 py-1.5 bg-white/5 border border-white/10 rounded-md">
            <div :class="['w-1.5 h-1.5 rounded-full', store.metrics.status === 'idle' ? 'bg-emerald-500' : 'bg-blue-500 animate-pulse']"></div>
            <span class="text-[11px] font-bold uppercase tracking-wider text-slate-400">{{ store.metrics.status || 'Offline' }}</span>
          </div>
          
          <div class="flex items-center border border-white/10 rounded-md overflow-hidden bg-white/5">
            <button @click="store.startRunner" :disabled="store.metrics.status !== 'stopped'"
              class="p-2 hover:bg-white/5 disabled:opacity-30 transition-colors border-r border-white/10">
              <Play class="w-3.5 h-3.5 text-emerald-500 fill-emerald-500" />
            </button>
            <button @click="store.stopRunner" :disabled="store.metrics.status === 'stopped'"
              class="p-2 hover:bg-white/5 disabled:opacity-30 transition-colors">
              <Square class="w-3.5 h-3.5 text-red-500 fill-red-500" />
            </button>
          </div>
        </div>
      </header>

      <main class="flex-1 overflow-y-auto">
        <div class="max-w-[1200px] mx-auto p-8 space-y-8">
          
          <!-- DASHBOARD -->
          <div v-if="activeTab === 'dashboard'" class="space-y-8 animate-in fade-in duration-500">
            
            <!-- Summary Stats -->
            <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div v-for="(val, label) in { 'Queue': store.metrics.pending, 'Published': store.metrics.posted, 'Failed': store.metrics.failed, 'Sources': store.metrics.sources }" :key="label"
                class="bg-[#141414] border border-white/5 p-5 rounded-lg">
                <p class="text-[11px] font-bold text-slate-500 uppercase tracking-widest">{{ label }}</p>
                <p class="text-2xl font-semibold text-white mt-1">{{ val ?? 0 }}</p>
              </div>
            </div>

            <!-- Queue & Activity Container -->
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
              
              <!-- Pending Queue (Table-like) -->
              <div class="lg:col-span-2 space-y-4">
                <div class="flex items-center justify-between">
                  <h3 class="text-sm font-bold text-white uppercase tracking-tight flex items-center gap-2">
                    <Clock class="w-4 h-4 text-slate-500" />
                    Pending Queue
                  </h3>
                  <span class="text-[10px] text-slate-500 font-medium">{{ store.queue.length }} items</span>
                </div>

                <div class="bg-[#141414] border border-white/5 rounded-lg overflow-hidden">
                  <div v-if="store.queue.length > 0" class="divide-y border-white/5 divide-white/5">
                    <div v-for="item in store.queue" :key="item.id" class="p-4 flex items-center gap-4 hover:bg-white/[0.02] transition-colors group">
                      <div class="w-24 aspect-video rounded overflow-hidden bg-slate-800 shrink-0 border border-white/5">
                        <img v-if="item.thumbnail_url" :src="item.thumbnail_url" class="w-full h-full object-cover" />
                        <div v-else class="w-full h-full flex items-center justify-center"><Play class="w-4 h-4 text-slate-600" /></div>
                      </div>
                      <div class="flex-1 min-w-0">
                        <p class="text-[10px] text-indigo-400 font-bold uppercase tracking-tight truncate">{{ item.channel_name }}</p>
                        <h4 class="text-xs font-semibold text-slate-200 truncate mt-0.5">{{ item.title || item.watch_url }}</h4>
                      </div>
                      <a :href="item.watch_url" target="_blank" class="p-2 text-slate-600 hover:text-slate-300 opacity-0 group-hover:opacity-100 transition-all">
                        <ExternalLink class="w-4 h-4" />
                      </a>
                    </div>
                  </div>
                  <div v-else class="py-12 text-center">
                    <p class="text-xs text-slate-500 font-medium">No items currently in queue</p>
                  </div>
                </div>
              </div>

              <!-- System Activity (Monospace) -->
              <div class="space-y-4">
                <h3 class="text-sm font-bold text-white uppercase tracking-tight flex items-center gap-2">
                  <Terminal class="w-4 h-4 text-slate-500" />
                  Activity
                </h3>
                <div class="bg-black border border-white/5 rounded-lg p-4 font-mono text-[11px] h-[400px] overflow-y-auto custom-scrollbar flex flex-col-reverse divide-y divide-white/5">
                  <div v-for="(log, i) in store.logs.slice().reverse()" :key="i" class="py-2 first:pt-0 last:pb-0 text-slate-400">
                    <span class="text-slate-600 mr-2">[{{ store.logs.length - i }}]</span>
                    {{ log }}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- SOURCES -->
          <div v-if="activeTab === 'sources'" class="space-y-6 animate-in fade-in duration-500">
            <div class="flex justify-between items-center border-b border-white/5 pb-4">
              <h3 class="text-lg font-semibold text-white">Configured Sources</h3>
              <button @click="addSource" class="bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold px-4 py-2 rounded transition-colors flex items-center gap-2">
                <PlusCircle class="w-3.5 h-3.5" /> Add Source
              </button>
            </div>
            
            <div class="bg-[#141414] border border-white/5 rounded-lg overflow-hidden">
              <table class="w-full text-left text-xs border-collapse">
                <thead class="bg-white/5 text-slate-500 font-bold uppercase tracking-wider border-b border-white/5">
                  <tr>
                    <th class="px-6 py-3 font-bold">Source ID</th>
                    <th class="px-6 py-3 font-bold">URL / Channel</th>
                    <th class="px-6 py-3 font-bold">Last Polled</th>
                    <th class="px-6 py-3 font-bold">Status</th>
                    <th class="px-6 py-3 text-right font-bold">Actions</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-white/5">
                  <tr v-for="source in store.sources" :key="source.id" class="hover:bg-white/[0.01] group">
                    <td class="px-6 py-4 text-slate-500 font-mono">{{ source.id }}</td>
                    <td class="px-6 py-4">
                      <div class="font-semibold text-slate-200 max-w-[300px] truncate">{{ source.api_channel_url || source.rss_url }}</div>
                    </td>
                    <td class="px-6 py-4 text-slate-400">{{ formatTs(source.last_polled_ts) }}</td>
                    <td class="px-6 py-4">
                      <span v-if="source.enabled" class="text-emerald-500 font-bold flex items-center gap-1.5 uppercase text-[10px]">
                        <CheckCircle2 class="w-3 h-3" /> Enabled
                      </span>
                      <span v-else class="text-slate-600 font-bold flex items-center gap-1.5 uppercase text-[10px]">
                        <XCircle class="w-3 h-3" /> Disabled
                      </span>
                    </td>
                    <td class="px-6 py-4 text-right">
                      <div class="flex items-center justify-end gap-2">
                        <button @click="store.toggleSource(source.id, !source.enabled)" class="p-1.5 hover:bg-white/5 rounded transition-colors text-slate-500 hover:text-white">
                          <RefreshCw class="w-3.5 h-3.5" />
                        </button>
                        <button @click="store.deleteSource(source.id)" class="p-1.5 hover:bg-red-500/10 rounded transition-colors text-slate-500 hover:text-red-500">
                          <Trash2 class="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- RELAYS -->
          <div v-if="activeTab === 'relays'" class="space-y-6 animate-in fade-in duration-500">
            <div class="flex justify-between items-center border-b border-white/5 pb-4">
              <h3 class="text-lg font-semibold text-white">Nostr Relays</h3>
              <button @click="addRelay" class="bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold px-4 py-2 rounded transition-colors flex items-center gap-2">
                <PlusCircle class="w-3.5 h-3.5" /> Add Relay
              </button>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div v-for="relay in store.relays" :key="relay.id" class="bg-[#141414] border border-white/5 p-5 rounded-lg flex items-center justify-between group">
                <div class="min-w-0">
                  <div class="flex items-center gap-3">
                    <span class="font-semibold text-slate-200 truncate">{{ relay.relay_url }}</span>
                    <span v-if="relay.latency_ms" class="text-[10px] bg-indigo-500/10 text-indigo-400 font-bold px-1.5 py-0.5 rounded border border-indigo-500/20">
                      {{ relay.latency_ms }}ms
                    </span>
                  </div>
                  <p class="text-[10px] text-slate-500 font-bold uppercase mt-1 tracking-tight">Relay Node</p>
                </div>
                <div class="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button @click="store.toggleRelay(relay.id, !relay.enabled)" :class="relay.enabled ? 'text-emerald-500' : 'text-slate-600'" class="p-2 hover:bg-white/5 rounded">
                    <CheckCircle2 class="w-4 h-4" />
                  </button>
                  <button @click="store.deleteRelay(relay.id)" class="p-2 hover:bg-red-500/10 rounded text-slate-600 hover:text-red-500">
                    <Trash2 class="w-4 h-4" />
                  </button>
                </div>
              </div>
            </div>
          </div>

          <!-- SETTINGS -->
          <div v-if="activeTab === 'settings'" class="max-w-xl animate-in fade-in duration-500 space-y-8">
            <section class="space-y-4">
              <h3 class="text-sm font-bold text-white uppercase tracking-tight">Identity & Signing</h3>
              <div class="bg-[#141414] border border-white/5 p-6 rounded-lg space-y-6">
                <div v-if="store.metrics.has_nsec" class="flex items-center justify-between p-4 bg-emerald-500/5 border border-emerald-500/10 rounded-md">
                  <div class="flex items-center gap-3">
                    <ShieldCheck class="w-5 h-5 text-emerald-500" />
                    <div>
                      <p class="text-xs font-bold text-emerald-500 uppercase tracking-wider">Secure Signing Active</p>
                      <p class="text-[11px] text-emerald-500/60 font-medium">NSEC is configured and encrypted on server</p>
                    </div>
                  </div>
                  <button @click="setNsec" class="text-[10px] font-bold text-emerald-500 hover:underline underline-offset-4 uppercase tracking-widest">Update</button>
                </div>
                <div v-else>
                  <button @click="setNsec" class="w-full bg-indigo-600 py-3 rounded text-xs font-bold text-white uppercase tracking-[0.1em]">Configure Private Key</button>
                </div>
              </div>
            </section>

            <section class="space-y-4 pt-4 border-t border-white/5">
              <h3 class="text-sm font-bold text-white uppercase tracking-tight text-red-500/80">Maintenance</h3>
              <div class="grid grid-cols-2 gap-4">
                <button class="bg-[#141414] hover:bg-white/5 border border-white/5 py-3 rounded text-[10px] font-bold uppercase tracking-widest transition-colors">Repair DB</button>
                <button class="bg-[#141414] hover:bg-red-500/10 border border-white/5 py-3 rounded text-[10px] font-bold uppercase tracking-widest text-red-500 transition-colors">Clear History</button>
              </div>
            </section>
          </div>

        </div>
      </main>
    </div>

    <!-- Modal -->
    <div v-if="modal.show" class="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
      <div class="bg-[#141414] border border-white/10 w-full max-w-md rounded-lg shadow-2xl p-8">
        <h3 class="text-sm font-bold text-white uppercase tracking-wider mb-1">{{ modal.title }}</h3>
        <p class="text-xs text-slate-500 mb-6 font-medium">{{ modal.label }}</p>
        
        <input v-model="modal.value" @keyup.enter="handleModalSubmit" autofocus
          class="w-full bg-black border border-white/10 rounded-md px-4 py-2.5 text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm mb-4" />
        
        <div v-if="modal.error" class="flex items-center gap-2 text-red-400 text-[11px] mb-6 font-bold uppercase tracking-tight">
          <AlertCircle class="w-3.5 h-3.5" />
          {{ modal.error }}
        </div>

        <div class="flex gap-3">
          <button @click="modal.show = false" class="flex-1 px-4 py-2 rounded bg-white/5 hover:bg-white/10 text-slate-400 font-bold text-[10px] uppercase tracking-widest transition-all">Cancel</button>
          <button @click="handleModalSubmit" class="flex-1 px-4 py-2 rounded bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-[10px] uppercase tracking-widest transition-all">Confirm</button>
        </div>
      </div>
    </div>

    <SetupWizard v-if="!store.setupComplete" />
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap');

:root {
  font-family: 'Inter', sans-serif;
}

.custom-scrollbar::-webkit-scrollbar {
  width: 4px;
}
.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}
.custom-scrollbar::-webkit-scrollbar-thumb {
  background: rgba(255,255,255,0.1);
  border-radius: 10px;
}

.animate-in {
  animation: animate-in 0.3s ease-out;
}

@keyframes animate-in {
  from { opacity: 0; transform: translateY(4px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>