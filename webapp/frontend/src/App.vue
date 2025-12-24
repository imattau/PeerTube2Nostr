<script setup lang="ts">
import { onMounted, onUnmounted, ref, reactive } from 'vue'
import { useAppStore } from './store/app'
import { 
  Activity, 
  Settings, 
  Database, 
  Rss, 
  Key, 
  ListRestart,
  PlusCircle,
  Trash2,
  CheckCircle2,
  XCircle,
  Play,
  Square,
  AlertCircle
} from 'lucide-vue-next'

const store = useAppStore()

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
  
  // Basic validation
  if (modal.title.includes('Relay') && !modal.value.startsWith('ws')) {
    modal.error = 'Relay must start with ws:// or wss://'
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

const formatTs = (ts: number | null) => {
  if (!ts) return 'Never'
  return new Date(ts * 1000).toLocaleTimeString()
}
</script>

<template>
  <div class="min-h-screen bg-slate-950 text-slate-200 font-sans p-4 md:p-8">
    <header class="max-w-6xl mx-auto mb-8 flex justify-between items-center">
      <div class="flex items-center gap-3">
        <div class="bg-indigo-600 p-2 rounded-lg">
          <Activity class="w-6 h-6 text-white" />
        </div>
        <h1 class="text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-400">
          PeerTube2Nostr
        </h1>
      </div>
      <div class="flex items-center gap-4">
        <div class="flex bg-slate-900 rounded-xl p-1 border border-slate-800">
          <button 
            @click="store.startRunner" 
            :disabled="store.metrics.status !== 'stopped'"
            class="p-2 rounded-lg transition-colors"
            :class="store.metrics.status !== 'stopped' ? 'text-slate-600' : 'text-emerald-500 hover:bg-emerald-500/10'"
          >
            <Play class="w-5 h-5" />
          </button>
          <button 
            @click="store.stopRunner" 
            :disabled="store.metrics.status === 'stopped'"
            class="p-2 rounded-lg transition-colors"
            :class="store.metrics.status === 'stopped' ? 'text-slate-600' : 'text-red-500 hover:bg-red-500/10'"
          >
            <Square class="w-5 h-5" />
          </button>
        </div>
        <span :class="[
          'px-3 py-1 rounded-full text-xs font-medium uppercase tracking-wider border',
          store.metrics.status === 'idle' ? 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20' : 
          store.metrics.status === 'stopped' ? 'bg-slate-500/10 text-slate-400 border-slate-500/20' : 
          'bg-indigo-500/10 text-indigo-400 border-indigo-500/20 animate-pulse'
        ]">
          {{ store.metrics.status || 'Unknown' }}
        </span>
      </div>
    </header>

    <main class="max-w-6xl mx-auto grid grid-cols-1 md:grid-cols-3 gap-6">
      <!-- Stats Overview -->
      <section class="md:col-span-3 grid grid-cols-2 md:grid-cols-4 gap-4">
        <div class="bg-slate-900/50 border border-slate-800 p-4 rounded-2xl">
          <p class="text-slate-500 text-sm mb-1">Queue / Posted</p>
          <p class="text-2xl font-bold">{{ store.metrics.pending ?? 0 }} / {{ store.metrics.posted ?? 0 }}</p>
        </div>
        <div class="bg-slate-900/50 border border-slate-800 p-4 rounded-2xl">
          <p class="text-slate-500 text-sm mb-1">Last Activity</p>
          <p class="text-xl font-semibold truncate">{{ formatTs(store.metrics.last_posted_ts) }}</p>
        </div>
        <div class="bg-slate-900/50 border border-slate-800 p-4 rounded-2xl">
          <p class="text-slate-500 text-sm mb-1">Next Poll</p>
          <p class="text-xl font-semibold">{{ formatTs(store.metrics.last_poll_ts ? store.metrics.last_poll_ts + 300 : null) }}</p>
        </div>
        <div class="bg-slate-900/50 border border-slate-800 p-4 rounded-2xl">
          <p class="text-slate-500 text-sm mb-1">Failed</p>
          <p class="text-2xl font-bold text-red-400">{{ store.metrics.failed ?? 0 }}</p>
        </div>
      </section>

      <!-- Sources -->
      <section class="md:col-span-2 bg-slate-900/50 border border-slate-800 rounded-3xl p-6">
        <div class="flex justify-between items-center mb-6">
          <div class="flex items-center gap-2">
            <Rss class="w-5 h-5 text-indigo-400" />
            <h2 class="text-xl font-semibold">Sources</h2>
          </div>
          <button @click="addSource" class="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-xl text-sm font-medium transition-colors flex items-center gap-2">
            <PlusCircle class="w-4 h-4" /> Add Source
          </button>
        </div>
        <div class="space-y-3">
          <div v-for="source in store.sources" :key="source.id" class="bg-slate-800/40 p-4 rounded-2xl flex justify-between items-center group">
            <div class="min-w-0">
              <p class="font-medium truncate">{{ source.api_channel_url || source.rss_url }}</p>
              <p class="text-xs text-slate-500">Last poll: {{ formatTs(source.last_polled_ts) }}</p>
            </div>
            <div class="flex items-center gap-3">
              <button @click="store.toggleSource(source.id, !source.enabled)" :class="source.enabled ? 'text-emerald-500' : 'text-slate-600'">
                <CheckCircle2 v-if="source.enabled" class="w-5 h-5" />
                <XCircle v-else class="w-5 h-5" />
              </button>
              <button @click="store.deleteSource(source.id)" class="text-slate-600 hover:text-red-400 transition-colors">
                <Trash2 class="w-5 h-5" />
              </button>
            </div>
          </div>
        </div>
      </section>

      <!-- Relays & Nsec -->
      <section class="space-y-6">
        <div class="bg-slate-900/50 border border-slate-800 rounded-3xl p-6">
          <div class="flex justify-between items-center mb-6">
            <div class="flex items-center gap-2">
              <Database class="w-5 h-5 text-indigo-400" />
              <h2 class="text-xl font-semibold">Relays</h2>
            </div>
            <button @click="addRelay" class="text-indigo-400 hover:text-indigo-300">
              <PlusCircle class="w-5 h-5" />
            </button>
          </div>
          <div class="space-y-3">
            <div v-for="relay in store.relays" :key="relay.id" class="flex justify-between items-center bg-slate-800/40 p-3 rounded-xl">
              <div class="flex flex-col min-w-0">
                <span class="text-sm truncate">{{ relay.relay_url }}</span>
                <span v-if="relay.latency_ms" class="text-[10px] text-slate-500">
                  {{ relay.latency_ms }}ms
                </span>
              </div>
              <div class="flex items-center gap-2">
                 <button @click="store.toggleRelay(relay.id, !relay.enabled)" :class="relay.enabled ? 'text-emerald-500' : 'text-slate-600'">
                    <CheckCircle2 class="w-4 h-4" />
                  </button>
                  <button @click="store.deleteRelay(relay.id)" class="text-slate-600">
                    <Trash2 class="w-4 h-4" />
                  </button>
              </div>
            </div>
          </div>
        </div>

        <div class="bg-slate-900/50 border border-slate-800 rounded-3xl p-6">
          <div class="flex items-center gap-2 mb-4">
            <Key class="w-5 h-5 text-amber-400" />
            <h2 class="text-xl font-semibold">Credentials</h2>
          </div>
          <div v-if="store.metrics.has_nsec" class="bg-emerald-500/10 border border-emerald-500/20 p-3 rounded-xl flex items-center gap-2">
            <CheckCircle2 class="w-4 h-4 text-emerald-500" />
            <span class="text-sm text-emerald-500">NSEC Configured</span>
          </div>
          <button @click="setNsec" class="w-full mt-3 bg-slate-800 hover:bg-slate-700 text-white py-2 rounded-xl text-sm transition-colors">
            Update NSEC
          </button>
        </div>
      </section>

      <!-- Pending Queue -->
      <section class="md:col-span-3 bg-slate-900/50 border border-slate-800 rounded-3xl p-6">
        <div class="flex items-center gap-2 mb-6">
          <ListRestart class="w-5 h-5 text-indigo-400" />
          <h2 class="text-xl font-semibold">Pending Queue</h2>
        </div>
        <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4">
          <div v-for="item in store.queue" :key="item.id" class="bg-slate-800/40 rounded-2xl overflow-hidden border border-slate-800 group hover:border-indigo-500/50 transition-colors">
            <div class="aspect-video relative overflow-hidden bg-slate-800">
              <img v-if="item.thumbnail_url" :src="item.thumbnail_url" class="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500" />
              <div v-else class="w-full h-full flex items-center justify-center">
                <Play class="w-8 h-8 text-slate-700" />
              </div>
            </div>
            <div class="p-3">
              <p class="text-[10px] text-indigo-400 font-medium uppercase truncate">{{ item.channel_name }}</p>
              <p class="text-xs font-semibold line-clamp-2 mt-1 leading-snug">{{ item.title || item.watch_url }}</p>
            </div>
          </div>
          <div v-if="store.queue.length === 0" class="col-span-full py-12 text-center text-slate-500 italic">
            Queue is empty. New videos will appear here.
          </div>
        </div>
      </section>

      <!-- Logs -->
      <section class="md:col-span-3 bg-slate-900/50 border border-slate-800 rounded-3xl p-6">
        <div class="flex justify-between items-center mb-4">
          <div class="flex items-center gap-2">
            <Activity class="w-5 h-5 text-indigo-400" />
            <h2 class="text-xl font-semibold">System Logs</h2>
          </div>
          <button @click="store.setApiKey(prompt('Enter API Key') || '')" class="text-xs text-slate-500 hover:text-indigo-400 flex items-center gap-1">
             <Key class="w-3 h-3" /> {{ store.apiKey ? 'Change Key' : 'Set Key' }}
          </button>
        </div>
        <div class="bg-black/40 rounded-2xl p-4 font-mono text-xs h-48 overflow-y-auto space-y-1">
          <div v-for="(log, i) in store.logs.slice().reverse()" :key="i" class="text-slate-400 border-l border-slate-800 pl-3">
            <span class="text-indigo-500/50 mr-2">{{ store.logs.length - i }}</span> {{ log }}
          </div>
        </div>
      </section>
    </main>

    <!-- Modal Overlay -->
    <div v-if="modal.show" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm">
      <div class="bg-slate-900 border border-slate-800 w-full max-w-md rounded-3xl p-8 shadow-2xl">
        <h3 class="text-xl font-bold mb-2">{{ modal.title }}</h3>
        <p class="text-slate-400 text-sm mb-6">{{ modal.label }}</p>
        
        <input 
          v-model="modal.value" 
          @keyup.enter="handleModalSubmit"
          class="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-3 text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 mb-2"
          :placeholder="modal.label"
          autofocus
        />
        
        <div v-if="modal.error" class="flex items-center gap-2 text-red-400 text-xs mb-4">
          <AlertCircle class="w-4 h-4" />
          {{ modal.error }}
        </div>

        <div class="flex gap-3">
          <button @click="modal.show = false" class="flex-1 px-4 py-3 rounded-xl bg-slate-800 hover:bg-slate-700 font-medium transition-colors">
            Cancel
          </button>
          <button @click="handleModalSubmit" class="flex-1 px-4 py-3 rounded-xl bg-indigo-600 hover:bg-indigo-500 font-medium transition-colors">
            Confirm
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<style>
::-webkit-scrollbar {
  width: 8px;
}
::-webkit-scrollbar-track {
  background: transparent;
}
::-webkit-scrollbar-thumb {
  background: #1e293b;
  border-radius: 10px;
}
::-webkit-scrollbar-thumb:hover {
  background: #334155;
}
</style>