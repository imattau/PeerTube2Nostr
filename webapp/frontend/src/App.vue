<script setup lang="ts">
import { onMounted, onUnmounted, reactive, ref, computed } from 'vue'
import { useAppStore } from './store/app'
import SetupWizard from './components/SetupWizard.vue'

// UI Components
import AppHeader from './components/ui/AppHeader.vue'
import AlertBanner from './components/ui/AlertBanner.vue'
import MetricCard from './components/ui/MetricCard.vue'
import SectionCard from './components/ui/SectionCard.vue'
import EmptyState from './components/ui/EmptyState.vue'
import BaseModal from './components/ui/BaseModal.vue'
import Button from './components/ui/Button.vue'
import Badge from './components/ui/Badge.vue'

import { 
  Rss, 
  PlusCircle,
  Trash2,
  CheckCircle2,
  Play,
  Square,
  AlertCircle,
  ShieldCheck,
  Zap,
  Terminal,
  ExternalLink,
  RefreshCw,
  Clock,
  Database
} from 'lucide-vue-next'

const store = useAppStore()
const showApiKeyBanner = ref(true)

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
const openApiKeyModal = () => openModal('API Security', 'Paste your API security key', store.setApiKey)

const formatTs = (ts: number | null) => {
  if (!ts) return '-'
  return new Date(ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

const statusVariant = computed(() => {
  const s = store.metrics.status
  if (s === 'idle') return 'success'
  if (s === 'stopped') return 'error'
  return 'info'
})
</script>

<template>
  <div class="min-h-screen bg-[#050505] text-slate-300 font-sans selection:bg-indigo-500/30 antialiased">
    
    <!-- Header System -->
    <AppHeader 
      :status="store.metrics.status" 
      :status-variant="statusVariant"
      @add-source="addSource"
      @configure-access="openApiKeyModal"
    />

    <!-- Main Content Container (Centered & Constrained) -->
    <main class="max-w-[1120px] mx-auto px-6 py-10 space-y-8">
      
      <!-- Inline Alert Banner -->
      <AlertBanner 
        v-if="!store.apiKey && showApiKeyBanner"
        @action="openApiKeyModal"
        @close="showApiKeyBanner = false"
      />

      <!-- Metrics Row: Structured Grid -->
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard title="Queue" :value="store.metrics.pending ?? 0" />
        <MetricCard title="Published" :value="store.metrics.posted ?? 0" />
        <MetricCard title="Failed" :value="store.metrics.failed ?? 0" />
        <MetricCard title="Sources" :value="store.metrics.sources ?? 0" />
      </div>

      <!-- Content Grid: 2 Columns -->
      <div class="grid grid-cols-1 lg:grid-cols-12 gap-8 items-start">
        
        <!-- Primary Column (8 Units) -->
        <div class="lg:col-span-8 space-y-8">
          
          <!-- Pending Queue Card -->
          <SectionCard title="Pending Queue" :icon="Clock">
            <div v-if="store.queue.length > 0" class="divide-y divide-white/[0.03]">
              <div v-for="item in store.queue" :key="item.id" 
                class="flex items-center gap-4 py-3 group first:pt-0 last:pb-0 transition-colors">
                <div class="w-16 aspect-video rounded bg-slate-900 overflow-hidden shrink-0 border border-white/5">
                  <img v-if="item.thumbnail_url" :src="item.thumbnail_url" class="w-full h-full object-cover opacity-80 group-hover:opacity-100 transition-opacity" />
                  <div v-else class="w-full h-full flex items-center justify-center text-slate-700"><Play class="w-3 h-3 fill-current" /></div>
                </div>
                <div class="flex-1 min-w-0">
                  <div class="flex items-center gap-2">
                    <span class="text-[10px] font-bold text-indigo-500 uppercase tracking-tighter">{{ item.channel_name }}</span>
                  </div>
                  <h4 class="text-[13px] font-semibold text-slate-200 truncate mt-0.5">{{ item.title || item.watch_url }}</h4>
                </div>
                <a :href="item.watch_url" target="_blank" class="p-2 text-slate-600 hover:text-slate-300 transition-colors">
                  <ExternalLink class="w-3.5 h-3.5" />
                </a>
              </div>
            </div>
            <EmptyState v-else title="Queue is clean" message="Add a source to start publishing." :icon="CheckCircle2" />
          </SectionCard>

          <!-- System Activity Card -->
          <SectionCard title="Recent Activity" :icon="Terminal">
            <div v-if="store.logs.length > 0" class="space-y-2 max-h-[300px] overflow-y-auto custom-scrollbar flex flex-col-reverse">
              <div v-for="(log, i) in store.logs.slice().reverse()" :key="i" class="flex gap-3 font-mono text-[11px] py-1">
                <span class="text-slate-700 shrink-0 font-bold w-8 text-right">#{{ store.logs.length - i }}</span>
                <span class="text-slate-500 leading-relaxed">{{ log }}</span>
              </div>
            </div>
            <EmptyState v-else title="No recent activity" :icon="Terminal" />
          </SectionCard>
        </div>

        <!-- Secondary Column (4 Units) -->
        <div class="lg:col-span-4 space-y-8">
          
          <!-- Control Surface -->
          <SectionCard title="Automation" :icon="Zap">
            <div class="space-y-4">
              <div class="flex items-center justify-between p-3 bg-white/[0.02] rounded-lg border border-white/[0.04]">
                <span class="text-[13px] font-medium text-slate-400">Runner</span>
                <Badge :variant="statusVariant">{{ store.metrics.status || 'Offline' }}</Badge>
              </div>
              <div class="grid grid-cols-2 gap-2">
                <Button variant="secondary" @click="store.startRunner" :disabled="store.metrics.status !== 'stopped'">
                  <Play class="w-3.5 h-3.5 mr-2 text-emerald-500 fill-emerald-500" /> Start
                </Button>
                <Button variant="secondary" @click="store.stopRunner" :disabled="store.metrics.status === 'stopped'">
                  <Square class="w-3.5 h-3.5 mr-2 text-red-500 fill-red-500" /> Stop
                </Button>
              </div>
            </div>
          </SectionCard>

          <!-- Channels List -->
          <SectionCard title="Active Sources" :icon="Rss">
            <template #actions>
              <button @click="addSource" class="text-slate-500 hover:text-white transition-colors"><PlusCircle class="w-4 h-4" /></button>
            </template>
            <div v-if="store.sources.length > 0" class="divide-y divide-white/[0.03]">
              <div v-for="source in store.sources" :key="source.id" class="py-2.5 flex items-center justify-between group first:pt-0 last:pb-0">
                <div class="min-w-0">
                  <p class="text-[13px] font-semibold text-slate-200 truncate pr-4">{{ source.api_channel_url || source.rss_url }}</p>
                  <p class="text-[11px] text-slate-600 font-medium mt-0.5 uppercase tracking-tighter">Polled: {{ formatTs(source.last_polled_ts) }}</p>
                </div>
                <div class="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button @click="store.toggleSource(source.id, !source.enabled)" :class="source.enabled ? 'text-emerald-500' : 'text-slate-600'" class="p-1.5 hover:bg-white/5 rounded">
                    <RefreshCw class="w-3.5 h-3.5" />
                  </button>
                  <button @click="store.deleteSource(source.id)" class="p-1.5 hover:bg-red-500/10 text-slate-600 hover:text-red-500 rounded transition-colors">
                    <Trash2 class="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
            </div>
            <EmptyState v-else title="No sources" :icon="Rss" />
          </SectionCard>

          <!-- Identity Card -->
          <SectionCard title="Nostr Identity" :icon="ShieldCheck">
            <div v-if="store.metrics.has_nsec" class="p-3 bg-emerald-500/5 border border-emerald-500/10 rounded-lg flex items-center justify-between">
              <div class="flex items-center gap-2 text-emerald-500">
                <CheckCircle2 class="w-4 h-4" />
                <span class="text-[13px] font-semibold uppercase tracking-tight">Active</span>
              </div>
              <button @click="setNsec" class="text-[11px] font-bold text-emerald-500/70 hover:text-emerald-500 hover:underline transition-all">REPLACE</button>
            </div>
            <Button v-else variant="primary" class="w-full" @click="setNsec">Configure Private Key</Button>
          </SectionCard>

          <!-- Nostr Relays -->
          <SectionCard title="Relays" :icon="Database">
            <template #actions>
              <button @click="addRelay" class="text-slate-500 hover:text-white transition-colors"><PlusCircle class="w-4 h-4" /></button>
            </template>
            <div v-if="store.relays.length > 0" class="space-y-2">
              <div v-for="relay in store.relays" :key="relay.id" class="flex items-center justify-between p-2.5 rounded-lg bg-white/[0.02] group">
                <div class="min-w-0">
                  <p class="text-[11px] font-bold text-slate-200 truncate">{{ relay.relay_url.replace('wss://', '') }}</p>
                  <div class="flex items-center gap-2 mt-0.5">
                    <span v-if="relay.latency_ms" class="text-[9px] text-indigo-500 font-black uppercase">{{ relay.latency_ms }}ms</span>
                    <span class="w-1 h-1 bg-white/10 rounded-full"></span>
                    <span class="text-[9px] text-slate-600 font-bold uppercase tracking-tighter">{{ relay.enabled ? 'Active' : 'Disabled' }}</span>
                  </div>
                </div>
                <div class="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button @click="store.toggleRelay(relay.id, !relay.enabled)" :class="relay.enabled ? 'text-emerald-500' : 'text-slate-600'" class="p-1 hover:bg-white/5 rounded"><CheckCircle2 class="w-3.5 h-3.5" /></button>
                  <button @click="store.deleteRelay(relay.id)" class="p-1 text-slate-600 hover:text-red-500"><Trash2 class="w-3.5 h-3.5" /></button>
                </div>
              </div>
            </div>
            <EmptyState v-else title="No relays" :icon="Database" />
          </SectionCard>
        </div>
      </div>
    </main>

    <!-- Global Modal Implementation -->
    <BaseModal :show="modal.show" :title="modal.title" @close="modal.show = false">
      <div class="space-y-5">
        <div>
          <label class="block text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-2.5">{{ modal.label }}</label>
          <input 
            v-model="modal.value" 
            @keyup.enter="handleModalSubmit"
            class="w-full bg-[#050505] border border-white/10 rounded-md px-4 py-2.5 text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-[13px]"
            :placeholder="modal.label"
            autofocus
          />
        </div>
        
        <div v-if="modal.error" class="flex items-center gap-3 text-red-500 text-[11px] font-bold uppercase p-3.5 bg-red-500/5 rounded-md border border-red-500/10">
          <AlertCircle class="w-4 h-4 shrink-0" /> {{ modal.error }}
        </div>

        <div class="flex gap-2.5 pt-2">
          <Button variant="secondary" size="md" class="flex-1" @click="modal.show = false">Cancel</Button>
          <Button variant="primary" size="md" class="flex-1" @click="handleModalSubmit">Confirm</Button>
        </div>
      </div>
    </BaseModal>

    <!-- setup wizard check -->
    <SetupWizard v-if="!store.setupComplete && !store.apiKey" />
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap');

:root {
  font-family: 'Plus Jakarta Sans', sans-serif;
}

.custom-scrollbar::-webkit-scrollbar {
  width: 2px;
}
.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}
.custom-scrollbar::-webkit-scrollbar-thumb {
  background: rgba(255,255,255,0.08);
}

.animate-in {
  animation: animate-in 0.3s ease-out;
}

@keyframes animate-in {
  from { opacity: 0; transform: translateY(4px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>