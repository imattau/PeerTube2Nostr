<script setup lang="ts">
import { onMounted, onUnmounted, reactive, ref, computed } from 'vue'
import { useAppStore } from './store/app'
import SetupWizard from './components/SetupWizard.vue'
import AppShell from './components/ui/AppShell.vue'
import TopBar from './components/ui/TopBar.vue'
import AlertBanner from './components/ui/AlertBanner.vue'
import MetricCard from './components/ui/MetricCard.vue'
import Card from './components/ui/Card.vue'
import CardHeader from './components/ui/CardHeader.vue'
import EmptyState from './components/ui/EmptyState.vue'
import BaseModal from './components/ui/BaseModal.vue'
import Button from './components/ui/Button.vue'
import Badge from './components/ui/Badge.vue'
import { PlusCircle, Rss, Trash2, CheckCircle2, Play, Square, ShieldCheck, Zap, Terminal, ExternalLink, Clock, Database, Key } from 'lucide-vue-next'

const store = useAppStore()
const showApiKeyBanner = ref(true)
const modal = reactive({ show: false, title: '', label: '', value: '', action: null as Function | null, error: '' })

const openModal = (title: string, label: string, action: Function) => {
  Object.assign(modal, { show: true, title, label, action, value: '', error: '' })
}

const handleModalSubmit = async () => {
  if (!modal.value) { modal.error = 'Field is required'; return }
  try {
    if (modal.action) await modal.action(modal.value)
    modal.show = false
  } catch (e: any) {
    modal.error = e.response?.data?.detail || 'An error occurred'
  }
}

let pollInterval: any
onMounted(async () => {
  await store.fetchSetupStatus()
  if (store.isSetupComplete) {
    store.fetchAll()
    pollInterval = setInterval(() => { store.fetchAll(); store.fetchLogs() }, 5000)
  }
})
onUnmounted(() => clearInterval(pollInterval))

const addSource = () => openModal('Add Source', 'PeerTube Channel or RSS URL', store.addSource)
const addRelay = () => openModal('Add Relay', 'Relay URL (wss://...)', store.addRelay)
const setNsec = () => openModal('Set NSEC', 'nsec1...', store.updateNsec)
const openApiKeyModal = () => openModal('API Security Key', 'Paste your API key', store.setApiKey)

const statusVariant = computed(() => store.metrics.status === 'idle' ? 'success' : store.metrics.status === 'stopped' ? 'error' : 'info')
</script>

<template>
  <AppShell>
    <div v-if="!store.isSetupComplete" class="fixed inset-0 z-[100]"><SetupWizard /></div>
    <div v-else>
      <TopBar :status="store.metrics.status" :status-variant="statusVariant" @add-source="addSource" @configure-access="openApiKeyModal" />
      <div class="py-8 space-y-6">
        <AlertBanner v-if="!store.apiKey && showApiKeyBanner" @action="openApiKeyModal" @close="showApiKeyBanner = false" />
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <MetricCard title="Queue" :value="store.metrics.pending ?? 0" />
          <MetricCard title="Published" :value="store.metrics.posted ?? 0" />
          <MetricCard title="Failed" :value="store.metrics.failed ?? 0" />
          <MetricCard title="Sources" :value="store.metrics.sources ?? 0" />
        </div>
        <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
          <div class="lg:col-span-7 space-y-6">
            <Card>
              <CardHeader title="Pending Queue" :icon="Clock" />
              <div class="p-4">
                <div v-if="store.queue.length > 0" class="divide-y divide-border-subtle">
                  <div v-for="item in store.queue" :key="item.id" class="flex items-center gap-4 py-3 first:pt-0 last:pb-0">
                    <img v-if="item.thumbnail_url" :src="item.thumbnail_url" class="w-16 h-9 object-cover rounded bg-surface-2" />
                    <div v-else class="w-16 h-9 rounded bg-surface-2 flex items-center justify-center"><Play class="w-3 h-3 text-slate-700" /></div>
                    <div class="flex-1 min-w-0">
                      <p class="text-[10px] font-bold text-accent uppercase">{{ item.channel_name }}</p>
                      <h4 class="text-[13px] font-semibold text-text-primary truncate">{{ item.title || item.watch_url }}</h4>
                    </div>
                    <a :href="item.watch_url" target="_blank" class="p-2 text-text-muted hover:text-text-primary"><ExternalLink class="w-3.5 h-3.5" /></a>
                  </div>
                </div>
                <EmptyState v-else title="Queue is clean" :icon="CheckCircle2" />
              </div>
            </Card>
            <Card>
              <CardHeader title="Recent Activity" :icon="Terminal" />
              <div class="p-4">
                <div v-if="store.logs.length > 0" class="space-y-2 text-xs h-64 overflow-y-auto custom-scrollbar flex flex-col-reverse">
                  <div v-for="(log, i) in store.logs.slice().reverse()" :key="i" class="font-mono text-text-muted leading-relaxed">
                    <span class="text-slate-700 mr-2">#{{ store.logs.length - i }}</span>{{ log }}
                  </div>
                </div>
                <EmptyState v-else title="No recent activity" :icon="Terminal" />
              </div>
            </Card>
          </div>
          <div class="lg:col-span-5 space-y-6">
            <Card>
              <CardHeader title="Automation" :icon="Zap" />
              <div class="p-4 space-y-4">
                <div class="grid grid-cols-2 gap-2">
                  <Button variant="secondary" @click="store.startRunner" :disabled="store.metrics.status !== 'stopped'"><Play class="w-3.5 h-3.5 mr-2" /> Start</Button>
                  <Button variant="secondary" @click="store.stopRunner" :disabled="store.metrics.status === 'stopped'"><Square class="w-3.5 h-3.5 mr-2" /> Stop</Button>
                </div>
              </div>
            </Card>
            <Card>
              <CardHeader title="Active Sources" :icon="Rss">
                <template #actions><Button size="sm" variant="secondary" @click="addSource"><PlusCircle class="w-3.5 h-3.5" /></Button></template>
              </CardHeader>
              <div class="p-4">
                <div v-if="store.sources.length > 0" class="divide-y divide-border-subtle">
                  <div v-for="source in store.sources" :key="source.id" class="py-2 flex items-center justify-between group">
                    <p class="text-[13px] font-semibold text-text-primary truncate max-w-xs">{{ source.api_channel_url || source.rss_url }}</p>
                    <div class="flex opacity-0 group-hover:opacity-100 transition-opacity"><Button variant="ghost" size="sm" @click="store.deleteSource(source.id)"><Trash2 class="w-3.5 h-3.5" /></Button></div>
                  </div>
                </div>
                <EmptyState v-else title="No sources" :icon="Rss" />
              </div>
            </Card>
            <Card>
              <CardHeader title="Nostr Identity" :icon="ShieldCheck">
                <template #actions><Button size="sm" variant="secondary" @click="setNsec">Configure</Button></template>
              </CardHeader>
              <div class="p-4">
                <Badge v-if="store.metrics.has_nsec" variant="success">NSEC Configured</Badge>
                <EmptyState v-else title="Not configured" :icon="ShieldCheck" />
              </div>
            </Card>
            <Card>
              <CardHeader title="Nostr Relays" :icon="Database">
                <template #actions><Button size="sm" variant="secondary" @click="addRelay"><PlusCircle class="w-3.5 h-3.5" /></Button></template>
              </CardHeader>
              <div class="p-4">
                <div v-if="store.relays.length > 0" class="divide-y divide-border-subtle">
                  <div v-for="relay in store.relays" :key="relay.id" class="py-2 flex items-center justify-between group">
                    <p class="text-[13px] font-semibold text-text-primary truncate max-w-xs">{{ relay.relay_url.replace('wss://', '') }}</p>
                    <div class="opacity-0 group-hover:opacity-100"><Button variant="ghost" size="sm" @click="store.deleteRelay(relay.id)"><Trash2 class="w-3.5 h-3.5" /></Button></div>
                  </div>
                </div>
                <EmptyState v-else title="No relays" :icon="Database" />
              </div>
            </Card>
            <Card>
              <CardHeader title="Security" :icon="Key" />
              <div class="p-4">
                <Button variant="secondary" class="w-full" @click="store.regenerateApiKey">Regenerate API Key</Button>
              </div>
            </Card>
          </div>
        </div>
      </div>
    </div>
    <BaseModal :show="modal.show" :title="modal.title" @close="modal.show = false">
      <div class="space-y-5">
        <input v-model="modal.value" @keyup.enter="handleModalSubmit" autofocus class="w-full bg-bg border border-border-subtle rounded-md px-4 py-2.5 text-text-primary" />
        <div v-if="modal.error" class="text-xs text-red-500 font-semibold">{{ modal.error }}</div>
        <div class="flex gap-2.5 pt-2">
          <Button variant="secondary" class="flex-1" @click="modal.show = false">Cancel</Button>
          <Button variant="primary" class="flex-1" @click="handleModalSubmit">Confirm</Button>
        </div>
      </div>
    </BaseModal>
  </AppShell>
</template>
