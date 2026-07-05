<script setup lang="ts">
import { onMounted, onUnmounted, ref, computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useMetricsStore } from '@/stores/metrics'
import { api } from '@/api/client'
import { storeIdentityNpub } from '@/api/sync-client'
import * as nip46 from '@/api/nip46'

const router = useRouter()
const route = useRoute()
const metrics = useMetricsStore()
const menuOpen = ref(false)
const menuProcessing = ref('')

function toggleMenu() {
  menuOpen.value = !menuOpen.value
}

async function menuAction(label: string, fn: () => Promise<any>) {
  menuProcessing.value = label
  menuOpen.value = false
  try { await fn() } catch {}
  menuProcessing.value = ''
}

const menuItems = [
  { label: 'Check Relays', action: () => api.checkRelays() },
  { label: 'Retry Failed', action: () => api.retryFailed() },
  { label: 'Sync State', action: () => api.syncState() },
  { label: 'Preferences', action: () => router.push({ name: 'preferences' }) },
]

function onWindowClick(e: MouseEvent) {
  const target = e.target as HTMLElement
  if (!target.closest('.header-menu-wrap')) {
    menuOpen.value = false
  }
}

onMounted(() => window.addEventListener('click', onWindowClick))
onUnmounted(() => window.removeEventListener('click', onWindowClick))

const navItems = [
  { name: 'overview', label: 'Overview', icon: '🌂' },
  { name: 'queue', label: 'Queue', icon: '≡' },
  { name: 'sources', label: 'Sources', icon: '◉' },
  { name: 'relays', label: 'Relays', icon: '⌁' },
  { name: 'activity', label: 'Activity', icon: '◤' },
  { name: 'diagnostics', label: 'Diagnostics', icon: '✚' },
  { name: 'preferences', label: 'Preferences', icon: '⚙' },
]

const activeView = computed(() => String(route.name || 'overview'))

function navigate(name: string) {
  router.push({ name })
}

const nip07Available = ref(false)
let nip07Interval: ReturnType<typeof setInterval> | null = null
let nip46Interval: ReturnType<typeof setInterval> | null = null

async function tryPublishWithNip07() {
  try {
    const win = window as any
    if (!win.nostr || !win.nostr.signEvent) return
    const next = await api.nextPending()
    if (!next.eligible || !next.video) return
    const data = await api.getPublishEventData(next.video.id)
    if (!data.relays.length) return
    const pubkey = await win.nostr.getPublicKey()
    const event: any = { content: data.content, kind: data.kind, tags: data.tags, created_at: Math.floor(Date.now() / 1000), pubkey }
    const signed = await win.nostr.signEvent(event)
    await api.publishSigned(next.video.id, signed)
  } catch { }
}

async function tryPublishWithNip46() {
  try {
    const status = nip46.getStatus()
    if (!status.connected) return
    const next = await api.nextPending()
    if (!next.eligible || !next.video) return
    const data = await api.getPublishEventData(next.video.id)
    if (!data.relays.length) return
    const event = { content: data.content, kind: data.kind, tags: data.tags, created_at: Math.floor(Date.now() / 1000) }
    const signed = await nip46.signEvent(event)
    await api.publishSigned(next.video.id, signed)
  } catch { }
}

onMounted(async () => {
  // Try to restore NIP-46 connection from session
  try {
    await nip46.tryRestore()
  } catch { }

  try {
    const status = await api.setupStatus()
    const nip46Status = nip46.getStatus()
    if (status.needs_onboarding && !nip46Status.connected && route.name !== 'wizard') {
      router.replace({ name: 'wizard' })
    }
  } catch { }
  metrics.startPolling(10000)
  // Try to restore state from encrypted Nostr sync on first load
  try {
    const restored: any = await api.syncRestore()
    if (restored.found) {
      console.log(`Sync state: ${restored.video_count ?? 0} videos, ${restored.source_count ?? 0} sources`)
    }
  } catch { }

  // NIP-07 publishing bridge
  const win = window as any
  nip07Available.value = !!(win.nostr && win.nostr.signEvent)
  if (nip07Available.value) {
    try {
      const pubkey = await win.nostr.getPublicKey()
      if (pubkey) storeIdentityNpub(pubkey)
    } catch {}
    try { await api.updateSettings({ signing_method: 'nip07' }) } catch {}
    nip07Interval = setInterval(tryPublishWithNip07, 30000)
    tryPublishWithNip07()
  }

  // NIP-46 publishing bridge
  const nip46Status = nip46.getStatus()
  if (nip46Status.connected && nip46Status.pubkey) {
    storeIdentityNpub(nip46Status.pubkey)
    try { await api.updateSettings({ signing_method: 'nip46' }) } catch {}
    nip46Interval = setInterval(tryPublishWithNip46, 30000)
    tryPublishWithNip46()
  }
})

onUnmounted(() => {
  metrics.stopPolling()
  if (nip07Interval) clearInterval(nip07Interval)
  if (nip46Interval) clearInterval(nip46Interval)
})
</script>

<template>
  <div class="app-container" v-if="route.name !== 'wizard'">
    <header class="header-bar">
      <div class="header-status">
        <span class="status-dot" :class="metrics.data?.status === 'idle' ? 'status-running' : 'status-stopped'">&#9679;</span>
        <span class="status-label">{{ metrics.data?.status || 'Loading...' }}</span>
      </div>
      <button class="btn-icon header-refresh-btn" @click="metrics.fetch()">&#8635;</button>
      <div class="header-menu-wrap">
        <button class="btn-icon header-menu-btn" @click.stop="toggleMenu">&#8942;</button>
        <div v-if="menuOpen" class="header-dropdown">
          <div
            v-for="item in menuItems"
            :key="item.label"
            class="header-dropdown-item"
            :class="{ 'header-dropdown-item-disabled': !!menuProcessing }"
            @click="menuAction(item.label, item.action)"
          >{{ item.label }}</div>
        </div>
      </div>
    </header>

    <div class="body">
      <nav class="sidebar">
        <div class="sidebar-brand">PeerTube2Nostr</div>
        <div
          v-for="item in navItems"
          :key="item.name"
          class="nav-item"
          :class="{ 'nav-item-active': activeView === item.name }"
          @click="navigate(item.name)"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span class="nav-label">{{ item.label }}</span>
          <span v-if="item.name === 'queue' && metrics.data && metrics.data.pending > 0" class="badge badge-accent">{{ metrics.data.pending }}</span>
        </div>
      </nav>

      <main class="content-area">
        <router-view />
      </main>
    </div>
  </div>
  <div v-else class="wizard-shell">
    <router-view />
  </div>
</template>

<style scoped>
.wizard-shell {
  height: 100vh;
  background: #F6F5F4;
  overflow-y: auto;
}
</style>

<style scoped>
.app-container {
  display: flex;
  flex-direction: column;
  height: 100vh;
  background: #F6F5F4;
}
.header-bar {
  display: flex;
  align-items: center;
  padding: 0 12px;
  height: 46px;
  background: #EBEAE9;
  border-bottom: 1px solid #D5D3D0;
}
.header-status {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-right: auto;
}
.status-dot { font-size: 14px; }
.status-running { color: #2EC27E; }
.status-stopped { color: #E66100; }
.status-label { font-size: 13px; color: #2E3436; }
.btn-icon {
  background: transparent;
  border: none;
  font-size: 18px;
  cursor: pointer;
  padding: 4px 8px;
  border-radius: 4px;
  color: #555;
}
.btn-icon:hover { background: #D5D3D0; }
.header-menu-wrap { position: relative; }
.header-dropdown {
  position: absolute;
  top: 100%;
  right: 0;
  margin-top: 4px;
  min-width: 160px;
  background: #FFF;
  border: 1px solid #D5D3D0;
  border-radius: 6px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.12);
  z-index: 50;
  overflow: hidden;
}
.header-dropdown-item {
  padding: 9px 14px;
  font-size: 13px;
  color: #2E3436;
  cursor: pointer;
  white-space: nowrap;
}
.header-dropdown-item:hover { background: #F0EFED; }
.header-dropdown-item-disabled { opacity: 0.5; pointer-events: none; }
.body {
  display: flex;
  flex: 1;
  overflow: hidden;
}
.sidebar {
  width: 240px;
  min-width: 240px;
  background: #FFFFFF;
  border-right: 1px solid #D5D3D0;
  display: flex;
  flex-direction: column;
  padding-top: 8px;
}
.sidebar-brand {
  padding: 20px 16px 12px;
  font-size: 16px;
  font-weight: 600;
  color: #2E3436;
}
.nav-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 9px 12px;
  margin: 4px 16px;
  border-radius: 6px;
  cursor: pointer;
  color: #555;
}
.nav-item:hover { background: #F0EFED; }
.nav-item-active {
  background: #CDE0F9;
  color: #1A5FB4;
  font-weight: 500;
}
.nav-icon { font-size: 16px; width: 20px; text-align: center; }
.nav-label { flex: 1; font-size: 14px; }
.badge {
  font-size: 11px;
  padding: 2px 6px;
  border-radius: 10px;
  font-weight: 600;
}
.badge-accent { background: #3584E4; color: #fff; }
.content-area {
  flex: 1;
  overflow-y: auto;
  background: #F6F5F4;
}
</style>
