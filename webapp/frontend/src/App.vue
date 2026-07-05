<script setup lang="ts">
import { onMounted, onUnmounted, ref, computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useMetricsStore } from '@/stores/metrics'
import { api } from '@/api/client'

const router = useRouter()
const route = useRoute()
const metrics = useMetricsStore()

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

onMounted(async () => {
  try {
    const status = await api.setupStatus()
    if (status.needs_onboarding && route.name !== 'wizard') {
      router.replace({ name: 'wizard' })
    }
  } catch { }
  metrics.startPolling(10000)
})

onUnmounted(() => {
  metrics.stopPolling()
})
</script>

<template>
  <div class="app-container" v-if="route.name !== 'wizard'">
    <header class="header-bar">
      <div class="header-status">
        <span class="status-dot" :class="metrics.data?.status === 'idle' ? 'status-running' : 'status-stopped'">&#9679;</span>
        <span class="status-label">{{ metrics.data?.status || 'Loading...' }}</span>
      </div>
      <button class="btn-icon header-refresh-btn" @click="metrics.fetch">&#8635;</button>
      <button class="btn-icon header-menu-btn">&#8942;</button>
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
