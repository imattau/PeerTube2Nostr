<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import { useLogStore, type LogEntry } from '@/stores/logs'

const store = useLogStore()
const autoScroll = ref(true)
const levelFilter = ref('ALL')
const logContainer = ref<HTMLDivElement | null>(null)

const levels = ['ALL', 'INFO', 'WARN', 'ERROR', 'DEBUG']

const filteredEntries = () => {
  if (levelFilter.value === 'ALL') return store.entries
  return store.entries.filter(e => e.level === levelFilter.value)
}

function scrollToBottom() {
  if (autoScroll.value && logContainer.value) {
    logContainer.value.scrollTop = logContainer.value.scrollHeight
  }
}

onMounted(() => {
  store.connect()
})

onUnmounted(() => {
  store.disconnect()
})
</script>

<template>
  <div class="px-40 py-32">
    <div class="flex items-center mb-16">
      <div class="flex-col" style="flex:1">
        <div class="heading-1">Activity</div>
        <div class="body mt-8">Live publishing log</div>
      </div>
      <div class="flex items-center gap-8">
        <span class="badge" :class="store.connected ? 'badge-success' : 'badge-error'">
          {{ store.connected ? 'Connected' : 'Disconnected' }}
        </span>
        <button class="button-default" @click="store.clear()">Clear</button>
        <label class="flex items-center gap-8" style="font-size:13px;color:#555;cursor:pointer">
          <input type="checkbox" v-model="autoScroll" />
          Auto-scroll
        </label>
      </div>
    </div>

    <div class="flex gap-8 mb-16">
      <button
        v-for="l in levels"
        :key="l"
        :class="levelFilter === l ? 'button-primary' : 'button-default'"
        @click="levelFilter = l"
      >{{ l }}</button>
    </div>

    <div
      ref="logContainer"
      class="log-viewer"
      style="height:60vh"
      @scroll="() => {}"
    >
      <div v-for="(entry, i) in filteredEntries()" :key="i">
        <span style="color:#888">{{ entry.timestamp }}</span>
        <span :style="{
          color: entry.level === 'ERROR' ? '#E66100' : entry.level === 'WARN' ? '#F5C267' : '#4E9A06',
          fontWeight: 600,
          margin: '0 8px',
        }">{{ entry.level.padEnd(8) }}</span>
        <span>{{ entry.message }}</span>
      </div>
      <div v-if="filteredEntries().length === 0" style="color:#888;text-align:center;padding:40px">
        No log entries yet. Open a WebSocket connection to see live logs.
      </div>
    </div>
  </div>
</template>
