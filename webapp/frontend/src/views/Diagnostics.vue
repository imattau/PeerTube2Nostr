<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api, type Metrics, type Relay, type Source, type Settings } from '@/api/client'

const metrics = ref<Metrics | null>(null)
const relays = ref<Relay[]>([])
const sources = ref<Source[]>([])
const settings = ref<Settings | null>(null)
const loading = ref(true)

onMounted(async () => {
  try {
    const [m, r, s, st] = await Promise.all([
      api.getMetrics(), api.listRelays(), api.listSources(), api.getSettings(),
    ])
    metrics.value = m
    relays.value = r.relays
    sources.value = s.sources
    settings.value = st
  } catch { }
  loading.value = false
})
</script>

<template>
  <div class="px-40 py-32">
    <div class="heading-1 mb-16">Diagnostics</div>
    <div class="body mb-16">System information and configuration details</div>

    <div v-if="loading" class="body">Loading...</div>

    <div v-if="metrics" class="card mb-16">
      <div class="heading-4 mb-8">System</div>
      <div class="body">Status: {{ metrics.status }}</div>
      <div class="body">Signing method: {{ metrics.signing_method || 'nsec' }}</div>
      <div class="body">NSEC configured: {{ metrics.has_nsec ? 'Yes' : 'No' }}</div>
      <div class="body">Next post: {{ metrics.next_post }}</div>
    </div>

    <div v-if="relays" class="card mb-16">
      <div class="heading-4 mb-8">Relays ({{ relays.length }})</div>
      <div class="body">Total configured: {{ relays.length }}</div>
      <div class="body">Enabled: {{ relays.filter(r => r.enabled).length }}</div>
      <div class="body">Healthy (latency &lt;200ms): {{ relays.filter(r => r.enabled && r.latency_ms !== null && r.latency_ms < 200).length }}</div>
      <div class="body">High latency: {{ relays.filter(r => r.enabled && r.latency_ms !== null && r.latency_ms >= 200).length }}</div>
      <div class="body">Offline: {{ relays.filter(r => r.enabled && r.latency_ms === null).length }}</div>
    </div>

    <div v-if="sources" class="card mb-16">
      <div class="heading-4 mb-8">Sources ({{ sources.length }})</div>
      <div class="body">Total: {{ sources.length }}</div>
      <div class="body">Enabled: {{ sources.filter(s => s.enabled).length }}</div>
    </div>

    <div v-if="settings" class="card mb-16">
      <div class="heading-4 mb-8">Publish limits</div>
      <div class="body">Min interval: {{ settings.min_publish_interval_seconds }}s</div>
      <div class="body">Max per hour: {{ settings.max_posts_per_hour }}</div>
      <div class="body">Max per day per source: {{ settings.max_posts_per_day_per_source }}</div>
    </div>

    <div v-if="!loading && !metrics && !relays && !sources" class="empty-state">
      <div class="empty-title">Unable to load diagnostics</div>
      <div class="empty-body">The backend may not be running or accessible.</div>
    </div>
  </div>
</template>
