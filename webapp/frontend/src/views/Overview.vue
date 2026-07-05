<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api, type Metrics } from '@/api/client'

const metrics = ref<Metrics | null>(null)
const loading = ref(true)

onMounted(async () => {
  try {
    metrics.value = await api.getMetrics()
  } catch { }
  loading.value = false
})
</script>

<template>
  <div class="px-40 py-32">
    <div class="flex items-center mb-16">
      <div class="flex-col" style="flex:1">
        <div class="heading-1">Overview</div>
        <div class="body mt-8">Monitor publishing health and upcoming posts</div>
      </div>
      <button class="button-default" v-if="metrics">Stop</button>
    </div>

    <div class="status-card" v-if="metrics">
      <span class="status-dot" :class="metrics.status === 'idle' ? 'status-running' : 'status-stopped'">&#9679;</span>
      <div>
        <div class="status-title">{{ metrics.status === 'idle' ? 'Running' : 'Idle' }}</div>
        <div class="status-subtitle">Status: {{ metrics.status }}</div>
      </div>
    </div>

    <div v-if="loading" class="body">Loading...</div>

    <div class="flex gap-16 mb-16" v-if="metrics">
      <div class="metric-card">
        <div class="metric-label">Queued</div>
        <div class="metric-value">{{ metrics.pending }}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Published today</div>
        <div class="metric-value">{{ metrics.posted }}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Failed</div>
        <div class="metric-value">{{ metrics.failed }}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Active sources</div>
        <div class="metric-value">{{ metrics.sources }}</div>
      </div>
    </div>

    <div class="banner banner-warning" v-if="metrics && metrics.failed > 0">
      <div>
        <div class="banner-title">Needs attention</div>
        <div class="banner-body">{{ metrics.failed }} failed publish(s). Check the queue.</div>
      </div>
      <button class="button-default banner-action">Review</button>
    </div>

    <div class="heading-3 mb-8" v-if="metrics && metrics.pending > 0">Next to publish</div>
    <div class="body" v-else-if="metrics && metrics.pending === 0">No pending items in queue.</div>

    <div class="body mt-16" v-if="metrics">
      <div>Last poll: {{ metrics.poll_age }}</div>
      <div>Last post: {{ metrics.post_age }}</div>
      <div>Next post: {{ metrics.next_post }}</div>
    </div>
  </div>
</template>
