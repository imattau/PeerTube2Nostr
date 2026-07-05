<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { api, type Video } from '@/api/client'

const filter = ref('pending')
const videos = ref<Video[]>([])
const search = ref('')
const loading = ref(true)
const counts = ref<Record<string, number>>({ pending: 0, failed: 0, posted: 0 })

const filters = ['pending', 'failed', 'posted'] as const

const subtitle = computed(() => ({
  pending: 'Videos waiting to be published',
  failed: 'Videos that failed to publish',
  posted: 'Videos published to Nostr',
}[filter.value] || ''))

let pollInterval: ReturnType<typeof setInterval> | null = null

async function loadCounts() {
  try {
    counts.value = await api.getQueueCounts()
  } catch { }
}

async function load() {
  loading.value = true
  try {
    const res = await api.listVideos(filter.value, 200)
    videos.value = res.videos
    await loadCounts()
  } catch { }
  loading.value = false
}

const filteredVideos = () => {
  if (!search.value) return videos.value
  const q = search.value.toLowerCase()
  return videos.value.filter(v => (v.title || '').toLowerCase().includes(q))
}

function setFilter(f: string) { filter.value = f; load() }

onMounted(() => {
  load()
  pollInterval = setInterval(load, 10000)
})

onUnmounted(() => {
  if (pollInterval) clearInterval(pollInterval)
})

function relativeTime(ts: number | null): string {
  if (!ts) return ''
  const diff = Math.floor(Date.now() / 1000) - ts
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff / 60)} min ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)} hours ago`
  return `${Math.floor(diff / 86400)} days ago`
}

</script>

<template>
  <div class="px-40 py-32">
    <div class="flex items-center mb-16">
      <div class="flex-col" style="flex:1">
        <div class="heading-1">Queue</div>
        <div class="body mt-8">{{ subtitle }}</div>
      </div>
      <button class="button-primary">+ Add source</button>
    </div>

    <div class="flex gap-8 mt-24 mb-16">
      <input class="search-input" v-model="search" placeholder="Search queued videos" @input="() => {}" />
      <button
        v-for="f in filters"
        :key="f"
        :class="filter === f ? 'button-primary' : 'button-default'"
        @click="setFilter(f)"
      >
        {{ f.charAt(0).toUpperCase() + f.slice(1) }} ({{ counts[f] }})
      </button>
    </div>

    <div v-if="loading" class="body">Loading...</div>

    <div class="queue-row" v-for="v in filteredVideos()" :key="v.id">
      <div class="thumbnail"></div>
      <div class="queue-info">
        <div class="queue-title">{{ v.title || 'Untitled' }}</div>
        <div class="queue-meta">
          {{ v.channel_name || '' }}
          <span v-if="v.first_seen_ts">· discovered {{ relativeTime(v.first_seen_ts) }}</span>
        </div>
      </div>
      <span class="badge" :class="{
        'badge-accent': v.status === 'pending',
        'badge-error': v.status === 'failed',
        'badge-success': v.status === 'posted',
      }">{{ v.status }}</span>
    </div>

    <div v-if="!loading && filteredVideos().length === 0" class="empty-state">
      <div class="empty-icon">&#9744;</div>
      <div class="empty-title">No {{ filter }} items</div>
      <div class="empty-body">No videos with status "{{ filter }}" found.</div>
    </div>
  </div>
</template>
