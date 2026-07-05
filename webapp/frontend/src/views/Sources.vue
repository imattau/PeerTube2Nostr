<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api, type Source } from '@/api/client'

const sources = ref<Source[]>([])
const loading = ref(true)
const showAdd = ref(false)
const addUrl = ref('')
const adding = ref(false)

async function load() {
  loading.value = true
  try {
    const res = await api.listSources()
    sources.value = res.sources
  } catch { }
  loading.value = false
}

async function addSource() {
  if (!addUrl.value) return
  adding.value = true
  try {
    await api.addSource(addUrl.value)
    addUrl.value = ''
    showAdd.value = false
    await load()
  } catch (e: any) {
    alert(e.message)
  }
  adding.value = false
}

async function toggleSource(s: Source) {
  if (s.enabled) {
    await api.disableSource(s.id)
  } else {
    await api.enableSource(s.id)
  }
  await load()
}

async function removeSource(id: number) {
  if (!confirm('Remove this source?')) return
  await api.deleteSource(id)
  await load()
}

async function resync(id: number) {
  await api.resyncSource(id)
  await load()
}

onMounted(load)
</script>

<template>
  <div class="px-40 py-32">
    <div class="flex items-center mb-16">
      <div class="flex-col" style="flex:1">
        <div class="heading-1">Sources</div>
        <div class="body mt-8">PeerTube channels and RSS feeds</div>
      </div>
      <button class="button-primary" @click="showAdd = true">+ Add source</button>
    </div>

    <div v-if="loading" class="body">Loading...</div>

    <div v-for="s in sources" :key="s.id" class="action-row">
      <div class="action-info">
        <div class="action-title">{{ s.api_channel || s.rss_url || `Source #${s.id}` }}</div>
        <div class="action-subtitle">
          {{ s.api_channel_url || s.rss_url || s.api_base || `Source #${s.id}` }}
          <span v-if="s.last_error" class="badge badge-error ml-auto">Error</span>
        </div>
      </div>
      <div class="action-right flex items-center gap-8">
        <button class="button-default" @click="resync(s.id)">Resync</button>
        <button class="button-default" @click="removeSource(s.id)">Remove</button>
        <label class="switch">
          <input type="checkbox" :checked="s.enabled" @change="toggleSource(s)" />
          <span class="slider"></span>
        </label>
      </div>
    </div>

    <div v-if="!loading && sources.length === 0" class="empty-state">
      <div class="empty-icon">&#9675;</div>
      <div class="empty-title">No sources configured</div>
      <div class="empty-body">Add a PeerTube channel or RSS feed to begin discovering videos.</div>
      <button class="button-primary" @click="showAdd = true">Add source</button>
    </div>

    <div v-if="showAdd" class="dialog-overlay" @click.self="showAdd = false">
      <div class="dialog">
        <div class="heading-3 mb-16">Add source</div>
        <input v-model="addUrl" placeholder="PeerTube channel or RSS URL" class="w-full" @keyup.enter="addSource" />
        <div class="flex gap-8 mt-16">
          <button class="button-default" @click="showAdd = false">Cancel</button>
          <button class="button-primary ml-auto" :disabled="adding || !addUrl" @click="addSource">
            {{ adding ? 'Adding...' : 'Add' }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
