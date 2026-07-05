import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api, type Metrics } from '@/api/client'

export const useMetricsStore = defineStore('metrics', () => {
  const data = ref<Metrics | null>(null)
  const loading = ref(false)
  const error = ref<string | null>(null)
  let interval: ReturnType<typeof setInterval> | null = null

  async function fetch() {
    loading.value = true
    try {
      data.value = await api.getMetrics()
      error.value = null
    } catch (e: any) {
      error.value = e.message
    } finally {
      loading.value = false
    }
  }

  function startPolling(ms = 10000) {
    fetch()
    interval = setInterval(fetch, ms)
  }

  function stopPolling() {
    if (interval) {
      clearInterval(interval)
      interval = null
    }
  }

  return { data, loading, error, fetch, startPolling, stopPolling }
})
