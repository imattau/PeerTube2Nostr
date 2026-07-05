import { defineStore } from 'pinia'
import { ref } from 'vue'

const WS_URL = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/api/ws/logs`

export interface LogEntry {
  timestamp: string
  level: string
  message: string
}

export const useLogStore = defineStore('logs', () => {
  const entries = ref<LogEntry[]>([])
  const connected = ref(false)
  let ws: WebSocket | null = null
  const maxEntries = 500

  function connect() {
    if (ws) return
    try {
      ws = new WebSocket(WS_URL)
      ws.onopen = () => { connected.value = true }
      ws.onmessage = (e) => {
        try {
          const entry: LogEntry = JSON.parse(e.data)
          entries.value.push(entry)
          if (entries.value.length > maxEntries) {
            entries.value = entries.value.slice(-maxEntries)
          }
        } catch { }
      }
      ws.onclose = () => {
        connected.value = false
        ws = null
        setTimeout(connect, 3000)
      }
      ws.onerror = () => { ws?.close() }
    } catch {
      setTimeout(connect, 3000)
    }
  }

  function disconnect() {
    ws?.close()
    ws = null
    connected.value = false
  }

  function clear() {
    entries.value = []
  }

  return { entries, connected, connect, disconnect, clear }
})
