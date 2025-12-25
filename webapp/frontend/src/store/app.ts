import { defineStore } from 'pinia'
import axios from 'axios'
import type { AxiosInstance } from 'axios'

interface AppState {
  metrics: any
  logs: string[]
  sources: any[]
  relays: any[]
  queue: any[]
  loading: boolean
  setupComplete: boolean
  apiKey: string
  api: AxiosInstance
}

export const useAppStore = defineStore('app', {
  state: (): AppState => ({
    metrics: {},
    logs: [],
    sources: [],
    relays: [],
    queue: [],
    loading: false,
    setupComplete: false,
    apiKey: localStorage.getItem('api_key') || '',
    api: axios.create({
      baseURL: 'http://localhost:8000/api',
      headers: localStorage.getItem('api_key') ? { 'X-API-Key': localStorage.getItem('api_key') } : {}
    })
  }),
  actions: {
    _updateApiInstance() {
      this.api = axios.create({
        baseURL: 'http://localhost:8000/api',
        headers: this.apiKey ? { 'X-API-Key': this.apiKey } : {}
      })
    },
    async fetchSetupStatus() {
      try {
        const res = await axios.get('http://localhost:8000/api/setup/status')
        this.setupComplete = res.data.is_complete
      } catch (e) {
        console.error('Setup status check failed', e)
      }
    },
    async finishSetup() {
      await this.api.post('/setup/complete')
      this.setupComplete = true
    },
    async fetchAll() {
      this.loading = true
      await this.fetchSetupStatus()
      if (!this.setupComplete && !this.apiKey) {
        this.loading = false
        return
      }
      try {
        const [m, s, r, q] = await Promise.all([
          this.api.get('/metrics'),
          this.api.get('/sources'),
          this.api.get('/relays'),
          this.api.get('/queue')
        ])
        this.metrics = m.data
        this.sources = s.data
        this.relays = r.data
        this.queue = q.data
      } catch (e) {
        console.error('Fetch failed', e)
        if (axios.isAxiosError(e) && e.response?.status === 401) {
           this.setupComplete = false
        }
      } finally {
        this.loading = false
      }
    },
    async fetchLogs() {
      try {
        const res = await this.api.get('/logs')
        this.logs = res.data.logs
      } catch (e) {
        // Suppress 401 errors for logs if setup isn't complete
        if (!axios.isAxiosError(e) || e.response?.status !== 401) {
          console.error('Logs fetch failed', e)
        }
      }
    },
    async addSource(url: string) {
      await this.api.post('/sources', { url })
      await this.fetchAll()
    },
    async deleteSource(id: number) {
      await this.api.delete(`/sources/${id}`)
      await this.fetchAll()
    },
    async toggleSource(id: number, enabled: boolean) {
      await this.api.patch(`/sources/${id}/toggle`, null, { params: { enabled } })
      await this.fetchAll()
    },
    async addRelay(url: string) {
      await this.api.post('/relays', { url })
      await this.fetchAll()
    },
    async deleteRelay(id: number) {
      await this.api.delete(`/relays/${id}`)
      await this.fetchAll()
    },
    async toggleRelay(id: number, enabled: boolean) {
      await this.api.patch(`/relays/${id}/toggle`, null, { params: { enabled } })
      await this.fetchAll()
    },
    async updateNsec(nsec: string) {
      await this.api.post('/nsec', { nsec })
      await this.fetchAll()
    },
    async updateSigningConfig(config: any) {
      await this.api.post('/setup/config', config)
    },
    async startRunner() {
      await this.api.post('/control/start')
      await this.fetchAll()
    },
    async stopRunner() {
      await this.api.post('/control/stop')
      await this.fetchAll()
    },
    setApiKey(key: string) {
      this.apiKey = key
      localStorage.setItem('api_key', key)
      this._updateApiInstance()
      this.fetchAll()
    }
  }
})