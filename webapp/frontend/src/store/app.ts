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
  isSetupComplete: boolean
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
    isSetupComplete: false,
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
        this.isSetupComplete = res.data.is_complete
      } catch (e) {
        this.isSetupComplete = false
      }
    },
    async signIn(credentials: { method: string, nsec?: string, bunkerUrl?: string }) {
      const res = await axios.post('http://localhost:8000/api/signIn', credentials)
      if (res.data.api_key) {
        this.setApiKey(res.data.api_key)
        this.isSetupComplete = true
        this.fetchAll()
      }
    },
    async fetchAll() {
      if (!this.apiKey) return
      this.loading = true
      try {
        const [m, s, r, q] = await Promise.all([
          this.api.get('/metrics'),
          this.api.get('/sources'),
          this.api.get('/relays'),
          this.api.get('/queue')
        ])
        this.metrics = m.data; this.sources = s.data; this.relays = r.data; this.queue = q.data
      } catch (e) {
        if (axios.isAxiosError(e) && e.response?.status === 401) {
           this.apiKey = ''
           localStorage.removeItem('api_key')
        }
      } finally {
        this.loading = false
      }
    },
    async regenerateApiKey() {
      const res = await this.api.post('/security/regenerate-key')
      this.setApiKey(res.data.api_key)
    },
    async fetchLogs() {
      try {
        const res = await this.api.get('/logs')
        this.logs = res.data.logs
      } catch (e) {
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
    },
  }
})