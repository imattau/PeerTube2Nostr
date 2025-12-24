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
}

export const useAppStore = defineStore('app', {
  state: (): AppState => ({
    metrics: {},
    logs: [],
    sources: [],
    relays: [],
    queue: [],
    loading: false,
    setupComplete: true,
    apiKey: localStorage.getItem('api_key') || ''
  }),
  actions: {
    getApi(): AxiosInstance {
      return axios.create({
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
      await this.getApi().post('/setup/complete')
      this.setupComplete = true
    },
    async fetchAll() {
      this.loading = true
      await this.fetchSetupStatus()
      if (!this.setupComplete && !this.apiKey) {
        this.loading = false
        return
      }
      const api = this.getApi()
      try {
        const [m, s, r, q] = await Promise.all([
          api.get('/metrics'),
          api.get('/sources'),
          api.get('/relays'),
          api.get('/queue')
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
      const api = this.getApi()
      try {
        const res = await api.get('/logs')
        this.logs = res.data.logs
      } catch (e) {
        console.error('Logs fetch failed', e)
      }
    },
    async addSource(url: string) {
      await this.getApi().post('/sources', { url })
      await this.fetchAll()
    },
    async deleteSource(id: number) {
      await this.getApi().delete(`/sources/${id}`)
      await this.fetchAll()
    },
    async toggleSource(id: number, enabled: boolean) {
      await this.getApi().patch(`/sources/${id}/toggle`, null, { params: { enabled } })
      await this.fetchAll()
    },
    async addRelay(url: string) {
      await this.getApi().post('/relays', { url })
      await this.fetchAll()
    },
    async deleteRelay(id: number) {
      await this.getApi().delete(`/relays/${id}`)
      await this.fetchAll()
    },
    async toggleRelay(id: number, enabled: boolean) {
      await this.getApi().patch(`/relays/${id}/toggle`, null, { params: { enabled } })
      await this.fetchAll()
    },
    async updateNsec(nsec: string) {
      await this.getApi().post('/nsec', { nsec })
      await this.fetchAll()
    },
    async updateSigningConfig(config: any) {
      await this.getApi().post('/setup/config', config)
    },
    async startRunner() {
      await this.getApi().post('/control/start')
      await this.fetchAll()
    },
    async stopRunner() {
      await this.getApi().post('/control/stop')
      await this.fetchAll()
    },
    setApiKey(key: string) {
      this.apiKey = key
      localStorage.setItem('api_key', key)
      this.fetchAll()
    }
  }
})