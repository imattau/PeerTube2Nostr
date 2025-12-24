import { defineStore } from 'pinia'
import axios from 'axios'

const api = axios.create({
  baseURL: 'http://localhost:8000/api'
})

export const useAppStore = defineStore('app', {
  state: () => ({
    metrics: {} as any,
    logs: [] as string[],
    sources: [] as any[],
    relays: [] as any[],
    queue: [] as any[],
    loading: false,
    apiKey: localStorage.getItem('api_key') || ''
  }),
  actions: {
    getApi() {
      return axios.create({
        baseURL: 'http://localhost:8000/api',
        headers: this.apiKey ? { 'X-API-Key': this.apiKey } : {}
      })
    },
    async fetchAll() {
      this.loading = true
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
