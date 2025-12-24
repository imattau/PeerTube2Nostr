import { describe, it, expect, beforeEach, vi } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useAppStore } from './app'
import axios from 'axios'

vi.mock('axios')

describe('App Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  it('initializes with default values', () => {
    const store = useAppStore()
    expect(store.setupComplete).toBe(true)
    expect(store.logs).toEqual([])
    expect(store.apiKey).toBe('')
  })

  it('updates API Key and persists to localStorage', () => {
    const store = useAppStore()
    store.setApiKey('test-key')
    expect(store.apiKey).toBe('test-key')
    expect(localStorage.getItem('api_key')).toBe('test-key')
  })

  it('fetches metrics correctly', async () => {
    const store = useAppStore()
    const metricsData = { status: 'idle', pending: 5 }
    
    // Mock getApi to return a mocked axios instance
    const mockApiInstance = {
      get: vi.fn().mockImplementation((url) => {
        if (url === '/metrics') return Promise.resolve({ data: metricsData })
        if (url === '/sources') return Promise.resolve({ data: [] })
        if (url === '/relays') return Promise.resolve({ data: [] })
        if (url === '/queue') return Promise.resolve({ data: [] })
        return Promise.resolve({ data: {} })
      })
    }
    
    vi.spyOn(store, 'getApi').mockReturnValue(mockApiInstance as any)
    vi.spyOn(axios, 'get').mockResolvedValue({ data: { is_complete: true } })

    await store.fetchAll()
    
    expect(store.metrics.pending).toBe(5)
    expect(store.metrics.status).toBe('idle')
  })
})
