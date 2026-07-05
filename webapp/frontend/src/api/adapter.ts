interface TauriCore {
  invoke: (cmd: string, args?: Record<string, unknown>) => Promise<unknown>
}

declare global {
  interface Window {
    __TAURI__?: { core: TauriCore }
  }
}

export function isTauri(): boolean {
  return typeof window !== 'undefined' && !!window.__TAURI__
}

export function tauriInvoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  const core = window.__TAURI__?.core
  if (!core) throw new Error('Not running in Tauri')
  return core.invoke(cmd, args) as Promise<T>
}
