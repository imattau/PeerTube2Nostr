interface TauriCore {
  invoke: (cmd: string, args?: Record<string, unknown>) => Promise<unknown>
}

declare global {
  interface Window {
    isTauri?: boolean
    __TAURI__?: { core: TauriCore }
    __TAURI_INTERNALS__?: TauriCore
  }
}

export function isTauri(): boolean {
  return typeof window !== 'undefined' && Boolean(window.isTauri || window.__TAURI__ || window.__TAURI_INTERNALS__)
}

export function tauriInvoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  const invoke = window.__TAURI__?.core?.invoke ?? window.__TAURI_INTERNALS__?.invoke
  if (!invoke) throw new Error('Not running in Tauri')
  return invoke(cmd, args) as Promise<T>
}
