import type {
  AuthResponse,
  NodesResponse,
  DebugResponse,
  SettingsData,
  SettingsUpdateResponse,
  ConfigNodesResponse,
  ConfigNodePayload,
  ConfigNodeMutationResponse,
  ReloadTaskStatus,
  SubscriptionStatus,
  Subscription,
  SubscriptionPayload,
  SubscriptionsResponse,
  SubscriptionNodesResponse,
  SubscriptionActionResponse,
  ProbeSSEEvent,
  TrafficStreamEvent,
  DebugLogEvent,
} from '../types'

// ---- Token management ----

let authToken: string | null = localStorage.getItem('auth_token')

export function getToken(): string | null {
  return authToken
}

export function setToken(token: string | null) {
  authToken = token
  if (token) {
    localStorage.setItem('auth_token', token)
  } else {
    localStorage.removeItem('auth_token')
  }
}

export function clearToken() {
  setToken(null)
}

// ---- Base request helper ----

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    ...(options.headers as Record<string, string> || {}),
  }

  // Add auth header if we have a token
  if (authToken) {
    headers['Authorization'] = `Bearer ${authToken}`
  }

  // Set JSON content type for non-GET requests with body
  if (options.body && typeof options.body === 'string') {
    headers['Content-Type'] = 'application/json'
  }

  const res = await fetch(path, {
    ...options,
    headers,
    credentials: 'include', // send cookies
  })

  if (res.status === 401) {
    clearToken()
    // Dispatch a custom event so App can react
    window.dispatchEvent(new CustomEvent('auth:unauthorized'))
    throw new ApiError('未授权，请重新登录', 401)
  }

  if (!res.ok) {
    let msg = `HTTP ${res.status}`
    try {
      const body = await res.json()
      if (body.error) msg = body.error
    } catch { /* ignore parse errors */ }
    throw new ApiError(msg, res.status)
  }

  // Handle empty responses
  const text = await res.text()
  if (!text) return {} as T
  return JSON.parse(text) as T
}

export class ApiError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

// ---- Auth API ----

/** Check if password is required & login */
export async function checkAuth(): Promise<AuthResponse> {
  // Use GET-like behavior: /api/auth without POST returns password status
  const res = await fetch('/api/auth', { credentials: 'include' })
  return res.json()
}

export async function login(password: string): Promise<AuthResponse> {
  const res = await fetch('/api/auth', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password }),
    credentials: 'include',
  })

  if (!res.ok) {
    const body = await res.json()
    throw new ApiError(body.error || '登录失败', res.status)
  }

  const data: AuthResponse = await res.json()
  if (data.token) {
    setToken(data.token)
  }
  return data
}

export function logout() {
  clearToken()
}

// ---- Nodes API ----

export async function fetchNodes(): Promise<NodesResponse> {
  return request<NodesResponse>('/api/nodes')
}

export async function probeNode(tag: string): Promise<{ message: string; latency_ms: number }> {
  return request(`/api/nodes/${encodeURIComponent(tag)}/probe`, { method: 'POST' })
}

export async function releaseNode(tag: string): Promise<{ message: string }> {
  return request(`/api/nodes/${encodeURIComponent(tag)}/release`, { method: 'POST' })
}

/** Probe all nodes with SSE progress updates */
export function probeAllNodes(
  onEvent: (event: ProbeSSEEvent) => void,
  onError?: (error: Error) => void
): AbortController {
  const controller = new AbortController()

  const doFetch = async () => {
    try {
      const headers: Record<string, string> = {}
      if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`
      }

      const res = await fetch('/api/nodes/probe-all', {
        method: 'POST',
        headers,
        credentials: 'include',
        signal: controller.signal,
      })

      if (!res.ok) {
        throw new ApiError(`探测失败: HTTP ${res.status}`, res.status)
      }

      const reader = res.body?.getReader()
      if (!reader) throw new Error('No response body')

      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const line of lines) {
          const trimmed = line.trim()
          if (trimmed.startsWith('data: ')) {
            try {
              const data = JSON.parse(trimmed.slice(6)) as ProbeSSEEvent
              onEvent(data)
            } catch { /* skip malformed events */ }
          }
        }
      }
    } catch (err) {
      if ((err as Error).name !== 'AbortError') {
        onError?.(err as Error)
      }
    }
  }

  doFetch()
  return controller
}

// ---- Traffic Stream API ----

/** Subscribe real-time traffic speeds via SSE */
export function streamTraffic(
  onEvent: (event: TrafficStreamEvent) => void,
  onError?: (error: Error) => void
): AbortController {
  const controller = new AbortController()

  const doFetch = async () => {
    try {
      const headers: Record<string, string> = {}
      if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`
      }

      const res = await fetch('/api/nodes/traffic/stream', {
        method: 'GET',
        headers,
        credentials: 'include',
        signal: controller.signal,
      })

      if (!res.ok) {
        throw new ApiError(`流量流订阅失败: HTTP ${res.status}`, res.status)
      }

      const reader = res.body?.getReader()
      if (!reader) throw new Error('No response body')

      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) {
          if (!controller.signal.aborted) {
            onError?.(new Error('实时流连接已断开'))
          }
          break
        }

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const line of lines) {
          const trimmed = line.trim()
          if (trimmed.startsWith('data: ')) {
            try {
              const data = JSON.parse(trimmed.slice(6)) as TrafficStreamEvent
              if (data.type === 'traffic') {
                onEvent(data)
              }
            } catch { /* skip malformed events */ }
          }
        }
      }
    } catch (err) {
      if ((err as Error).name !== 'AbortError') {
        onError?.(err as Error)
      }
    }
  }

  doFetch()
  return controller
}

// ---- Debug API ----

export async function fetchDebug(): Promise<DebugResponse> {
  return request<DebugResponse>('/api/debug')
}

export function streamDebugLogs(onEvent: (event: DebugLogEvent) => void, onStatus: (connected: boolean) => void): AbortController {
  const controller = new AbortController()
  const connect = async () => {
    while (!controller.signal.aborted) {
      try {
        const headers: Record<string, string> = {}
        if (authToken) headers.Authorization = `Bearer ${authToken}`
        const response = await fetch('/api/debug/stream', { headers, credentials: 'include', signal: controller.signal })
        if (response.status === 401) {
          clearToken()
          window.dispatchEvent(new CustomEvent('auth:unauthorized'))
          return
        }
        if (!response.ok || !response.body) throw new ApiError(`HTTP ${response.status}`, response.status)
        onStatus(true)
        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''
        while (!controller.signal.aborted) {
          const { done, value } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })
          const messages = buffer.split('\n\n')
          buffer = messages.pop() ?? ''
          for (const message of messages) {
            const line = message.split('\n').find((item) => item.startsWith('data: '))
            if (line) onEvent(JSON.parse(line.slice(6)) as DebugLogEvent)
          }
        }
      } catch (error) {
        if ((error as Error).name === 'AbortError') return
      } finally {
        onStatus(false)
      }
      await new Promise((resolve) => setTimeout(resolve, 2000))
    }
  }
  void connect()
  return controller
}

// ---- Settings API ----

export async function fetchSettings(): Promise<SettingsData> {
  return request<SettingsData>('/api/settings')
}

export async function updateSettings(settings: SettingsData): Promise<SettingsUpdateResponse> {
  return request<SettingsUpdateResponse>('/api/settings', {
    method: 'PUT',
    body: JSON.stringify(settings),
  })
}

// ---- Config Nodes CRUD API ----

export async function fetchConfigNodes(subscriptionId?: number): Promise<ConfigNodesResponse> {
  const query = subscriptionId === undefined ? '' : `?subscription_id=${encodeURIComponent(subscriptionId)}`
  return request<ConfigNodesResponse>(`/api/nodes/config${query}`)
}

export async function createConfigNode(payload: ConfigNodePayload): Promise<ConfigNodeMutationResponse> {
  return request<ConfigNodeMutationResponse>('/api/nodes/config', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export type ConfigNodeRef = number | string

function configNodePath(ref: ConfigNodeRef): string {
  return typeof ref === 'number' && ref > 0
    ? `/api/nodes/config/id/${encodeURIComponent(String(ref))}`
    : `/api/nodes/config/${encodeURIComponent(String(ref))}`
}

export async function updateConfigNode(ref: ConfigNodeRef, payload: ConfigNodePayload): Promise<ConfigNodeMutationResponse> {
  return request<ConfigNodeMutationResponse>(configNodePath(ref), {
    method: 'PUT',
    body: JSON.stringify(payload),
  })
}

export async function deleteConfigNode(ref: ConfigNodeRef): Promise<ConfigNodeMutationResponse> {
  return request<ConfigNodeMutationResponse>(configNodePath(ref), {
    method: 'DELETE',
  })
}

export async function toggleConfigNode(ref: ConfigNodeRef, enabled: boolean): Promise<ConfigNodeMutationResponse> {
  return request<ConfigNodeMutationResponse>(configNodePath(ref), {
    method: 'PATCH',
    body: JSON.stringify({ enabled }),
  })
}

function splitConfigNodeRefs(refs: ConfigNodeRef[]): { ids?: number[]; names?: string[] } {
  const ids = refs.filter((ref): ref is number => typeof ref === 'number' && ref > 0)
  const names = refs.filter((ref): ref is string => typeof ref === 'string')
  return {
    ...(ids.length ? { ids } : {}),
    ...(names.length ? { names } : {}),
  }
}

export async function batchToggleConfigNodes(refs: ConfigNodeRef[], enabled: boolean): Promise<ConfigNodeMutationResponse> {
  return request('/api/nodes/config/batch-toggle', {
    method: 'POST',
    body: JSON.stringify({ ...splitConfigNodeRefs(refs), enabled }),
  })
}

export async function batchDeleteConfigNodes(refs: ConfigNodeRef[]): Promise<ConfigNodeMutationResponse> {
  return request('/api/nodes/config/batch-delete', {
    method: 'POST',
    body: JSON.stringify(splitConfigNodeRefs(refs)),
  })
}

// ---- Reload API ----

export interface ReloadResponse {
  message: string
  reload?: ReloadTaskStatus
}

export async function triggerReload(): Promise<ReloadResponse> {
  return request<ReloadResponse>('/api/reload', { method: 'POST' })
}

export async function fetchReloadStatus(): Promise<ReloadTaskStatus> {
  return request<ReloadTaskStatus>('/api/reload/status')
}

// ---- Subscription API ----

export async function fetchSubscriptionStatus(): Promise<SubscriptionStatus> {
  return request<SubscriptionStatus>('/api/subscription/status')
}

export async function refreshSubscription(): Promise<{ message: string; node_count: number }> {
  return request('/api/subscription/refresh', { method: 'POST' })
}

export async function listSubscriptions(): Promise<SubscriptionsResponse> {
  return request<SubscriptionsResponse>('/api/subscriptions')
}

export async function createSubscription(payload: SubscriptionPayload): Promise<Subscription> {
  return request<Subscription>('/api/subscriptions', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function updateSubscription(id: number, payload: SubscriptionPayload): Promise<Subscription> {
  return request<Subscription>(`/api/subscriptions/${id}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  })
}

export async function deleteSubscription(id: number): Promise<SubscriptionActionResponse> {
  return request<SubscriptionActionResponse>(`/api/subscriptions/${id}`, { method: 'DELETE' })
}

export async function toggleSubscription(id: number, enabled: boolean): Promise<SubscriptionActionResponse> {
  return request<SubscriptionActionResponse>(`/api/subscriptions/${id}/enabled`, {
    method: 'PATCH',
    body: JSON.stringify({ enabled }),
  })
}

export async function activateSubscription(id: number): Promise<SubscriptionActionResponse> {
  return request<SubscriptionActionResponse>(`/api/subscriptions/${id}/activate`, { method: 'POST' })
}

export async function refreshOneSubscription(id: number): Promise<SubscriptionActionResponse> {
  return request<SubscriptionActionResponse>(`/api/subscriptions/${id}/refresh`, { method: 'POST' })
}

export async function listSubscriptionNodes(id: number): Promise<SubscriptionNodesResponse> {
  return request<SubscriptionNodesResponse>(`/api/subscriptions/${id}/nodes`)
}

// ---- Export API ----

export async function exportProxies(): Promise<string> {
  const headers: Record<string, string> = {}
  if (authToken) {
    headers['Authorization'] = `Bearer ${authToken}`
  }
  const res = await fetch('/api/export', {
    headers,
    credentials: 'include',
  })
  if (!res.ok) throw new ApiError('导出失败', res.status)
  return res.text()
}

// ---- Import API ----

export async function importNodes(content: string): Promise<{ message: string; imported: number; errors?: string[] }> {
  return request('/api/import', {
    method: 'POST',
    body: JSON.stringify({ content }),
  })
}
