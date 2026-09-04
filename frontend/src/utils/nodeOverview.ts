import type { ConfigNodeConfig, NodeSnapshot, NodesResponse, TrafficStreamEvent } from '../types'

export type NodeRuntimeStatus = 'normal' | 'unavailable' | 'blacklisted' | 'pending' | 'disabled'

export interface NodeOverview extends ConfigNodeConfig {
  runtimeStatus: NodeRuntimeStatus
  runtimePresent: boolean
  runtimeIdentityMatches: boolean
  latency_ms: number
  last_latency_ms: number
  region?: string
  country?: string
  active_connections: number
  success_count: number
  failure_count: number
  blacklisted: boolean
  available: boolean
  initial_check_done: boolean
  total_upload: number
  total_download: number
  upload_speed: number
  download_speed: number
  tag?: string
}

export interface NodeOverviewStats {
  total: number
  normal: number
  unavailable: number
  blacklisted: number
  pending: number
  disabled: number
  checked: number
  healthRate: number
}

function statusFromSnapshot(snapshot: NodeSnapshot): NodeRuntimeStatus {
  if (snapshot.blacklisted) return 'blacklisted'
  if (!snapshot.initial_check_done) return 'pending'
  return snapshot.available ? 'normal' : 'unavailable'
}

function emptyRuntime(config: ConfigNodeConfig, status: NodeRuntimeStatus): NodeOverview {
  return {
    ...config,
    runtimeStatus: status,
    runtimePresent: false,
    runtimeIdentityMatches: false,
    latency_ms: -1,
    last_latency_ms: -1,
    region: undefined,
    country: undefined,
    active_connections: 0,
    success_count: 0,
    failure_count: 0,
    blacklisted: false,
    available: false,
    initial_check_done: false,
    total_upload: 0,
    total_download: 0,
    upload_speed: 0,
    download_speed: 0,
    tag: undefined,
  }
}

function overviewFromSnapshot(config: ConfigNodeConfig, snapshot: NodeSnapshot): NodeOverview {
  return {
    ...config,
    runtimeStatus: statusFromSnapshot(snapshot),
    runtimePresent: true,
    runtimeIdentityMatches: true,
    latency_ms: snapshot.last_latency_ms,
    last_latency_ms: snapshot.last_latency_ms,
    region: snapshot.region,
    country: snapshot.country,
    active_connections: snapshot.active_connections,
    success_count: typeof snapshot.success_count === 'number' ? snapshot.success_count : 0,
    failure_count: snapshot.failure_count,
    blacklisted: snapshot.blacklisted,
    available: snapshot.available,
    initial_check_done: snapshot.initial_check_done,
    total_upload: snapshot.total_upload,
    total_download: snapshot.total_download,
    upload_speed: snapshot.upload_speed,
    download_speed: snapshot.download_speed,
    tag: snapshot.tag,
  }
}

export function buildNodeOverview(
  configNodes: ConfigNodeConfig[] | null,
  runtimeNodes: NodeSnapshot[],
): { nodes: NodeOverview[]; unmatchedRuntimeCount: number } {
  if (configNodes === null) {
    return {
      nodes: runtimeNodes.map((snapshot) => overviewFromSnapshot({
        id: snapshot.config_id,
        name: snapshot.name || snapshot.tag,
        uri: snapshot.uri,
        port: snapshot.port || 0,
        username: '',
        password: '',
        subscription_ids: [],
      }, snapshot)),
      unmatchedRuntimeCount: 0,
    }
  }

  const snapshotsByConfigID = new Map<number, NodeSnapshot>()
  const snapshotsByURI = new Map<string, NodeSnapshot>()
  for (const snapshot of runtimeNodes) {
    if (snapshot.config_id && snapshot.config_id > 0) {
      snapshotsByConfigID.set(snapshot.config_id, snapshot)
    }
    if (snapshot.uri) snapshotsByURI.set(snapshot.uri, snapshot)
  }

  const matchedRuntimeTags = new Set<string>()
  const nodes = configNodes.map((config) => {
    let snapshot: NodeSnapshot | undefined
    if (config.id && config.id > 0) {
      snapshot = snapshotsByConfigID.get(config.id)
    }
    if (!snapshot) snapshot = snapshotsByURI.get(config.uri)

    if (config.disabled) return emptyRuntime(config, 'disabled')
    if (!snapshot) return emptyRuntime(config, 'pending')

    // During a reload the stable ID may still reference the old runtime
    // instance. Keep the configured node pending until its URI also matches,
    // and leave the stale runtime node visible to the mismatch counter.
    if (snapshot.uri !== config.uri || matchedRuntimeTags.has(snapshot.tag)) {
      return {
        ...emptyRuntime(config, 'pending'),
        runtimePresent: true,
      }
    }

    matchedRuntimeTags.add(snapshot.tag)
    return overviewFromSnapshot(config, snapshot)
  })

  return {
    nodes,
    unmatchedRuntimeCount: runtimeNodes.filter((snapshot) => !matchedRuntimeTags.has(snapshot.tag)).length,
  }
}

export function summarizeNodeOverview(nodes: NodeOverview[]): NodeOverviewStats {
  const stats: NodeOverviewStats = {
    total: nodes.length,
    normal: 0,
    unavailable: 0,
    blacklisted: 0,
    pending: 0,
    disabled: 0,
    checked: 0,
    healthRate: -1,
  }

  for (const node of nodes) {
    stats[node.runtimeStatus]++
  }
  stats.checked = stats.normal + stats.unavailable + stats.blacklisted
  if (stats.checked > 0) {
    stats.healthRate = Math.round((stats.normal / stats.checked) * 100)
  }
  return stats
}

export function mergeTrafficEvent(
  current: NodesResponse | null,
  event: TrafficStreamEvent,
): NodesResponse | null {
  if (!current) return current

  const realtimeNodes = new Map(event.nodes.map((node) => [node.tag, node]))
  const nodes = current.nodes.map((node) => {
    const realtime = realtimeNodes.get(node.tag)
    if (!realtime) return node

    return {
      ...node,
      config_id: realtime.config_id || node.config_id,
      total_upload: realtime.total_upload,
      total_download: realtime.total_download,
      upload_speed: realtime.upload_speed,
      download_speed: realtime.download_speed,
      active_connections: realtime.active_connections,
      failure_count: realtime.failure_count,
      success_count: realtime.success_count,
      blacklisted: realtime.blacklisted,
      blacklisted_until: realtime.blacklisted_until,
      last_error: realtime.last_error,
      last_failure: realtime.last_failure,
      last_success: realtime.last_success,
      last_latency_ms: realtime.last_latency_ms,
      available: realtime.available,
      initial_check_done: realtime.initial_check_done,
    }
  })

  return {
    ...current,
    nodes,
    total_nodes: event.node_count,
    total_upload: event.total_upload,
    total_download: event.total_download,
    upload_speed: event.upload_speed,
    download_speed: event.download_speed,
    traffic_sampled: event.sampled_at,
  }
}
