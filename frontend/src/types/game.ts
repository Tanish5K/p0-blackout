/* ------------------------------------------------------------------ *
 *  Shared types mirroring the Go backend's JSON.
 *  Phase 4: full snapshot shapes; every field optional unless the
 *  snapshot is full (first tick after connect or after delta-reset).
 * ------------------------------------------------------------------ */

/* ── Server → Client messages ────────────────────────────────────── */

export interface HelloMessage {
  type: 'hello'
  msg: string
}

export interface SnapshotMessage {
  type: 'snapshot'
  tick: number
  clock: string
  phase: 'running' | 'ended'
  services?: ServiceSnapshot[]
  queues?: QueueSnapshot[]
  pools?: PoolSnapshot[]
  metrics?: MetricsSnapshot
  events?: BackendEvent[]
}

export type ServerMessage = HelloMessage | SnapshotMessage

/* ── Snapshot sub-types ──────────────────────────────────────────── */

export type ServiceStatus = 'healthy' | 'degraded' | 'stalled' | 'failed' | 'idle'

export interface ServiceSnapshot {
  id: string
  name: string
  load: number
  health: number
  status: ServiceStatus
}

export interface QueueSnapshot {
  id: string
  depth: number
  inFlight: number
  unacked: number
  rateIn: number
  rateOut: number
}

export interface PoolSnapshot {
  queue: string
  workers: number
}

export interface LatencyMs {
  p50: number
  p99: number
}

export interface MetricsSnapshot {
  systemHealth: number
  successRate: number
  latencyMs: LatencyMs
}

export interface BackendEvent {
  tick: number
  time: number  // nanoseconds since run start
  type: 'request' | 'publish' | 'route' | 'consume' | 'ack'
    | 'fail' | 'action' | 'metric' | 'state'
  subject: string
  value?: number
  data?: string
}

/* ── Merged (authoritative) snapshot used by the reducer ─────────── */

export interface MergedSnapshot {
  tick: number
  clock: string
  phase: 'running' | 'ended'
  services: ServiceSnapshot[]
  queues: QueueSnapshot[]
  pools: PoolSnapshot[]
  metrics: MetricsSnapshot
  events: BackendEvent[]
}

/* ── Connection state (useGameState return type) ──────────────────── */

export interface ConnectionState {
  status: 'connecting' | 'connected' | 'disconnected'
  error?: string
  snapshot: MergedSnapshot
}

/* ── Player actions (Phase 5) ────────────────────────────────────── */

export interface ActionResponse {
  ok: boolean
  message?: string
}
