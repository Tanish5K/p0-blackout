/* ------------------------------------------------------------------ *
 *  Shared types mirroring the Go backend's JSON.
 *  Phase 4: full snapshot shapes; every field optional unless the
 *  snapshot is full (first tick after connect or after delta-reset).
 * ------------------------------------------------------------------ */

/* ── Server → Client messages ────────────────────────────────────── */

export type RunStatus = 'idle' | 'running' | 'ended'
export type Phase = RunStatus

export interface HelloMessage {
  type: 'hello'
  msg: string
}

export interface SnapshotMessage {
  type: 'snapshot'
  tick: number
  clock: string
  phase: Phase
  // runId identifies which run this frame belongs to: it increments on every
  // start/retry, and the client wipes its merged state the instant it changes.
  runId?: number
  // runStatus drives the start-overlay / postmortem gating: "idle" before the
  // first Start, "running" during play, "ended" once the outcome fired.
  runStatus?: RunStatus
  services?: ServiceSnapshot[]
  queues?: QueueSnapshot[]
  pools?: PoolSnapshot[]
  metrics?: MetricsSnapshot
  objectives?: ObjectiveSnapshot[]
  outcome?: OutcomeSnapshot
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
  // Wired=false means the service is paused (or, for stubs, never live): its
  // controls switch to resume and its mode toggle is disabled.
  wired?: boolean
  // Stalled flags a paused service whose queue is filling with nobody
  // consuming — the failure signature the rail's latency/success can't see.
  // The map node renders it grey with a "stalled" tag instead of green.
  stalled?: boolean
  // Synchronous reflects the orders path's processing mode (Incident 1's
  // sync/async toggle).
  synchronous?: boolean
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
  // Stale flags + ages mark rail numbers with no fresh samples (e.g. a paused
  // queue): the UI grays them out with an age suffix instead of reporting a
  // frozen/fabricated reading as current. Numerics stay unchanged behind them.
  latencyStale: boolean
  successStale: boolean
  latencyAgeMs: number
  successAgeMs: number
}

/* ── Objectives + terminal outcome (Phase 5) ─────────────────────── */

export interface ObjectiveSnapshot {
  id: string
  label: string
  role: 'fail' | 'survive'
  current: number
  target: number
  met: boolean
}

export interface OutcomeSnapshot {
  failed: boolean
  reason?: string
  endedAtMs: number
  health: number
  success: number
  p50Ms: number
  p99Ms: number
  // Terminal-stale flags (same semantics as MetricsSnapshot): success/p99 get
  // grayed with an age suffix instead of reading as a live 100% / 8ms.
  latencyStale: boolean
  successStale: boolean
  latencyAgeMs: number
  successAgeMs: number
  timeline: string[]
}

export interface BackendEvent {
  tick: number
  seq?: number
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
  phase: Phase
  runId: number
  runStatus: RunStatus
  services: ServiceSnapshot[]
  queues: QueueSnapshot[]
  pools: PoolSnapshot[]
  metrics: MetricsSnapshot
  objectives: ObjectiveSnapshot[]
  outcome?: OutcomeSnapshot
  events: BackendEvent[]
}

/* ── Connection state (useGameState return type) ──────────────────── */

export interface ConnectionState {
  status: 'connecting' | 'connected' | 'disconnected'
  error?: string
  snapshot: MergedSnapshot
}

/* ── Player actions (Phase 5) ────────────────────────────────────── */

export type ActionPayload = Record<string, number | string | boolean>

export interface ActionMessage {
  type: 'action'
  action: string
  payload: ActionPayload
}

export type RunAction = (action: string, payload?: ActionPayload) => void

export interface ActionResponse {
  ok: boolean
  message?: string
}

/* ── Run lifecycle control (Phase 5: play from the UI) ────────────── */

export interface ControlMessage {
  type: 'control'
  action: 'start' | 'retry'
}

export type RunControl = (action: 'start' | 'retry') => void
