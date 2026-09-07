/* ------------------------------------------------------------------ *
 *  Snapshot helpers: merge, lerp, formatting, map layout constants.
 *  This is the single source-of-truth layer for all derived game state.
 * ------------------------------------------------------------------ */

import type {
  MergedSnapshot,
  SnapshotMessage,
  ServiceSnapshot,
  QueueSnapshot,
  PoolSnapshot,
  MetricsSnapshot,
  BackendEvent,
  ServiceStatus,
} from '../types/game'

/* ── Initial snapshot ─────────────────────────────────────────────── */

export function emptySnapshot(): MergedSnapshot {
  return {
    tick: 0,
    clock: '--:--:--',
    phase: 'running',
    services: [],
    queues: [],
    pools: [],
    metrics: { systemHealth: 0, successRate: 0, latencyMs: { p50: 0, p99: 0 } },
    objectives: [],
    events: [],
  }
}

/* ── Merge delta into a running merged snapshot ────────────────────── */

export function mergeSnapshot(prev: MergedSnapshot, update: SnapshotMessage): MergedSnapshot {
  // If the tick went backwards the backend restarted (its tick counter reset);
  // treat the incoming frame as a fresh run rather than merging over stale state.
  if (update.tick < prev.tick) {
    return {
      tick: update.tick,
      clock: update.clock,
      phase: update.phase,
      services: update.services ?? prev.services,
      queues: update.queues ?? prev.queues,
      pools: update.pools ?? prev.pools,
      metrics: { ...prev.metrics, ...update.metrics },
      objectives: update.objectives ?? prev.objectives,
      outcome: update.outcome ?? prev.outcome,
      events: update.events ?? [],
    }
  }

  const metrics = mergeMetrics(prev.metrics, update.metrics)
  const events = mergeEvents(prev.events, update.events ?? [])

  return {
    tick: update.tick,
    clock: update.clock,
    phase: update.phase,
    services: update.services ?? prev.services,
    queues: update.queues ?? prev.queues,
    pools: update.pools ?? prev.pools,
    metrics,
    objectives: update.objectives ?? prev.objectives,
    outcome: update.outcome ?? prev.outcome,
    events,
  }
}

function mergeMetrics(
  prev: MetricsSnapshot,
  next: MetricsSnapshot | undefined,
): MetricsSnapshot {
  if (!next || (!next.systemHealth && !next.successRate && !next.latencyMs.p50 && !next.latencyMs.p99)) {
    return prev
  }
  return {
    systemHealth: next.systemHealth || prev.systemHealth,
    successRate: next.successRate || prev.successRate,
    latencyMs: {
      p50: next.latencyMs.p50 || prev.latencyMs.p50,
      p99: next.latencyMs.p99 || prev.latencyMs.p99,
    },
  }
}

const MAX_EVENTS = 400

function mergeEvents(prev: BackendEvent[], incoming: BackendEvent[]): BackendEvent[] {
  if (incoming.length === 0) return prev
  // Events are strictly per-tick and ticks are monotonic: only append events
  // newer than the last tick we've seen. A re-delivered tick is ignored.
  const lastTick = prev.length > 0 ? prev[prev.length - 1].tick : -1
  const fresh = incoming.filter((e) => e.tick > lastTick)
  const next = fresh.length > 0 ? prev.concat(fresh) : prev
  return next.length > MAX_EVENTS ? next.slice(next.length - MAX_EVENTS) : next
}

/* ── Linear interpolation between two merged snapshots ─────────────── */

export function lerpSnapshot(
  a: MergedSnapshot,
  b: MergedSnapshot,
  t: number,
): MergedSnapshot {
  const c = Math.max(0, Math.min(1, t))

  // Scalars: snap to b
  const tick = b.tick
  const clock = b.clock
  const phase = b.phase

  // Arrays: lerp in-place, fall back to a if b doesn't carry it
  const services = lerpServices(a.services, b.services, c)
  const queues = lerpQueues(a.queues, b.queues, c)
  const pools = lerpPools(a.pools, b.pools, c)
  const metrics = lerpMetrics(a.metrics, b.metrics, c)
  const objectives = b.objectives // live objective strip (never interpolated)
  const outcome = b.outcome
  const events = b.events // always latest events (never interpolated)

  return { tick, clock, phase, services, queues, pools, metrics, objectives, outcome, events }
}

function lerpServices(
  a: ServiceSnapshot[],
  b: ServiceSnapshot[],
  t: number,
): ServiceSnapshot[] {
  const len = Math.max(a.length, b.length)
  const out: ServiceSnapshot[] = []
  for (let i = 0; i < len; i++) {
    const sa = a[i] ?? STUB_SERVICE
    const sb = b[i] ?? STUB_SERVICE
    out.push({
      id: sb.id || sa.id,
      name: sb.name || sa.name,
      load: lerp(sa.load, sb.load, t),
      health: lerp(sa.health, sb.health, t),
      status: t >= 0.5 ? sb.status : sa.status,
      wired: t >= 0.5 ? sb.wired : sa.wired,
      synchronous: t >= 0.5 ? sb.synchronous : sa.synchronous,
    })
  }
  return out
}

const STUB_SERVICE: ServiceSnapshot = {
  id: '',
  name: '',
  load: 0,
  health: 100,
  status: 'idle',
  wired: false,
  synchronous: false,
}

function lerpQueues(
  a: QueueSnapshot[],
  b: QueueSnapshot[],
  t: number,
): QueueSnapshot[] {
  const len = Math.max(a.length, b.length)
  const out: QueueSnapshot[] = []
  for (let i = 0; i < len; i++) {
    const qa = a[i] ?? STUB_QUEUE
    const qb = b[i] ?? STUB_QUEUE
    out.push({
      id: qb.id || qa.id,
      depth: lerpNum(qa.depth, qb.depth, t),
      inFlight: lerpNum(qa.inFlight, qb.inFlight, t),
      unacked: lerpNum(qa.unacked, qb.unacked, t),
      rateIn: lerp(qa.rateIn, qb.rateIn, t),
      rateOut: lerp(qa.rateOut, qb.rateOut, t),
    })
  }
  return out
}

const STUB_QUEUE: QueueSnapshot = {
  id: '',
  depth: 0,
  inFlight: 0,
  unacked: 0,
  rateIn: 0,
  rateOut: 0,
}

function lerpPools(
  a: PoolSnapshot[],
  b: PoolSnapshot[],
  t: number,
): PoolSnapshot[] {
  const len = Math.max(a.length, b.length)
  const out: PoolSnapshot[] = []
  for (let i = 0; i < len; i++) {
    const pa = a[i] ?? STUB_POOL
    const pb = b[i] ?? STUB_POOL
    out.push({
      queue: pb.queue || pa.queue,
      workers: t >= 0.5 ? pb.workers : pa.workers, // snap (discrete)
    })
  }
  return out
}

const STUB_POOL: PoolSnapshot = { queue: '', workers: 0 }

function lerpMetrics(a: MetricsSnapshot, b: MetricsSnapshot, t: number): MetricsSnapshot {
  return {
    systemHealth: lerp(a.systemHealth, b.systemHealth, t),
    successRate: lerp(a.successRate, b.successRate, t),
    latencyMs: {
      p50: lerp(a.latencyMs.p50, b.latencyMs.p50, t),
      p99: lerp(a.latencyMs.p99, b.latencyMs.p99, t),
    },
  }
}

/* ── Numeric helpers ──────────────────────────────────────────────── */

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t
}

function lerpNum(a: number, b: number, t: number): number {
  return Math.round(a + (b - a) * t)
}

/* ── Formatting ──────────────────────────────────────────────────── */

/** Format nanoseconds → "mm:ss" (or "hh:mm:ss" for long runs). */
export function formatDuration(ns: number): string {
  const sec = Math.floor(ns / 1e9)
  const h = Math.floor(sec / 3600)
  const m = Math.floor((sec % 3600) / 60)
  const s = sec % 60
  if (h > 0) return `${pad(h)}:${pad(m)}:${pad(s)}`
  return `${pad(m)}:${pad(s)}`
}

function pad(n: number): string {
  return n < 10 ? `0${n}` : `${n}`
}

export function formatPct(v: number): string {
  return `${Math.round(v)}%`
}

export function formatRate(v: number): string {
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k`
  return `${Math.round(v)}`
}

export function formatDepth(v: number): string {
  if (v >= 10000) return `${(v / 1000).toFixed(0)}k`
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k`
  return `${v}`
}

/* ── Service → queue mapping ─────────────────────────────────────── */

const SERVICE_QUEUE_MAP: Record<string, string> = {
  orders: 'orders.work',
  payments: 'payments.work',
  analytics: 'analytics.events',
}

/** Returns the primary queue ID for a service, or undefined (e.g. gateway). */
export function serviceQueueID(serviceID: string): string | undefined {
  return SERVICE_QUEUE_MAP[serviceID]
}

/** Returns queue snapshot for a given queue ID, or undefined. */
export function findQueue(queues: QueueSnapshot[], queueID: string): QueueSnapshot | undefined {
  return queues.find((q) => q.id === queueID)
}

/** Returns pool snapshot for a given queue ID, or undefined. */
export function findPool(pools: PoolSnapshot[], queueID: string): PoolSnapshot | undefined {
  return pools.find((p) => p.queue === queueID)
}

/* ── Service status helpers ──────────────────────────────────────── */

export function statusColor(status: ServiceStatus): string {
  switch (status) {
    case 'healthy': return 'var(--ok)'
    case 'degraded': return 'var(--warn)'
    case 'stalled': return 'var(--danger)'
    case 'failed':  return 'var(--danger)'
    case 'idle':    return 'var(--idle)'
  }
}

export function isStub(svc: ServiceSnapshot): boolean {
  return svc.status === 'idle'
}

/* ── Map layout ─────────────────────────────────────────────────── */

export interface NodeLayout {
  id: string
  x: number
  y: number
}

export interface EdgeLayout {
  from: string
  to: string
}

/** Absolute coordinates in a 1100×600 SVG viewbox. */
export const MAP_NODES: NodeLayout[] = [
  { id: 'gateway',       x: 550, y: 70  },
  { id: 'orders',        x: 340, y: 230 },
  { id: 'payments',      x: 760, y: 230 },
  { id: 'identity',      x: 160, y: 230 },
  { id: 'analytics',     x: 440, y: 430 },
  { id: 'notifications', x: 700, y: 430 },
  { id: 'audit',         x: 160, y: 430 },
]

export const MAP_EDGES: EdgeLayout[] = [
  { from: 'gateway', to: 'orders' },
  { from: 'gateway', to: 'payments' },
  { from: 'gateway', to: 'identity' },
  { from: 'gateway', to: 'analytics' },
  { from: 'orders',  to: 'analytics' },
  { from: 'payments', to: 'notifications' },
  { from: 'orders',  to: 'audit' },
]

/** Map serviceID → node layout (constant lookup). */
export const NODE_BY_ID: Record<string, NodeLayout> = Object.fromEntries(
  MAP_NODES.map((n) => [n.id, n]),
)
