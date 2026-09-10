import type { MergedSnapshot, ServiceSnapshot, QueueSnapshot } from '../types/game'
import {
  MAP_NODES,
  MAP_EDGES,
  NODE_BY_ID,
  statusColor,
  serviceQueueID,
  findQueue,
} from '../lib/snapshot'

const NODE_RADIUS = 42
const LOAD_RING_R  = 50
const STROKE_W     = 3

interface SystemMapProps {
  snapshot: MergedSnapshot
  onSelect: (id: string) => void
  selectedId?: string | null
}

export function SystemMap({ snapshot, onSelect, selectedId }: SystemMapProps) {
  return (
    <svg
      viewBox="0 0 1100 600"
      className="system-map-svg"
      xmlns="http://www.w3.org/2000/svg"
    >
      {/* Edges */}
      {MAP_EDGES.map((e) => (
        <Edge
          key={`${e.from}-${e.to}`}
          from={e.from}
          to={e.to}
          rate={edgeRate(e.from, snapshot)}
        />
      ))}

      {/* Nodes */}
      {MAP_NODES.map((n) => {
        const svc = snapshot.services.find((s) => s.id === n.id)
        const queueID = serviceQueueID(n.id)
        const queue = queueID ? findQueue(snapshot.queues, queueID) : undefined
        return (
          <g
            key={n.id}
            className={`map-node ${selectedId === n.id ? 'selected' : ''}`}
            onClick={() => onSelect(n.id)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => e.key === 'Enter' && onSelect(n.id)}
          >
            <NodeRing cx={n.x} cy={n.y} svc={svc} />
            <NodeRingActive cx={n.x} cy={n.y} svc={svc} />
            <NodeCircle cx={n.x} cy={n.y} svc={svc} />
            <NodeLabel cx={n.x} cy={n.y} svc={svc} />
            {queue && <QueueBadge cx={n.x} cy={n.y} queue={queue} />}
          </g>
        )
      })}
    </svg>
  )
}

/* ── Edge ────────────────────────────────────────────────────────── */

function Edge({ from, to, rate }: { from: string; to: string; rate: number }) {
  const a = NODE_BY_ID[from]
  const b = NODE_BY_ID[to]
  if (!a || !b) return null

  // Speed proportional to rate (1s at 0 req/s, 0.15s at 200 req/s)
  const duration = rate > 0 ? Math.max(0.15, 2 / (rate / 10 + 1)) : 0
  const animated = duration > 0

  return (
    <g className="map-edge">
      <line
        x1={a.x}
        y1={a.y}
        x2={b.x}
        y2={b.y}
        className="edge-line"
      />
      {animated && (
        <line
          x1={a.x}
          y1={a.y}
          x2={b.x}
          y2={b.y}
          className="edge-pulse"
          style={{
            animationDuration: `${duration}s`,
            strokeDasharray: '4 12',
          }}
        />
      )}
    </g>
  )
}

/** Total rateIn for edges originating from `from`. */
function edgeRate(from: string, snap: MergedSnapshot): number {
  // Sum the rateIn of all queues served by downstream services
  const downstream = MAP_EDGES.filter((e) => e.from === from).map((e) => e.to)
  let total = 0
  for (const svcID of downstream) {
    const qID = serviceQueueID(svcID)
    const q = qID ? findQueue(snap.queues, qID) : undefined
    if (q) total += q.rateIn
  }
  return total
}

/* ── Node parts ──────────────────────────────────────────────────── */

// dormantSvc is the union of "paused" and "stalled": a node that is not doing
// work right now. Stalled (paused AND its queue filling with nobody consuming)
// supersedes paused for display — it is the urgent reading the rail's
// latency/success can't see. Stubs read wired=false but status 'idle', so they
// never count as dormant. Either way the node renders grey, pulse off.
function dormantSvc(svc?: ServiceSnapshot): boolean {
  return !!svc && (svc.stalled === true || (!svc.wired && svc.status !== 'idle'))
}

function NodeCircle({ cx, cy, svc }: { cx: number; cy: number; svc?: ServiceSnapshot }) {
  const status = svc?.status ?? 'idle'
  const dormant = dormantSvc(svc)
  const fill = dormant ? 'var(--idle)' : statusColor(status)
  const showPulse = !dormant && status !== 'idle' && status !== 'failed'

  return (
    <>
      {showPulse && (
        <circle
          cx={cx}
          cy={cy}
          r={NODE_RADIUS + 4}
          className="node-pulse-ring"
          style={{ stroke: fill }}
        />
      )}
      <circle
        cx={cx}
        cy={cy}
        r={NODE_RADIUS}
        className="node-circle"
        style={{ fill }}
      />
    </>
  )
}

function NodeRing({ cx, cy }: { cx: number; cy: number; svc?: ServiceSnapshot }) {
  return (
    <circle
      cx={cx}
      cy={cy}
      r={LOAD_RING_R}
      fill="none"
      stroke="var(--line)"
      strokeWidth={STROKE_W}
      className="load-ring-bg"
    />
  )
}

function NodeRingActive({ cx, cy, svc }: { cx: number; cy: number; svc?: ServiceSnapshot }) {
  const load = svc?.load ?? 0
  const circumference = 2 * Math.PI * LOAD_RING_R
  const dashLen = circumference * load
  const gap = circumference - dashLen
  // While dormant (paused or stalled) the arc keeps growing — pressure
  // building in the queue — but renders in the neutral idle colour, not a
  // false "healthy green".
  const fill = dormantSvc(svc) ? 'var(--idle)' : svc ? statusColor(svc.status) : 'var(--idle)'

  return (
    <circle
      cx={cx}
      cy={cy}
      r={LOAD_RING_R}
      fill="none"
      stroke={fill}
      strokeWidth={STROKE_W}
      strokeDasharray={`${dashLen} ${gap}`}
      className="load-ring"
    />
  )
}

function NodeLabel({ cx, cy, svc }: { cx: number; cy: number; svc?: ServiceSnapshot }) {
  const name = svc?.name ?? '—'
  const health = svc?.health ?? 0
  const status = svc?.status ?? 'idle'
  const stalled = dormantSvc(svc) && svc?.stalled === true
  const paused = dormantSvc(svc) && !stalled

  return (
    <>
      <text
        x={cx}
        y={cy - 6}
        textAnchor="middle"
        className="node-label"
      >
        {name}
      </text>
      {status !== 'idle' && !dormantSvc(svc) && (
        <text
          x={cx}
          y={cy + 14}
          textAnchor="middle"
          className="node-stat"
        >
          {Math.round(health)}%
        </text>
      )}
      {(status === 'idle' || dormantSvc(svc)) && (
        <text
          x={cx}
          y={cy + 14}
          textAnchor="middle"
          className="node-stat idle"
        >
          {stalled ? 'stalled' : paused ? 'paused' : 'idle'}
        </text>
      )}
    </>
  )
}

function QueueBadge({ cx, cy, queue }: { cx: number; cy: number; queue: QueueSnapshot }) {
  const depth = queue.depth
  const label = depth >= 1000 ? `${(depth / 1000).toFixed(1)}k` : `${depth}`

  return (
    <g className="queue-badge">
      <rect
        x={cx - 24}
        y={cy + NODE_RADIUS + 10}
        width={48}
        height={18}
        rx={4}
        className="queue-badge-bg"
      />
      <text
        x={cx}
        y={cy + NODE_RADIUS + 23}
        textAnchor="middle"
        className="queue-badge-text"
      >
        {queue.id.split('.').pop()}: {label}
      </text>
    </g>
  )
}
