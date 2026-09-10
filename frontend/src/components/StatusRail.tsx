import type { MergedSnapshot, ConnectionState, ObjectiveSnapshot, RunControl } from '../types/game'
import { formatAgo, formatPct } from '../lib/snapshot'

interface StatusRailProps {
  status: ConnectionState['status']
  error?: string
  snapshot: MergedSnapshot
  runControl: RunControl
}

export function StatusRail({ status, error, snapshot, runControl }: StatusRailProps) {
  const m = snapshot.metrics
  const latencyP50 = m.latencyStale ? '—' : `${m.latencyMs.p50.toFixed(0)}ms`
  const latencyP99 = m.latencyStale ? '—' : `${m.latencyMs.p99.toFixed(0)}ms`
  const success = m.successStale ? '—' : formatPct(m.successRate * 100)

  return (
    <header className="status-rail">
      <div className="rail-left">
        <h1>P0-BLACKOUT</h1>
        <span className="rail-sep">|</span>
        <span className="rail-clock">{snapshot.clock}</span>
        <span className={`pill run ${snapshot.runStatus}`}>{snapshot.runStatus}</span>
        <span className={`pill phase ${snapshot.phase}`}>{snapshot.phase}</span>
      </div>

      <div className="rail-metrics">
        <Metric label="health" value={formatPct(m.systemHealth)} accent={m.systemHealth < 50 ? 'danger' : m.systemHealth < 70 ? 'warn' : 'ok'} />
        <Metric label="success" value={success} age={m.successAgeMs} stale={m.successStale} accent={!m.successStale && m.successRate < 0.7 ? 'danger' : !m.successStale && m.successRate < 0.85 ? 'warn' : 'ok'} />
        <Metric label="p50" value={latencyP50} age={m.latencyAgeMs} stale={m.latencyStale} />
        <Metric label="p99" value={latencyP99} age={m.latencyAgeMs} stale={m.latencyStale} />
        <Metric label="tick" value={`${snapshot.tick}`} />
      </div>

      <div className="rail-objectives">{(snapshot.objectives ?? []).map((o) => <Objective o={o} key={o.id} />)}</div>

      {snapshot.runStatus === 'ended' && (
        <button className="pill pill-btn" onClick={() => runControl('retry')}>
          play again
        </button>
      )}

      <span className={`pill conn ${status}`}>
        {status === 'connected' ? 'live' : status === 'connecting' ? 'connecting…' : 'offline'}
      </span>
      {error && <span className="pill conn error">{error}</span>}
    </header>
  )
}

function Objective({ o }: { o: ObjectiveSnapshot }) {
  const display = o.role === 'survive'
    ? `${fmtClock(o.current)} / ${fmtClock(o.target)}`
    : `${Math.round(o.current)}% of ${Math.round(o.target)}%`
  return (
    <span className={`obj ${o.role} ${o.met ? 'met' : ''}`}>
      <span className="obj-label">{o.label}</span>
      <span className="obj-val">{display}</span>
    </span>
  )
}

function fmtClock(sec: number): string {
  const s = Math.floor(Math.max(0, sec))
  const m = Math.floor(s / 60)
  const r = s % 60
  return `${m}:${r < 10 ? '0' : ''}${r}`
}

function Metric({ label, value, age, stale, accent }: { label: string; value: string; age?: number; stale?: boolean; accent?: string }) {
  const ago = stale && age !== undefined ? formatAgo(age) : ''
  return (
    <span className={`rail-metric ${stale ? 'metric-stale' : ''} ${accent ?? ''}`}>
      <span className="metric-label">{label}</span>
      <span className="metric-value">
        {value}
        {ago && <em className="metric-age"> · {ago}</em>}
      </span>
    </span>
  )
}
