import type { MergedSnapshot, ConnectionState, ObjectiveSnapshot, RunControl } from '../types/game'
import { formatPct } from '../lib/snapshot'

interface StatusRailProps {
  status: ConnectionState['status']
  error?: string
  snapshot: MergedSnapshot
  runControl: RunControl
}

export function StatusRail({ status, error, snapshot, runControl }: StatusRailProps) {
  const m = snapshot.metrics
  const latencyP50 = m.latencyMs.p50.toFixed(0)
  const latencyP99 = m.latencyMs.p99.toFixed(0)

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
        <Metric label="success" value={formatPct(m.successRate * 100)} accent={m.successRate < 0.7 ? 'danger' : m.successRate < 0.85 ? 'warn' : 'ok'} />
        <Metric label="p50" value={`${latencyP50}ms`} />
        <Metric label="p99" value={`${latencyP99}ms`} />
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

function Metric({ label, value, accent }: { label: string; value: string; accent?: string }) {
  return (
    <span className={`rail-metric ${accent ?? ''}`}>
      <span className="metric-label">{label}</span>
      <span className="metric-value">{value}</span>
    </span>
  )
}
