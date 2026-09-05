import type { MergedSnapshot, ConnectionState } from '../types/game'
import { formatPct } from '../lib/snapshot'

interface StatusRailProps {
  status: ConnectionState['status']
  error?: string
  snapshot: MergedSnapshot
}

export function StatusRail({ status, error, snapshot }: StatusRailProps) {
  const m = snapshot.metrics
  const latencyP50 = m.latencyMs.p50.toFixed(0)
  const latencyP99 = m.latencyMs.p99.toFixed(0)

  return (
    <header className="status-rail">
      <div className="rail-left">
        <h1>P0-BLACKOUT</h1>
        <span className="rail-sep">|</span>
        <span className="rail-clock">{snapshot.clock}</span>
        <span className={`pill phase ${snapshot.phase}`}>{snapshot.phase}</span>
      </div>

      <div className="rail-metrics">
        <Metric label="health" value={formatPct(m.systemHealth)} accent={m.systemHealth < 50 ? 'danger' : m.systemHealth < 70 ? 'warn' : 'ok'} />
        <Metric label="success" value={formatPct(m.successRate * 100)} accent={m.successRate < 0.7 ? 'danger' : m.successRate < 0.85 ? 'warn' : 'ok'} />
        <Metric label="p50" value={`${latencyP50}ms`} />
        <Metric label="p99" value={`${latencyP99}ms`} />
        <Metric label="tick" value={`${snapshot.tick}`} />
      </div>

      <span className={`pill conn ${status}`}>
        {status === 'connected' ? 'live' : status === 'connecting' ? 'connecting…' : 'offline'}
      </span>
      {error && <span className="pill conn error">{error}</span>}
    </header>
  )
}

function Metric({ label, value, accent }: { label: string; value: string; accent?: string }) {
  return (
    <span className={`rail-metric ${accent ?? ''}`}>
      <span className="metric-label">{label}</span>
      <span className="metric-value">{value}</span>
    </span>
  )
}
