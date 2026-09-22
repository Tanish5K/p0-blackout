import type { OutcomeSnapshot } from '../types/game'
import { formatAgo, formatPct } from '../lib/snapshot'

interface PostmortemProps {
  outcome: OutcomeSnapshot
  clock: string
  incidentNumber: number
  campaignTotal: number
  onClose: () => void
  onRetry: () => void
}

// Stale terminal metrics (no completions in the sample window) render as a
// grayed "—" with an age suffix: the number stopped being true a while back,
// and showing it as current is exactly the misleading reading this fixes.
function metricOutcome(value: string, stale: boolean, ageMs: number): { value: string; stale: boolean; ago: string } {
  const ago = stale ? formatAgo(ageMs) : ''
  return { value: stale ? '—' : value, stale, ago }
}

export function Postmortem({ outcome, clock, incidentNumber, campaignTotal, onClose, onRetry }: PostmortemProps) {
  const success = metricOutcome(formatPct(outcome.success * 100), outcome.successStale, outcome.successAgeMs)
  const p99 = metricOutcome(`${outcome.p99Ms.toFixed(0)}ms`, outcome.latencyStale, outcome.latencyAgeMs)

  const hasNext = !outcome.failed && incidentNumber < campaignTotal
  const nextLabel = outcome.failed
    ? `Retry incident ${incidentNumber}`
    : hasNext
      ? `Continue to incident ${incidentNumber + 1}`
      : 'Run it back'

  return (
    <div className="postmortem-overlay" role="dialog" aria-modal="true">
      <div className={`postmortem-card ${outcome.failed ? 'failed' : 'survived'}`}>
        <button className="inspector-close" onClick={onClose} aria-label="Close postmortem">×</button>

        <h2 className={outcome.failed ? 'verdict-fail' : 'verdict-pass'}>
          {outcome.failed
            ? `INCIDENT ${incidentNumber} NOT RESOLVED`
            : hasNext
              ? `INCIDENT ${incidentNumber} SURVIVED`
              : 'CAMPAIGN COMPLETE'}
        </h2>
        {outcome.reason && <p className="postmortem-reason">{outcome.reason}</p>}

        <div className="postmortem-metrics">
          <div className="pm-metric">
            <span className="pm-label">Final System Health</span>
            <span className={`pm-value ${outcome.health < 50 ? 'danger' : outcome.health < 70 ? 'warn' : 'ok'}`}>
              {formatPct(outcome.health)}
            </span>
          </div>
          <div className="pm-metric">
            <span className="pm-label">Customer Success</span>
            <span className={`pm-value ${success.stale ? 'metric-stale' : ''}`}>
              {success.value}
              {success.ago && <em className="metric-age"> · {success.ago}</em>}
            </span>
          </div>
          <div className="pm-metric">
            <span className="pm-label">P99 latency</span>
            <span className={`pm-value ${p99.stale ? 'metric-stale' : ''}`}>
              {p99.value}
              {p99.ago && <em className="metric-age"> · {p99.ago}</em>}
            </span>
          </div>
          <div className="pm-metric">
            <span className="pm-label">Incident clock</span>
            <span className="pm-value">{clock}</span>
          </div>
        </div>

        <h3 className="postmortem-h">Event timeline</h3>
        <ol className="timeline">
          {outcome.timeline.map((b, i) => (
            <li key={i}>{b}</li>
          ))}
        </ol>

        <div className="postmortem-actions">
          <button className="start-button" onClick={onRetry}>
            {nextLabel}
          </button>
          <button className="postmortem-dismiss" onClick={onClose}>
            Inspect the frozen state
          </button>
        </div>

        <p className="postmortem-hint">
          The backend is paused on this final state —{' '}
          {hasNext
            ? 'continuing launches the next incident with a clean slate.'
            : 'pressing again restarts the incident from scratch.'}
        </p>
      </div>
    </div>
  )
}