import type { OutcomeSnapshot } from '../types/game'
import { formatPct } from '../lib/snapshot'

interface PostmortemProps {
  outcome: OutcomeSnapshot
  clock: string
  onClose: () => void
  onRetry: () => void
}

export function Postmortem({ outcome, clock, onClose, onRetry }: PostmortemProps) {
  return (
    <div className="postmortem-overlay" role="dialog" aria-modal="true">
      <div className={`postmortem-card ${outcome.failed ? 'failed' : 'survived'}`}>
        <button className="inspector-close" onClick={onClose} aria-label="Close postmortem">×</button>

        <h2 className={outcome.failed ? 'verdict-fail' : 'verdict-pass'}>
          {outcome.failed ? 'INCIDENT NOT RESOLVED' : 'INCIDENT SURVIVED'}
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
            <span className="pm-value">{formatPct(outcome.success * 100)}</span>
          </div>
          <div className="pm-metric">
            <span className="pm-label">P99 latency</span>
            <span className="pm-value">{outcome.p99Ms.toFixed(0)}ms</span>
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
            {outcome.failed ? 'Play again' : 'Run it back'}
          </button>
          <button className="postmortem-dismiss" onClick={onClose}>
            Inspect the frozen state
          </button>
        </div>

        <p className="postmortem-hint">
          The backend is paused on this final state — pressing Play again
          restarts the incident with a clean slate.
        </p>
      </div>
    </div>
  )
}