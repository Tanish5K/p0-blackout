import type { MergedSnapshot, RunAction } from '../types/game'
import {
  serviceQueueID,
  findQueue,
  findPool,
  statusColor,
  isStub,
  formatDepth,
} from '../lib/snapshot'

interface InspectorProps {
  snapshot: MergedSnapshot
  selectedId: string | null
  onClose: () => void
  onAction: RunAction
}

export function Inspector({ snapshot, selectedId, onClose, onAction }: InspectorProps) {
  if (!selectedId) return null

  const svc = snapshot.services.find((s) => s.id === selectedId)
  if (!svc) return null

  if (isStub(svc)) {
    return (
      <aside className="inspector">
        <InspectorHeader name={svc.name} onClose={onClose} />
        <div className="inspector-stub">
          <span className="stub-icon">◯</span>
          <p>Not yet part of this incident.</p>
          <p className="stub-sub">Topology arrives in a later incident.</p>
        </div>
      </aside>
    )
  }

  const queueID = serviceQueueID(selectedId)
  const queue = queueID ? findQueue(snapshot.queues, queueID) : undefined
  const pool = queueID ? findPool(snapshot.pools, queueID) : undefined
  const hasControls = pool !== undefined

  return (
    <aside className="inspector">
      <InspectorHeader name={svc.name} onClose={onClose} />

      <div className="inspector-section">
        <h3>Health</h3>
        <div className="stat-row">
          <span className="stat-label">Status</span>
          <span className="stat-value" style={{ color: statusColor(svc.status) }}>
            {svc.status}
          </span>
        </div>
        <HealthBar value={svc.health} label={`${Math.round(svc.health)}%`} />
        <div className="stat-row">
          <span className="stat-label">Load</span>
          <span className="stat-value">{Math.round(svc.load * 100)}%</span>
        </div>
        <LoadBar value={svc.load} />
      </div>

      {queue && (
        <div className="inspector-section">
          <h3>Queue — {queue.id}</h3>
          <div className="stat-row">
            <span className="stat-label">Depth</span>
            <span className="stat-value">{formatDepth(queue.depth)}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">In-flight</span>
            <span className="stat-value">{queue.inFlight}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Unacked</span>
            <span className="stat-value">{queue.unacked}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Rate in</span>
            <span className="stat-value">{queue.rateIn.toFixed(1)}/s</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Rate out</span>
            <span className="stat-value">{queue.rateOut.toFixed(1)}/s</span>
          </div>
        </div>
      )}

      {hasControls && (
        <div className="inspector-section">
          <h3>Operations</h3>

          {pool && (
            <div className="stat-row">
              <span className="stat-label">Workers</span>
              <span className="stat-value">
                <span className="workers-count">{pool.workers}</span>
                <span className="worker-btns">
                  <button
                    className="mgmt-btn"
                    onClick={() => onAction('scale_workers', { service: selectedId, delta: -1 })}
                    aria-label="Remove a worker"
                  >
                    −
                  </button>
                  <button
                    className="mgmt-btn"
                    onClick={() => onAction('scale_workers', { service: selectedId, delta: 1 })}
                    aria-label="Add a worker"
                  >
                    +
                  </button>
                </span>
              </span>
            </div>
          )}

          {selectedId === 'orders' && pool && (
            <div className="stat-row">
              <span className="stat-label">Processing</span>
              <span className="stat-value">
                <span className={`mode-chip ${svc.synchronous ? 'sync' : 'async'}`}>
                  {svc.synchronous ? 'sync' : 'async'}
                </span>
                <button
                  className="mgmt-btn"
                  onClick={() =>
                    onAction('set_processing_mode', {
                      service: selectedId,
                      mode: svc.synchronous ? 'async' : 'sync',
                    })
                  }
                  aria-label="Toggle processing mode"
                >
                  toggle
                </button>
              </span>
            </div>
          )}

          <div className="stat-row">
            <span className="stat-label">Connections</span>
            <span className="stat-value">
              {svc.wired ? (
                <button
                  className="mgmt-btn warn"
                  onClick={() => onAction('pause_service', { service: selectedId })}
                  aria-label={`Pause ${svc.name}`}
                >
                  pause
                </button>
              ) : (
                <button
                  className="mgmt-btn ok"
                  onClick={() => onAction('resume_service', { service: selectedId })}
                  aria-label={`Resume ${svc.name}`}
                >
                  resume
                </button>
              )}
            </span>
          </div>
        </div>
      )}

      {!hasControls && !selectedId.startsWith('gateway') && (
        <div className="inspector-section stub">
          <p>No queue attached yet</p>
        </div>
      )}
      {hasControls && !svc.wired && (
        <p className="control-hint">Paused — traffic is accumulating in the queue.</p>
      )}
    </aside>
  )
}

/* ── Sub-components ──────────────────────────────────────────────── */

function InspectorHeader({ name, onClose }: { name: string; onClose: () => void }) {
  return (
    <div className="inspector-header">
      <h2>{name}</h2>
      <button className="inspector-close" onClick={onClose} aria-label="Close inspector">
        ×
      </button>
    </div>
  )
}

function HealthBar({ value, label }: { value: number; label: string }) {
  const color = value < 50 ? 'var(--danger)' : value < 70 ? 'var(--warn)' : 'var(--ok)'
  return (
    <div className="bar-wrap">
      <div
        className="bar health-bar"
        style={{ width: `${Math.min(value, 100)}%`, background: color }}
      />
      <span className="bar-label">{label}</span>
    </div>
  )
}

function LoadBar({ value }: { value: number }) {
  const pct = Math.min(value * 100, 100)
  const color = value > 0.9 ? 'var(--danger)' : value > 0.7 ? 'var(--warn)' : 'var(--cyan)'
  return (
    <div className="bar-wrap">
      <div
        className="bar load-bar"
        style={{ width: `${pct}%`, background: color }}
      />
    </div>
  )
}