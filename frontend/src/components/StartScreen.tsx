import type { RunStatus } from '../types/game'

interface StartScreenProps {
  status: 'connecting' | 'connected' | 'disconnected'
  runStatus: RunStatus
  onStart: () => void
}

/**
 * Full-screen start overlay shown while the backend is idle (runStatus =
 * "idle"): an incident briefing card with the Play button that sends a
 * {"type":"control","action":"start"} over the socket. The button stays
 * disabled until the socket is live so a click can't be silently dropped.
 */
export function StartScreen({ status, onStart }: StartScreenProps) {
  const ready = status === 'connected'
  return (
    <div className="start-overlay" role="dialog" aria-modal="true">
      <div className="start-card">
        <h1 className="start-title">P0-BLACKOUT</h1>
        <p className="start-subtitle">Live incident response drill</p>

        <div className="start-brief">
          <h2>Incident 1 — The Great Stampede</h2>
          <p>
            Orders traffic is about to ramp from 200 to 10,000 req/s over five
            minutes. Keep the system alive through the surge: scale workers,
            shrink queues, and flip the orders pipeline to async before sync
            mode starts melting the gateway.
          </p>
          <ul>
            <li>Survive the 8:00 window.</li>
            <li>System Health stays above 70% (fail below 50% is instant).</li>
            <li>Customer success stays above 85%.</li>
            <li>P99 order latency stays under 500ms.</li>
          </ul>
        </div>

        <button
          className="start-button"
          onClick={onStart}
          disabled={!ready}
        >
          {ready ? 'Start incident shift' : 'Connecting to backend…'}
        </button>
      </div>
    </div>
  )
}