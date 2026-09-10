import type { MergedSnapshot, RunStatus } from '../types/game'

interface HomeScreenProps {
  status: 'connecting' | 'connected' | 'disconnected'
  runStatus: RunStatus
  snapshot: MergedSnapshot
  onStart: () => void
}

/**
 * Static campaign briefing the backend can't send as a whole: the live snapshot
 * only carries the current incident, but the home screen's timeline names every
 * incident in the campaign. Mirrors backend/incidents.go.
 */
const CAMPAIGN: { n: number; name: string; desc: string }[] = [
  {
    n: 1,
    name: 'The Great Stampede',
    desc: 'Orders traffic ramps from 200 to 10,000 req/s over five minutes. Ride out the surge: scale workers, shrink queues, and flip the orders pipeline to async before sync mode melts the gateway.',
  },
  {
    n: 2,
    name: 'Falling Workers',
    desc: 'Identity workers start crashing under the new routing load. A harness death-tests the sync path with a read replica added for 120% of orders traffic — keep identity up, restart crashed workers, and spend emergency DB failovers when the replica crowds out.',
  },
]

type CardStatus = 'cleared' | 'next' | 'locked'

/**
 * Full-screen idle gate: the campaign map + a start button for the current
 * incident. The campaign auto-advances server-side (survive → next incident),
 * so the only real control here is "start the next incident shift".
 */
export function HomeScreen({ status, onStart, snapshot }: HomeScreenProps) {
  const ready = status === 'connected'
  const current = snapshot.incidentNumber
  const objectives = snapshot.objectives ?? []

  const cardStatus = (n: number): CardStatus =>
    n < current ? 'cleared' : n === current ? 'next' : 'locked'

  return (
    <div className="start-overlay home-overlay" role="dialog" aria-modal="true">
      <div className="start-card home-card">
        <h1 className="start-title">P0-BLACKOUT</h1>
        <p className="start-subtitle">Live incident response campaign</p>

        <div className="campaign-list">
          {CAMPAIGN.map((inc) => {
            const st = cardStatus(inc.n)
            return (
              <div key={inc.n} className={`campaign-card ${st}`}>
                <div className="incident-head">
                  <span className="incident-number">INC{String(inc.n).padStart(2, '0')}</span>
                  <span className="incident-name">{inc.name}</span>
                  <span className={`incident-status ${st}`}>{st}</span>
                </div>
                <p className="incident-desc">{inc.desc}</p>

                {st === 'next' && objectives.length > 0 && (
                  <ul className="incident-objectives">
                    {objectives.map((o) => (
                      <li key={o.id}>
                        <span className={`obj ${o.role}`}>{o.label}</span>
                        {o.role === 'survive'
                          ? ` — survive the ${fmtMin(o.target)} window`
                          : ` — keep above ${Math.round(o.target)}%`}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )
          })}
        </div>

        <p className="home-hint">
          {current <= CAMPAIGN.length
            ? `Next shift: Incident ${current}.`
            : 'Campaign complete — replay the final incident anytime.'}
        </p>

        <button className="start-button" onClick={onStart} disabled={!ready}>
          {ready
            ? `Start incident shift — ${CAMPAIGN[Math.min(current, CAMPAIGN.length) - 1]?.name ?? ''}`.trim()
            : 'Connecting to backend…'}
        </button>
      </div>
    </div>
  )
}

function fmtMin(sec: number): string {
  const s = Math.floor(Math.max(0, sec))
  const m = Math.floor(s / 60)
  const r = s % 60
  return `${m}:${r < 10 ? '0' : ''}${r}`
}