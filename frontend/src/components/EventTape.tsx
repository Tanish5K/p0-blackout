import { useEffect, useRef } from 'react'
import type { BackendEvent } from '../types/game'
import { formatDuration } from '../lib/snapshot'

interface EventTapeProps {
  events: BackendEvent[]
}

const MAX_ROWS = 200

export function EventTape({ events }: EventTapeProps) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const rows = events.slice(-MAX_ROWS)

  // Auto-scroll to newest event
  useEffect(() => {
    const el = scrollRef.current
    if (el) el.scrollTop = el.scrollHeight
  }, [rows.length])

  return (
    <footer className="event-tape">
      <div className="event-tape-head">
        <span className="tape-title">EVENT TAPE</span>
        <span className="tape-count">{events.length}</span>
      </div>
      <div className="event-tape-body" ref={scrollRef}>
        {rows.length === 0 && <div className="tape-empty">No events yet…</div>}
        {rows.map((e, i) => (
          <EventRow key={`${e.tick}-${i}`} e={e} />
        ))}
      </div>
    </footer>
  )
}

function EventRow({ e }: { e: BackendEvent }) {
  return (
    <div className={`tape-row tape-${e.type}`}>
      <span className="tape-time">{formatDuration(e.time)}</span>
      <span className="tape-type">{e.type}</span>
      <span className="tape-subject">{e.subject}</span>
      {e.value !== undefined && <span className="tape-value">{e.value}</span>}
      {e.data && <span className="tape-data">{e.data}</span>}
    </div>
  )
}
