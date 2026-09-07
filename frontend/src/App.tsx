import { useState } from 'react'
import { useGameState } from './hooks/useGameState'
import { useTick } from './hooks/useTick'
import { StatusRail } from './components/StatusRail'
import { SystemMap } from './components/SystemMap'
import { Inspector } from './components/Inspector'
import { EventTape } from './components/EventTape'
import { Postmortem } from './components/Postmortem'

export default function App() {
  const conn = useGameState()
  const { runAction } = conn
  const display = useTick(conn.status === 'connected' ? conn.snapshot : null)
  const [selectedId, setSelectedId] = useState<string | null>(null)

  // The map renders interpolated (smooth) state; the inspector and tape
  // read the authoritative merged snapshot from the WS.
  const mapSnapshot = display ?? conn.snapshot

  return (
    <main className="app">
      <StatusRail
        status={conn.status}
        error={conn.error}
        snapshot={conn.snapshot}
      />

      <section className="map-area">
        {conn.status === 'connected' ? (
          <SystemMap
            snapshot={mapSnapshot}
            onSelect={setSelectedId}
            selectedId={selectedId}
          />
        ) : (
          <div className="placeholder">
            {conn.status === 'connecting' ? 'Connecting to backend…' : 'Disconnected — reconnecting…'}
          </div>
        )}

        <Inspector
          snapshot={conn.snapshot}
          selectedId={selectedId}
          onClose={() => setSelectedId(null)}
          onAction={runAction}
        />
      </section>

      <EventTape events={conn.snapshot.events} />

      {conn.snapshot.outcome && (
        <Postmortem
          outcome={conn.snapshot.outcome}
          clock={conn.snapshot.clock}
          onClose={() => undefined}
        />
      )}
    </main>
  )
}
