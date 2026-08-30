import { useGameState } from './hooks/useGameState'

export default function App() {
  const conn = useGameState()

  return (
    <main className="app">
      <header className="status-rail">
        <h1>P0-BLACKOUT</h1>
        <span className={`conn pill ${conn.status}`}>{conn.status}</span>
      </header>
      <section className="system-map">
        {conn.status === 'connected' ? (
          <p className="placeholder">Backend connected. Game state arrives in a later phase.</p>
        ) : (
          <p className="placeholder">{conn.error ?? 'Connecting…'}</p>
        )}
      </section>
    </main>
  )
}
