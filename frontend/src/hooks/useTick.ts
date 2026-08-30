import { useEffect, useState, useRef } from 'react'

// Interpolates server snapshots toward 60fps client rendering. Phase 4.
export function useTick<T>(latest: T | null, intervalMs = 100): T | null {
  const [display, setDisplay] = useState<T | null>(latest)
  const latestRef = useRef<T | null>(latest)

  useEffect(() => {
    latestRef.current = latest
  }, [latest])

  useEffect(() => {
    const id = setInterval(() => {
      setDisplay(latestRef.current)
    }, intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])

  return display
}
