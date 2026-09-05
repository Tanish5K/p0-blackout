import { useEffect, useState, useRef } from 'react'
import type { MergedSnapshot } from '../types/game'
import { lerpSnapshot } from '../lib/snapshot'

/**
 * Smoothly interpolates between successive server snapshots at 60fps.
 *
 * `latest` is the authoritative merged snapshot (updated at 10Hz from
 * the WebSocket).  This hook linearly lerps numeric fields over the
 * interpolation window (100ms = one tick) so the renderer produces
 * smooth motion between ticks.
 *
 * Returns the interpolated snapshot to render, or `null` before the
 * first snapshot arrives.
 */
export function useTick(latest: MergedSnapshot | null, intervalMs = 100): MergedSnapshot | null {
  const [display, setDisplay] = useState<MergedSnapshot | null>(latest)

  // Refs that survive across frames without triggering re-renders
  const prevSnap = useRef<MergedSnapshot | null>(null)
  const targetSnap = useRef<MergedSnapshot | null>(latest)
  const startT = useRef<number>(0)
  const rafId = useRef<number>(0)
  const hasStarted = useRef(false)

  // Whenever `latest` changes (new snapshot arrives from WS), update
  // the interpolation targets.
  useEffect(() => {
    if (!latest) return

    if (!hasStarted.current) {
      // First snapshot: show immediately, no lerp needed.
      hasStarted.current = true
      prevSnap.current = latest
      targetSnap.current = latest
      startT.current = performance.now()
      setDisplay(latest)
      return
    }

    // Advance: old target becomes prev; new snapshot becomes target.
    prevSnap.current = targetSnap.current ?? latest
    targetSnap.current = latest
    startT.current = performance.now()

    // Kick the rAF loop
    if (rafId.current) cancelAnimationFrame(rafId.current)

    const frame = () => {
      const elapsed = performance.now() - startT.current
      const t = Math.min(elapsed / intervalMs, 1)

      const prev = prevSnap.current
      const target = targetSnap.current

      if (prev && target) {
        setDisplay(lerpSnapshot(prev, target, t))
      } else if (target) {
        setDisplay(target)
      }

      // Continue lerp until interval elapsed
      if (t < 1) {
        rafId.current = requestAnimationFrame(frame)
      }
    }

    rafId.current = requestAnimationFrame(frame)
  }, [latest, intervalMs])

  // Cleanup rAF on unmount
  useEffect(() => {
    return () => {
      if (rafId.current) cancelAnimationFrame(rafId.current)
    }
  }, [])

  return display
}
