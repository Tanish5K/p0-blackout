import { useEffect, useCallback, useReducer, useRef } from 'react'
import type {
  ActionMessage,
  ConnectionState,
  ControlMessage,
  RunAction,
  RunControl,
  SnapshotMessage,
  ServerMessage,
} from '../types/game'
import { emptySnapshot, mergeSnapshot } from '../lib/snapshot'

/* ── Action types for the internal reducer ─────────────────────────── */

type Action =
  | { type: 'OPEN' }
  | { type: 'HELLO' }
  | { type: 'SNAPSHOT'; snapshot: SnapshotMessage }
  | { type: 'ERROR'; error: string }
  | { type: 'CLOSE' }

function reducer(state: ConnectionState, action: Action): ConnectionState {
  switch (action.type) {
    case 'OPEN':
      return { ...state, status: 'connecting', error: undefined }

    case 'HELLO':
      return { ...state, status: 'connected', error: undefined }

    case 'SNAPSHOT':
      return {
        ...state,
        status: 'connected',
        snapshot: mergeSnapshot(state.snapshot, action.snapshot),
        error: undefined,
      }

    case 'ERROR':
      return { ...state, error: action.error }

    case 'CLOSE':
      return { ...state, status: 'disconnected', error: 'connection lost' }

    default:
      return state
  }
}

const INIT: ConnectionState = {
  status: 'connecting',
  snapshot: emptySnapshot(),
}

const RECONNECT_MS = 2000
const MAX_ATTEMPTS = 20

/**
 * Opens a WebSocket, merges incoming snapshots into a single authoritative
 * merged snapshot, and exposes connection status plus a stable runAction that
 * broadcasts a player action (scale, pause, sync toggle…) over the live socket.
 * Auto-reconnects on drop with exponential back-off (2-10s).
 */
export function useGameState(): ConnectionState & { runAction: RunAction; runControl: RunControl } {
  const [state, dispatch] = useReducer(reducer, INIT)
  const wsRef = useRef<WebSocket | null>(null)

  const connect = useCallback((attempt: number) => {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${protocol}://${window.location.host}/ws`)
    wsRef.current = ws

    ws.onopen = () => {
      dispatch({ type: 'OPEN' })
    }

    ws.onmessage = (event) => {
      let msg: ServerMessage
      try {
        msg = JSON.parse(event.data) as ServerMessage
      } catch {
        return
      }
      switch (msg.type) {
        case 'hello':
          dispatch({ type: 'HELLO' })
          break
        case 'snapshot':
          dispatch({ type: 'SNAPSHOT', snapshot: msg })
          break
      }
    }

    ws.onerror = () => {
      dispatch({ type: 'ERROR', error: 'websocket error' })
    }

    ws.onclose = () => {
      if (wsRef.current === ws) wsRef.current = null
      dispatch({ type: 'CLOSE' })
      // Auto-reconnect with capped back-off
      const delay = Math.min(RECONNECT_MS * (attempt + 1), 10000)
      if (attempt < MAX_ATTEMPTS) {
        setTimeout(() => connect(attempt + 1), delay)
      }
    }

    return () => {
      ws.close()
    }
  }, [])

  useEffect(() => {
    const cleanup = connect(0)
    return () => cleanup?.()
  }, [connect])

  const runAction = useCallback<RunAction>((action, payload) => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) return
    const msg: ActionMessage = { type: 'action', action, payload: payload ?? {} }
    ws.send(JSON.stringify(msg))
  }, [])

  const runControl = useCallback<RunControl>((action) => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) return
    const msg: ControlMessage = { type: 'control', action }
    ws.send(JSON.stringify(msg))
  }, [])

  return { ...state, runAction, runControl }
}
