import { useEffect, useCallback, useReducer } from 'react'
import type {
  ConnectionState,
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
 * merged snapshot, and exposes connection status. Auto-reconnects on drop
 * with exponential back-off (2-10s).
 */
export function useGameState(): ConnectionState {
  const [state, dispatch] = useReducer(reducer, INIT)

  const connect = useCallback((attempt: number) => {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${protocol}://${window.location.host}/ws`)

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

  return state
}
