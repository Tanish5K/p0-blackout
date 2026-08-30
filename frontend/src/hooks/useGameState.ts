import { useEffect, useState, useRef } from 'react'
import type { ConnectionState, ServerMessage } from '../types/game'

// Opens a WebSocket to the backend via the Vite proxy and reports
// connection status. Phase 0 goal: print "connected" on the console.
export function useGameState(): ConnectionState {
  const [state, setState] = useState<ConnectionState>({
    status: 'connecting',
  })
  const wsRef = useRef<WebSocket | null>(null)

  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${protocol}://${window.location.host}/ws`)
    wsRef.current = ws

    ws.onopen = () => {
      setState((s) => ({ ...s, status: 'connected', error: undefined }))
      console.log('connected')
    }

    ws.onmessage = (event) => {
      let msg: ServerMessage
      try {
        msg = JSON.parse(event.data)
      } catch {
        return
      }
      setState((s) => ({ ...s, lastMessage: msg }))
    }

    ws.onerror = () => {
      setState((s) => ({ ...s, error: 'websocket error' }))
    }

    ws.onclose = () => {
      setState((s) => ({
        ...s,
        status: 'disconnected',
        error: 'websocket closed',
      }))
    }

    return () => {
      ws.close()
      wsRef.current = null
    }
  }, [])

  return state
}
