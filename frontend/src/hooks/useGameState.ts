import { useEffect, useCallback, useReducer, useRef } from 'react'
import type {
  ActionFeedback,
  ActionMessage,
  ActionResultMessage,
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
  | { type: 'ACTION_PENDING'; requestId: string; action: string }
  | { type: 'ACTION_RESULT'; result: ActionFeedback }
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

    case 'ACTION_PENDING':
      return {
        ...state,
        pendingActions: { ...state.pendingActions, [action.requestId]: action.action },
        actionFeedback: undefined,
      }

    case 'ACTION_RESULT': {
      const pendingActions = { ...state.pendingActions }
      if (action.result.requestId) delete pendingActions[action.result.requestId]
      return { ...state, pendingActions, actionFeedback: action.result }
    }

    case 'ERROR':
      return { ...state, error: action.error }

    case 'CLOSE':
      return {
        ...state,
        status: 'disconnected',
        error: 'connection lost',
        pendingActions: {},
      }

    default:
      return state
  }
}

const INIT: ConnectionState = {
  status: 'connecting',
  snapshot: emptySnapshot(),
  pendingActions: {},
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
  const pendingRef = useRef(new Map<string, string>())
  const requestCounter = useRef(0)

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
      if (isLegacyActionResult(msg)) {
        const pending = pendingRef.current.entries().next().value as [string, string] | undefined
        if (!pending) return
        const [requestId, action] = pending
        pendingRef.current.delete(requestId)
        dispatch({ type: 'ACTION_RESULT', result: toFeedback(msg, requestId, action) })
        return
      }

      switch (msg.type) {
        case 'hello':
          dispatch({ type: 'HELLO' })
          break
        case 'snapshot':
          dispatch({ type: 'SNAPSHOT', snapshot: msg })
          break
        case 'action_result': {
          const pending = resolvePending(pendingRef.current, msg)
          dispatch({
            type: 'ACTION_RESULT',
            result: toFeedback(msg, pending?.[0] ?? msg.requestId, pending?.[1] ?? msg.action),
          })
          break
        }
      }
    }

    ws.onerror = () => {
      dispatch({ type: 'ERROR', error: 'websocket error' })
    }

    ws.onclose = () => {
      if (wsRef.current === ws) wsRef.current = null
      pendingRef.current.clear()
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
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      dispatch({
        type: 'ACTION_RESULT',
        result: { action, ok: false, message: 'Action not sent — backend is offline.' },
      })
      return undefined
    }
    const requestId = `action-${Date.now()}-${requestCounter.current++}`
    const msg: ActionMessage = { type: 'action', action, payload: payload ?? {}, requestId }
    pendingRef.current.set(requestId, action)
    dispatch({ type: 'ACTION_PENDING', requestId, action })
    ws.send(JSON.stringify(msg))
    return requestId
  }, [])

  const runControl = useCallback<RunControl>((action) => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) return
    const msg: ControlMessage = { type: 'control', action }
    ws.send(JSON.stringify(msg))
  }, [])

  return { ...state, runAction, runControl }
}

type ActionResultLike = Pick<ActionResultMessage, 'ok' | 'message' | 'error' | 'requestId' | 'action'>

function isLegacyActionResult(message: unknown): message is ActionResultLike & { type?: undefined } {
  if (!message || typeof message !== 'object') return false
  const candidate = message as Record<string, unknown>
  return candidate.type === undefined && typeof candidate.ok === 'boolean'
}

function resolvePending(
  pending: Map<string, string>,
  result: ActionResultMessage,
): [string, string] | undefined {
  if (result.requestId && pending.has(result.requestId)) {
    const action = pending.get(result.requestId)!
    pending.delete(result.requestId)
    return [result.requestId, action]
  }
  if (result.requestId) return undefined
  const first = pending.entries().next().value as [string, string] | undefined
  if (first) pending.delete(first[0])
  return first
}

function toFeedback(
  result: ActionResultLike,
  requestId?: string,
  action?: string,
): ActionFeedback {
  const actionLabel = action?.replace(/_/g, ' ')
  return {
    requestId,
    action,
    ok: result.ok,
    message: result.message ?? result.error ?? (result.ok
      ? `${actionLabel ?? 'Action'} applied.`
      : `${actionLabel ?? 'Action'} failed.`),
  }
}
