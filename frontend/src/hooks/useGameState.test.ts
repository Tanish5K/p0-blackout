import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useGameState } from './useGameState'

class MockWebSocket {
  static OPEN = 1
  static instances: MockWebSocket[] = []

  readyState = MockWebSocket.OPEN
  sent: string[] = []
  onopen: ((event: Event) => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null
  onerror: (() => void) | null = null
  onclose: (() => void) | null = null

  constructor(readonly url: string) {
    MockWebSocket.instances.push(this)
  }

  send(data: string) {
    this.sent.push(data)
  }

  close() {}

  open() {
    this.onopen?.(new Event('open'))
    this.message({ type: 'hello', msg: 'connected' })
  }

  message(message: unknown) {
    this.onmessage?.({ data: JSON.stringify(message) } as MessageEvent)
  }
}

describe('useGameState action acknowledgements', () => {
  beforeEach(() => {
    MockWebSocket.instances = []
    vi.stubGlobal('WebSocket', MockWebSocket)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('shows legacy action errors and clears pending state', () => {
    const { result, unmount } = renderHook(() => useGameState())
    const socket = MockWebSocket.instances[0]
    act(() => socket.open())

    act(() => {
      result.current.runAction('emergency_db_failover', { service: 'orders' })
    })
    expect(Object.keys(result.current.pendingActions)).toHaveLength(1)

    act(() => socket.message({ ok: false, error: 'no emergency budget remaining' }))
    expect(result.current.pendingActions).toEqual({})
    expect(result.current.actionFeedback).toMatchObject({
      action: 'emergency_db_failover',
      ok: false,
      message: 'no emergency budget remaining',
    })
    unmount()
  })

  it('correlates typed results when responses arrive out of order', () => {
    const { result, unmount } = renderHook(() => useGameState())
    const socket = MockWebSocket.instances[0]
    act(() => socket.open())

    act(() => {
      result.current.runAction('pause_service', { service: 'orders' })
      result.current.runAction('scale_workers', { service: 'orders', delta: 1 })
    })
    const sent = socket.sent.map((raw) => JSON.parse(raw) as { requestId: string; action: string })

    act(() => socket.message({
      type: 'action_result',
      requestId: sent[1].requestId,
      ok: true,
    }))
    expect(result.current.pendingActions).toHaveProperty(sent[0].requestId)
    expect(result.current.pendingActions).not.toHaveProperty(sent[1].requestId)
    expect(result.current.actionFeedback).toMatchObject({ action: 'scale_workers', ok: true })

    act(() => socket.message({
      type: 'action_result',
      requestId: sent[0].requestId,
      ok: false,
      error: 'service is already paused',
    }))
    expect(result.current.pendingActions).toEqual({})
    expect(result.current.actionFeedback?.message).toBe('service is already paused')
    unmount()
  })
})
