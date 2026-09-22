import { describe, expect, it } from 'vitest'
import { emptySnapshot, mergeSnapshot } from './snapshot'
import type { BackendEvent, SnapshotMessage } from '../types/game'

function update(overrides: Partial<SnapshotMessage> = {}): SnapshotMessage {
  return {
    type: 'snapshot',
    tick: 1,
    clock: '00:00:01',
    phase: 'running',
    runId: 1,
    runStatus: 'running',
    ...overrides,
  }
}

function event(seq: number, subject: string, tick = 4): BackendEvent {
  return { seq, tick, time: tick * 100_000_000, type: 'action', subject }
}

describe('mergeSnapshot', () => {
  it('preserves explicit zero metric values from delta snapshots', () => {
    const populated = mergeSnapshot(emptySnapshot(), update({
      metrics: {
        systemHealth: 82,
        successRate: 0.91,
        latencyMs: { p50: 45, p99: 180 },
      },
    }))

    const zeroed = mergeSnapshot(populated, update({
      tick: 2,
      metrics: {
        systemHealth: 0,
        successRate: 0,
        latencyMs: { p50: 0, p99: 0 },
        latencyStale: false,
        successStale: false,
        latencyAgeMs: 0,
        successAgeMs: 0,
      },
    }))

    expect(zeroed.metrics).toEqual({
      systemHealth: 0,
      successRate: 0,
      latencyMs: { p50: 0, p99: 0 },
      latencyStale: false,
      successStale: false,
      latencyAgeMs: 0,
      successAgeMs: 0,
    })
  })

  it('keeps distinct same-tick events and deduplicates re-delivered sequences', () => {
    const first = mergeSnapshot(emptySnapshot(), update({
      events: [event(10, 'pause_service'), event(11, 'scale_workers')],
    }))
    const second = mergeSnapshot(first, update({
      tick: 2,
      events: [event(11, 'scale_workers'), event(12, 'set_ack_policy')],
    }))

    expect(second.events.map((item) => item.seq)).toEqual([10, 11, 12])
  })

  it('clears prior-run events and metrics when the run id changes', () => {
    const previous = mergeSnapshot(emptySnapshot(), update({
      runId: 8,
      metrics: { systemHealth: 55, successRate: 0.7, latencyMs: { p50: 80, p99: 900 } },
      events: [event(20, 'old-run')],
    }))
    const restarted = mergeSnapshot(previous, update({
      runId: 9,
      tick: 0,
      clock: '00:00:00',
      metrics: { systemHealth: 0, successRate: 0, latencyMs: { p50: 0, p99: 0 } },
      events: [event(1, 'new-run', 0)],
    }))

    expect(restarted.metrics.systemHealth).toBe(0)
    expect(restarted.metrics.latencyMs.p99).toBe(0)
    expect(restarted.events.map((item) => item.subject)).toEqual(['new-run'])
  })
})
