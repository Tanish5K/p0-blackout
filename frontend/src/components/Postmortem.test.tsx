import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { Postmortem } from './Postmortem'

const outcome = {
  failed: false,
  endedAtMs: 480_000,
  health: 72,
  success: 0.88,
  p50Ms: 110,
  p99Ms: 640,
  latencyStale: false,
  successStale: false,
  latencyAgeMs: 0,
  successAgeMs: 0,
  timeline: ['Traffic peaked', 'Orders remained available'],
}

describe('Postmortem', () => {
  it('offers a working path back to the frozen map', () => {
    const onClose = vi.fn()
    render(
      <Postmortem
        outcome={outcome}
        clock="00:08:00"
        incidentNumber={1}
        campaignTotal={2}
        onClose={onClose}
        onRetry={vi.fn()}
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Inspect the frozen state' }))
    expect(onClose).toHaveBeenCalledOnce()
  })
})
