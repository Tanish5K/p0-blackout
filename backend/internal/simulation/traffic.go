package simulation

import "time"

// IdentityShare is the fraction of inbound requests that also pass through the
// synchronous Identity hop in Incident 2 (0 when Identity is not wired).
const IdentityShare = 0.30

// WindowSize is how many ticks each rolling rate window holds. Rates are
// reported as averages over this window rather than instantaneous per-tick
// counts, so downstream UIs won't flicker.
const WindowSize = 20

// TrafficProfile defines how the inbound request rate evolves over the run.
// Implementations must be deterministic functions of elapsed time — the RNG
// only adds variance on top of the underlying curve.
type TrafficProfile interface {
	RateAt(elapsed time.Duration) float64
}

// ConstantProfile is a flat request rate (useful for the Phase 2 demo and
// Chaos Mode base).
type ConstantProfile struct {
	Rate float64
}

func (p ConstantProfile) RateAt(time.Duration) float64 { return p.Rate }

// StampedeProfile is Incident 1: demand climbs linearly from BaseRate to
// PeakRate over Ramp, holds, then decays. Per the plan, traffic goes
// 200 → 10,000 req/s over five minutes.
type StampedeProfile struct {
	BaseRate float64
	PeakRate float64
	Ramp     time.Duration
	Hold     time.Duration
}

// RateAt evaluates the curve at elapsed time.
func (p StampedeProfile) RateAt(elapsed time.Duration) float64 {
	switch {
	case elapsed >= p.Ramp+p.Hold:
		// decay after the hold
		return p.PeakRate
	case elapsed >= p.Ramp:
		return p.PeakRate
	default:
		// linear ramp
		f := float64(elapsed) / float64(p.Ramp)
		return p.BaseRate + (p.PeakRate-p.BaseRate)*f
	}
}

// pushWindow appends a sample to a fixed-size rolling window, dropping the
// oldest element when full.
func pushWindow[T any](w []T, v T) []T {
	if len(w) >= WindowSize {
		copy(w, w[1:])
		w[len(w)-1] = v
		return w
	}
	return append(w, v)
}

// windowSum returns the sum of a numeric window; zero on empty.
func windowSum[T ~int64 | ~float64](w []T) T {
	var total T
	for _, v := range w {
		total += v
	}
	return total
}
