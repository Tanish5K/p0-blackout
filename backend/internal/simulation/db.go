package simulation

import (
	"sync"
	"sync/atomic"
)

// DB is the simulated shared database a service talks to (§16.1: "DB
// simulation: simulated in Go — latency distribution, capacity, failure
// injection"). Two forces raise per-message latency:
//
//   - contention: more consumers hitting the same capacity concurrently costs
//     more per-message time (Incident 1's "aggressive scaling saturates DB")
//   - saturation: the deeper the backlog relative to capacity, the harder the
//     curve bends (Incident 1's "moderate scaling stays safer")
//
// Incident scripts can override the baseline with SetMultiplier etc., which is
// what produces authored events like Incident 2's "DB latency spikes 10x,
// workers crash mid-processing" on top of the self-tuning baseline.
type DB struct {
	mu         sync.RWMutex
	baseMS     float64 // baseline per-message latency (ms)
	capacity   float64 // outstanding-work threshold where saturation kicks in
	maxActive  float64 // concurrent ops before contention costs kick in
	sat        float64 // superlinear bending of the backlog term
	contention float64 // weight of the concurrency term
	mult       float64 // incident override multiplier (default 1)
	baseFail   float64 // base per-message failure chance
	maxFail    float64 // failure chance ceiling (kept low: transient noise)

	active atomic.Int64 // current concurrent operations
}

// NewDB builds a DB with sensible defaults.
func NewDB(baseMS, capacity, maxActive float64) *DB {
	return &DB{
		baseMS:     baseMS,
		capacity:   capacity,
		maxActive:  maxActive,
		sat:        2,
		contention: 1,
		mult:       1,
		baseFail:   0.002,
		maxFail:    0.01,
	}
}

// BeginOp marks the start of a concurrent database operation and returns the
// now-active concurrency, which feeds the contention term.
func (d *DB) BeginOp() int64 { return d.active.Add(1) }

// EndOp marks the end of a concurrent operation.
func (d *DB) EndOp() { d.active.Add(-1) }

// LatencyMS returns the per-message processing time in milliseconds for the
// given backlog, given the current concurrency (caller passes active from
// BeginOp).
func (d *DB) LatencyMS(active, pending float64) float64 {
	d.mu.RLock()
	base, cap_, maxA, sat, cont, mult := d.baseMS, d.capacity, d.maxActive, d.sat, d.contention, d.mult
	d.mu.RUnlock()
	backlog := pending / cap_
	return base * mult * (1 + cont*(active/maxA) + sat*backlog*backlog)
}

// FailureChance returns the per-message failure probability. Kept low and
// largely uncorrelated: a redelivered failure is transient noise (Incident 1
// has no poison message), slightly load-sensitive but capped well below a
// retry storm.
func (d *DB) FailureChance(pending int64) float64 {
	d.mu.RLock()
	base, cap_, maxF := d.baseFail, d.capacity, d.maxFail
	d.mu.RUnlock()
	p := base * (1 + float64(pending)/cap_)
	if p > maxF {
		p = maxF
	}
	return p
}

// Capacity exposes the saturation threshold (for service-load metrics).
func (d *DB) Capacity() float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.capacity
}

// SetMultiplier is the incident-script override hook (e.g. Incident 2's 10x
// DB latency spike). 1 = baseline.
func (d *DB) SetMultiplier(m float64) {
	if m <= 0 {
		return
	}
	d.mu.Lock()
	d.mult = m
	d.mu.Unlock()
}

// Multiplier reads back the current override.
func (d *DB) Multiplier() float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mult
}

// SetCapacity tunes the saturation threshold (incident tuning).
func (d *DB) SetCapacity(c float64) {
	if c <= 0 {
		return
	}
	d.mu.Lock()
	d.capacity = c
	d.mu.Unlock()
}
