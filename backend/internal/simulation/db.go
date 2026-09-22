package simulation

import (
	"sync"
	"sync/atomic"
	"time"
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

	// failRateMult is the campaign cascade: Incident 1's aggressive scaling
	// raises it and Incident 2's DBs are seeded from it, so a hot prior run
	// makes failures more likely (DB.SetFailureMult).
	failRateMult float64

	// Emergency-failover overrides: while a failover lease is live the service
	// reads from a speed-shifted replica (failLatMult < 1 latency) that has its
	// own elevated inconsistency rate (failBaseAdd); after the lease it keeps a
	// small residual error rate (residFail) for a while — the "imperfect fix"
	// the metrics are meant to show. Zero values = no failover.
	failLatMult float64
	failBaseAdd float64
	failUntil   time.Time
	residFail   float64
	residUntil  time.Time

	active atomic.Int64 // current concurrent operations
}

// NewDB builds a DB with sensible defaults.
func NewDB(baseMS, capacity, maxActive float64) *DB {
	return &DB{
		baseMS:      baseMS,
		capacity:    capacity,
		maxActive:   maxActive,
		sat:         2,
		contention:  1,
		mult:        1,
		baseFail:    0.002,
		maxFail:     0.01,
		failRateMult: 1,
		failLatMult:  1,
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
	base, cap_, maxA, sat, cont, mult, fmult := d.baseMS, d.capacity, d.maxActive, d.sat, d.contention, d.mult, d.failLatMult
	failoverNow := d.failoverActiveLocked(time.Now())
	d.mu.RUnlock()
	// The failover's ~5x speed shift only applies while the replica lease is
	// LIVE; after it expires the service is back on the (still-spiking)
	// primary. The residual error rate is a failure-side artifact, not a
	// latency one.
	if !failoverNow {
		fmult = 1
	}
	backlog := pending / cap_
	// failLatMult defaults to 1 (no failover); the spike's SetMultiplier and
	// the failover's speed shift multiply cumulatively on top of the baseline.
	return base * mult * fmult * (1 + cont*(active/maxA) + sat*backlog*backlog)
}

// FailureChance returns the per-message failure probability. Kept low and
// largely uncorrelated: a redelivered failure is transient noise (Incident 1
// has no poison message), slightly load-sensitive but capped well below a
// retry storm. The campaign cascade (failRateMult) and the failover's replica
// inconsistency (failBaseAdd/residFail) push it up but stay bounded.
func (d *DB) FailureChance(pending int64) float64 {
	d.mu.RLock()
	base, cap_, maxF, mult := d.baseFail, d.capacity, d.maxFail, d.failRateMult
	now := time.Now()
	failoverNow := now.Before(d.failUntil)
	residNow := now.Before(d.residUntil)
	baseAdd := d.failBaseAdd
	resid := d.residFail
	d.mu.RUnlock()

	p := base*mult*(1+float64(pending)/cap_) + failoverAdd(failoverNow, baseAdd) + failoverAdd(residNow, resid)
	ceiling := maxF * maxFloat(mult, 1)
	if p > ceiling {
		p = ceiling
	}
	return p
}

// failoverAdd returns add when active, 0 otherwise (helper keeps the branch out
// of the LatencyMS read).
func failoverAdd(active bool, add float64) float64 {
	if active {
		return add
	}
	return 0
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func (d *DB) failoverActiveLocked(now time.Time) bool {
	return now.Before(d.failUntil)
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

// SetFailureMult is the campaign-cascade hook: Incident 1's aggressive scaling
// raises the multiplier before Incident 2 starts, and every DB is seeded from
// it so the prior run's behaviour has a real, visible price. 1 = baseline.
func (d *DB) SetFailureMult(m float64) {
	if m <= 0 {
		return
	}
	d.mu.Lock()
	d.failRateMult = m
	d.mu.Unlock()
}

// FailureMult reads back the current cascade multiplier.
func (d *DB) FailureMult() float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.failRateMult
}

// BeginFailover shifts this DB onto a degraded replica for the failover lease:
// latency drops (failLatMult < 1), the replica surfaces its own inconsistency
// rate during the lease, and a small residual error rate lingers for a second
// lease — the "imperfect fix". Reapplying extends the lease instead of stacking
// (a re-escalation buys more time, not cumulative discounts).
func (d *DB) BeginFailover(lease time.Duration, latMult, inconsistency, residual float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	d.failLatMult = latMult
	d.failBaseAdd = inconsistency
	d.failUntil = now.Add(lease)
	d.residFail = residual
	d.residUntil = now.Add(2 * lease)
}

// FailoverUntil returns when the current failover lease expires (zero Time if
// none). Exposed for verification/tests.
func (d *DB) FailoverUntil() time.Time {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.failUntil
}

// FailoverLease is how long one emergency_db_failover runs: the service reads
// off the speed-shifted replica for this window, keeps a residual error rate
// for a second lease after, then the override clears (BeginFailover 0.2/0.05/
// 0.01).
const FailoverLease = 60 * time.Second
