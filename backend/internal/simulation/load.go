package simulation

import (
	"sync/atomic"
)

// LoadTracker estimates queue "depth" at high frequency without waiting on
// the slow management-API poller: it counts published messages on Publish and
// drained messages on ack, so pending = published - acked tracks load almost
// in real time. The management snapshot is used to periodically reconcile
// drift (client-prediction / server-snapshot pattern) so the estimate stays
// honest even if publishes or acks are missed.
type LoadTracker struct {
	pending atomic.Int64
}

// NewLoadTracker builds an empty tracker (start at zero, mirroring the seeded
// baseline the driver uses for the very first poll).
func NewLoadTracker() *LoadTracker { return &LoadTracker{} }

// AddPublished adds n messages handed to the broker (after a successful
// publish, best-effort).
func (t *LoadTracker) AddPublished(n int64) { t.pending.Add(n) }

// AddAcked subtracts n messages acked by consumers.
func (t *LoadTracker) AddAcked(n int64) { t.pending.Add(-n) }

// Pending returns the current estimated depth.
func (t *LoadTracker) Pending() int64 {
	if p := t.pending.Load(); p < 0 {
		return 0
	} else {
		return p
	}
}

// Reconcile overwrites the estimate with the authoritative depth from the
// queue snapshot (server truth), forgiving any drift.
func (t *LoadTracker) Reconcile(depth int64) {
	if depth < 0 {
		depth = 0
	}
	t.pending.Store(depth)
}
