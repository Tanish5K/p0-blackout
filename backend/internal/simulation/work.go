package simulation

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// ErrTransient is returned when simulated processing "fails" — the consumer
// nacks the message so the broker redelivers it. Failures are rare, transient
// noise (see DB.FailureChance), never poison-message storms: retry limits and
// DLQs are Incident 3 tooling, not part of this run.
var ErrTransient = errors.New("simulated transient service failure")

// Work processes a single message from the given queue against its simulated
// database. This is the real worker loop body: it walks the concurrency
// (BeginOp/EndOp), sleeps proportional to the DB's load curve, then reports
// latency and outcome to the runtime.
func Work(ctx context.Context, rt *Runtime, queue string) error {
	db := rt.DBForQueue(queue)
	if db == nil {
		return nil // unknown queue: not our problem
	}

	active := db.BeginOp()
	defer db.EndOp()

	var pending int64
	if tr := rt.Tracker(queue); tr != nil {
		pending = tr.Pending()
	}

	// Failure is decided up front, before any work is "done", so a rejected
	// message does not also get recorded as a latency sample.
	if rand.Float64() < db.FailureChance(pending) {
		rt.AddFailed(queue, 1)
		return ErrTransient
	}

	// Latency scales with concurrency and backlog (DB.LatencyMS), plus a small
	// per-message jitter so workers don't perfectly synchronize.
	ms := db.LatencyMS(float64(active), float64(pending)) * (0.85 + rand.Float64()*0.3)

	select {
	case <-time.After(time.Duration(ms * float64(time.Millisecond))):
	case <-ctx.Done():
		return ctx.Err()
	}

	rt.RecordLatency(queue, time.Duration(ms*float64(time.Millisecond)))
	rt.AddAcked(queue, 1)
	return nil
}
