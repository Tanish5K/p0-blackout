package simulation

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// ErrDropped is returned when simulated processing "fails" — the consumer
// nacks with requeue=false, so the message is LOST, not redelivered. The name
// is deliberately honest: an injected failure that never recovers on retry is
// a permanent loss, not a transient error. Failures are rare noise (see
// DB.FailureChance); the Customer Success objective reads their real cost.
// Genuine retry/recovery semantics (requeue with capped redeliveries, DLQ) are
// Incident 3 tooling via set_retry_policy / route_to_dlq, not this run.
var ErrDropped = errors.New("simulated worker failure: message dropped")

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
		return ErrDropped
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
