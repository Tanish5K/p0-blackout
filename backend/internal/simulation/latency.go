package simulation

import (
	"sort"
	"sync"
	"time"
)

// LatencySampler keeps a time-windowed buffer of real per-message processing
// times. Percentiles are computed over the window so p50/p99 reflect CURRENT
// pressure — not a stale burst from the first seconds of a run.
const sampleWindow = 10 * time.Second

type latSample struct {
	t time.Time
	d time.Duration
}

// LatencySampler is a thread-safe time-windowed accumulator.
type LatencySampler struct {
	mu   sync.Mutex
	buf  []latSample
	next int // ring insertion index
	full bool
}

const samplerSize = 4096

// NewLatencySampler builds an empty time-windowed sampler.
func NewLatencySampler() *LatencySampler {
	return &LatencySampler{buf: make([]latSample, samplerSize)}
}

// Record appends one observed processing time tagged with the wall-clock time.
func (s *LatencySampler) Record(d time.Duration) {
	now := time.Now()
	s.mu.Lock()
	s.buf[s.next] = latSample{t: now, d: d}
	s.next = (s.next + 1) % len(s.buf)
	if s.next == 0 {
		s.full = true
	}
	s.mu.Unlock()
}

// active returns the subset of samples within sampleWindow of now, sorted by
// duration. Caller holds s.mu.
func (s *LatencySampler) active(now time.Time) []time.Duration {
	cutoff := now.Add(-sampleWindow)
	n := s.next
	full := s.full
	total := len(s.buf)
	if !full {
		total = n
	}
	active := make([]time.Duration, 0, total)
	for i := 0; i < total; i++ {
		if full {
			idx := (n + i) % len(s.buf)
			if s.buf[idx].t.Before(cutoff) {
				continue
			}
			active = append(active, s.buf[idx].d)
		} else {
			if s.buf[i].t.Before(cutoff) {
				continue
			}
			active = append(active, s.buf[i].d)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i] < active[j] })
	return active
}

// Percentile returns the p-th percentile (0-100) of samples recorded in the
// last 10s, or zero if nothing has been recorded yet.
func (s *LatencySampler) Percentile(p float64) time.Duration {
	now := time.Now()
	s.mu.Lock()
	sorted := s.active(now)
	s.mu.Unlock()

	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1)*p/100 + 0.5)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Count returns the number of samples within the active window (for
// diagnostics).
func (s *LatencySampler) Count() int {
	now := time.Now()
	s.mu.Lock()
	n := len(s.active(now))
	s.mu.Unlock()
	return n
}
