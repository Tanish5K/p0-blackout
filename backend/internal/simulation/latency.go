package simulation

import (
	"sort"
	"sync"
	"time"
)

// LatencySampler keeps a rolling window of real per-message processing times
// measured by the workers. Percentiles are computed from this window, so the
// p50/p99 shown in metrics are actual observed behaviour — not a guess
// derived from backlog math.
type LatencySampler struct {
	mu   sync.RWMutex
	buf  []time.Duration
	next int
	full bool
}

const samplerSize = 512

// NewLatencySampler builds an empty rolling sampler.
func NewLatencySampler() *LatencySampler {
	return &LatencySampler{buf: make([]time.Duration, samplerSize)}
}

// Record appends one observed processing time.
func (s *LatencySampler) Record(d time.Duration) {
	s.mu.Lock()
	s.buf[s.next] = d
	s.next = (s.next + 1) % len(s.buf)
	if s.next == 0 {
		s.full = true
	}
	s.mu.Unlock()
}

// Percentile returns the p-th percentile (0-100) of the current window, or
// zero if nothing has been recorded yet.
func (s *LatencySampler) Percentile(p float64) time.Duration {
	s.mu.RLock()
	n := s.next
	full := s.full
	sorted := make([]time.Duration, 0, len(s.buf))
	if full {
		sorted = append(sorted, s.buf...)
		sorted = sorted[:len(s.buf)]
	} else {
		for i := 0; i < n; i++ {
			sorted = append(sorted, s.buf[i])
		}
	}
	s.mu.RUnlock()

	if len(sorted) == 0 {
		return 0
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1)*p/100 + 0.5)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Count returns how many samples are currently buffered (for diagnostics).
func (s *LatencySampler) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.full {
		return len(s.buf)
	}
	return s.next
}
