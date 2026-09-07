package events

import (
	"sort"
	"sync"
	"time"
)

// Event is a single state-changing occurrence in the simulation. The event log
// is the basis for the postmortem causal chain (§7) and deterministic replay:
// every tick that changes something appends an Event tagged with its tick
// number, so the sequence can be replayed exactly. Seq is assigned by the Log
// on Append and is unique and monotonic even after Trim culls old entries, so
// a broadcast cursor ("everything since seq N") survives log trimming.
type Event struct {
	Seq     uint64        `json:"seq,omitempty"`
	Tick    int64         `json:"tick"`
	Time    time.Duration `json:"time"` // elapsed since run start
	Type    string        `json:"type"` // request | publish | route | consume | ack | fail | action | metric | state
	Subject string        `json:"subject"`
	Value   float64       `json:"value,omitempty"`
	Data    string        `json:"data,omitempty"`
}

// Log is an append-only event accumulator. Stable across ticks; safe to walk
// for postmortems without holding a server. It is safe for concurrent use: the
// simulation bridge appends from both the tick loop and the telemetry poller.
type Log struct {
	mu      sync.Mutex
	Entries []Event
	seq     uint64
}

func NewLog() *Log {
	return &Log{Entries: make([]Event, 0, 4096)}
}

// Append records an event. Appends are cheap; cap growth to bound memory.
func (l *Log) Append(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	e.Seq = l.seq
	l.Entries = append(l.Entries, e)
}

// Len returns the number of recorded events.
func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.Entries)
}

// Trim drops all but the most recent n entries.
func (l *Log) Trim(n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.Entries) > n {
		l.Entries = append([]Event(nil), l.Entries[len(l.Entries)-n:]...)
	}
}

// Tail returns the last n events, or all if fewer exist.
func (l *Log) Tail(n int) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n >= len(l.Entries) {
		return l.Entries
	}
	return l.Entries[len(l.Entries)-n:]
}

// Seq returns the highest sequence number assigned so far.
func (l *Log) Seq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// After returns a copy of every event with Seq greater than the given
// sequence. It is the broadcast cursor primitive: the driver keeps the last
// Seq it broadcast and asks for everything appended since, regardless of how
// much Trim has since culled from the front of the log.
func (l *Log) After(seq uint64) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	idx := sort.Search(len(l.Entries), func(i int) bool { return l.Entries[i].Seq > seq })
	out := make([]Event, 0, len(l.Entries)-idx)
	for _, e := range l.Entries[idx:] {
		out = append(out, e)
	}
	return out
}
