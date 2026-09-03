# Known Gaps and Future Changes

Not a full roadmap — just things that surfaced during implementation and need
remembering. Each item includes enough context to revisit without re-reading the
full source.

## Incident 1 balance (the real dev target)

The `NewRuntime()` DB tuning in `runtime.go` is currently a dev-fast pass
designed to make the stampede visibly break within ~35-45s of a 5-min ramp.
The real Incident 1 balance assumes:

- **Ramp:** 5 minutes (200 → 10,000 req/s linear)
- **Hold:** 8 minutes after ramp peak
- **Service ceilings well above the 200/s calm baseline** so strain only
  builds near the peak — the operator must respond during the final minutes,
  not at t=20s
- Original slower-onset values kept in comments inside `runtime.go`:
  orders.NewDB(6,400,4), analytics.NewDB(10,120,3), payments.NewDB(14,80,3)
- When the dev-fast values are ready to be replaced, also revisit `maxFail`
  in `db.go` (currently 1% cap) — may need a higher ceiling for the real
  Incident 1 to make failures visible enough during the endure window.

## StampedeProfile decay after hold

`StampedeProfile.RateAt()` returns `PeakRate` forever after `Ramp+Hold`
(plateau, no decay). This is fine today — the scenario ends when the survive
window completes and the generator shuts off — but if a later incident or a
"recovery" phase expects traffic to taper after the peak, this is the spot
that needs a decay curve. Add an `After` branch in `RateAt` that drops back
towards `BaseRate` over a configurable tail, or replace the plateau with a
configurable `Decay` duration.

## System Health floor vs depth penalty

The health erosion formula in `metrics.go` now caps the depth penalty at 10
points, beginning at a depth threshold of 2,000. This means System Health can
hover in the 50-100 band during the "endangered but not yet failed" window.
When Incident 1 tuning is finalised, the penalty cap and threshold may need
a pass to match the intended severity curve (e.g. deeper queues should push
health below 50 faster, once the final ceilings are confirmed).

## p50 / p99 windowing caveat

The latency sampler is now time-windowed (last 10s), which resolves the
"p50 stuck at calm because the ring is full of early fast samples" bug. Two
remaining caveats:

1. Under very low throughput (<10 msgs/s) the 10s window may hold only a
   handful of samples, making p99 unstable. Consider falling back to a wider
   window (or full-history) when the active sample count is small.
2. The percentile is recomputed from a sorted copy of all active samples.
   At very high completion rates (thousands/s) this is a sort over several
   thousand entries each time `deriveMetrics` is called. Fine in practice;
   worth noting if profiling ever shows hot spots.

## MessageId parsing cost

`idFromBody` does a `json.Unmarshal` per publish (body → `{id string}`). At
10k msgs/s this is a handful of microseconds per message — negligible
compared to the broker round-trip and confirm wait. If it ever matters, the
body could be constructed with the ID already available in the caller, and
the ID threaded into `PublishBatch` as an explicit parameter instead of
parsed.

## Run terminal state — not frozen yet

After an outcome (fail or success), `generatorOn` goes false and the tick
loop keeps running: depth drains as workers finish, the management API keeps
being polled, and the HTTP server stays live. The console summary is logged
once. The next step is to store the final `Outcome` on `GameState` (or a
separate postmortem state) so the WebSocket can broadcast it to the frontend
when Phase 4 lands.
