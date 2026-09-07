# Known Gaps and Future Changes

Not a full roadmap — just things that surfaced during implementation and need
remembering. Each item includes enough context to revisit without re-reading the
full source.

## Incident 1 balance (PLAYTEST PASS 1)

The committed defaults in `runtime.go` target the real incident:

- **Ramp:** 5 minutes (200 → 10,000 req/s linear); `BLACKOUT_RAMP` overrides
  the ramp portion only for dev loops.
- **Survive:** the window is a fixed 8:00 from t=0 — **Hold = Survive − Ramp**
  — so a compressed dev ramp still exercises the full survive objective.
- **Ceilings sit far above the 200/s calm baseline** so strain only builds near
  the peak; scaling to 20 workers can still cope at peak. Playing nothing by
  t≈6-7 minutes should lose.

These are **PLAYTEST PASS 1**: verify by playing the run a dozen times and tune
the `NewDB` slopes to match the intended severity curve. Notes:

- `BLACKOUT_FAST_DB=1` reproduces the old dev-fast trio (knee ~35-45s) for
  quick tape/UI iteration — intentionally breaks far too early for real play.
- `maxFail` in `db.go` is still 1% cap. The customer-success floor (≤70% fail)
  is defensive today; if playtest shows success can't approach 70% during the
  endure window, that's the knob to raise.
- The original slower-onset comment values are gone; if the final balance needs
  a different shape, the current defaults are the tuning baseline.

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

## Run terminal state — now frozen

Since Phase 5, the driver stores the final `Outcome` + `Timeline` on
`GameState` and broadcasts them in the terminal snapshot, then returns from
`runSimulation` — the tick loop stops, the console summary logs once, and the
HTTP/WS server stays alive for inspection. Remaining: `Outcome.FinalTicks` and
the streak fields on `SurviveResult` are recorded but only `FailReason` /
`EndedAtMs` / final metrics / timeline are serialised today; surface the per-
objective streaks in the postmortem UI later if the beats feel thin.
