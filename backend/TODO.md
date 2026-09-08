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

## Incident 1 balance (PLAYTEST PASS 2 — Feb 2026 live broker)

The first live-broker playtest FAILED at 22s — far too early. Root cause was
NOT the DB ceilings (`variables.md`-style paper math) but the real broker
path being ~10× slower than the DB sim model:

- **Prefetch was `= workers`** (one shared channel per queue), so the whole
  pool was bound by the ack round-trip, not by worker DB work → the channel
  effectively ran single-threaded. Fixed in `consumer.go`: `Qos(500, 0, false)`
  pipelines the channel and drain scales with worker count again. The old
  comment "fair dispatch: one message per worker" was true in RabbitMQ's
  older models, not with go-quorum/pipelined acks.
- **Messages were `DeliveryMode: Persistent`**; every batch forced an fsync on
  the broker, pushing per-message cost way up. Now TRANSIENT by default.
- Default workers were 3/2/2. Now **6/6/4** (`defaultWorkerCounts()` in
  `run.go`) — comfortable at 200/s baseline, strain near the peak.
- Failures now **Nack(false,false)** (drop, counted once) instead of
  requeue-looping forever; the constant requeue spam is what buried the
  `=== SCENARIO FAILED ===` line despite it always being printed.

The 22s failure was orders-running-at-load≥1.0 → sync-gateway p99 over 0.5s →
gateway load pinned → health drained 10/hold-per-second → health 48 →
`system-health-floor` tripped. With prefetch 500 + transient + 6/6/4 this
should shift the strain onto the final minutes; re-verify with a live run.

## Observability gaps (from PLAYTEST PASS 2)

- **Health floor crossings were invisible until the final tape scan.** The
  console now prints one-shot `WARN: system health dropped below 70%/50%`
  lines the tick they cross, and `logOutcome` opens with a prominent
  `RUN ENDED: FAILURE — <reason>` banner instead of relying on the mid-stream
  `=== SCENARIO FAILED ===` line.
- **Per-message failure logs: still ONE line per dropped message when
  `BLACKOUT_DEBUG=0`?** No — `consumer.go` now rolls failures into a
  rate-limited `N failures in the last 1s (total M)` summary, always printed.
  Details stay behind `BLACKOUT_DEBUG=1`. Do not reuse this roll-up to mute a
  later incident's genuine problem (e.g. Incident 3 poison-message storms need
  their own signal).

## mgmt `/api/queues` message_stats retention — rail rates

The `/api/queues` `message_stats` counters (publish/ack totals) reset on
RabbitMQ's OWN retention schedule, so diffing them against a fixed 1s poll
produced rail rates off by the retention period. Rates now come from
`Runtime.SnapshotRates` — deltas over the runtime's own cumulative counters
(published vs acked+failed), exact by construction. `applyCache` only reads
depth + consumers from management now; the rate code in `queueCache.update`
was removed. If the retention behaviour is ever changed upstream, having both
sources is fine but the rail should keep using the tracker source (single,
trusted window).

## Transient (non-durable) publishes — Incident 2 trade-off

Since PLAYTEST PASS 2 every message is published TRANSIENT (no more fsync per
batch). This is a deliberate trade-off: a broker/backend restart mid-run loses
in-flight messages instead of surviving on the durable queues. That is fine
for a playable incident (the sim's event log is the durable record), but
Incident 2 (worker crash / recovery) will likely need more of the broker
fidelity the old persistent mode gave. When that incident lands, revisit
`publisher.go`: either an explicit "durability" knob per incident, or a
hybrid (persistent for payment auths, transient for order/analytics).

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

## Run lifecycle / play-from-UI (main.go → run.go)

Incident 1 is now playable without restarting the backend: `RunController`
manages the run lifecycle. Known follow-ups:

- **RUNNING guard is by design, not a heal:** `begin()` rejects a second
  `start` while a run is running. If a player clicks Play again mid-run the
  control returns an error; the frontend only offers retry when
  `runStatus === "ended"`, so this is inert in practice. Keep the guard—it
  prevents stomp accidentally, and a "stop run" control belongs to a later
  incident anyway.
- **5s teardown timeout:** if the previous run's goroutine doesn't return
  within 5s of cancel, the controller proceeds anyway (logs a WARNING). A
  hung runSimulation would then fight the fresh run over the same runtime —
  worth a panic/abort path if it ever shows in logs.
- **PoolManager.Reset vs pm.Run race (theoretical):** `Stop()` deletes finished
  entries outside the lock (inherited from the pre-refactor shutdown path), and
  `pm.Run`'s reconcile loop shares the same map. A registry change signalling
  between `Stop` and `reconcile` inside `Reset` could start a pool at last
  scaled counts; `Reset`'s reconcile would then see it present and leave it.
  In practice reg changes during `begin()` are only `ResetWired` (called after),
  so it's inert today — if a pause ever lands mid-`begin()`, move Reset's
  stop+recount+reconcile under one `m.mu` critical section.
- **runId / runStatus broadcast:** every snapshot is tagged with the current
  run UUID (`runId`, incremented per start/retry). The client wipes its merged
  event tape on any `runId` change; the server's `prevSnap` is per-run (nil at
  start) so the first frame of a new run is always full.
- **Stale-process hazard (operational):** a previously-started backend left
  running will keep consuming the queues and "eat" any integration test's
  published messages (observed: `TestInt_*` failing with 0 processed while the
  old playtest binary sat on :8080). Stop the backend before running
  tests / starting a new playtest.
- **`BLACKOUT_AUTOSTART=1`** preserves the old boot-and-run behaviour for
  scripts; default is idle-on-boot with the control channel driving start.
