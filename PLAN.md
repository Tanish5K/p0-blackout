# P0-BLACKOUT — Master Implementation Plan

A Discovery-Driven Distributed-Systems Survival Roguelike.

> **Core promise:** Something breaks. The player notices symptoms, investigates the evidence, changes the system, sees what happened, and adapts before the next failure arrives.
>
> **Most important acceptance criterion:** A player should learn by changing a live system and watching it respond—not by selecting a correct answer from a menu.

---

## 1. Overview & Objectives

**p0-Blackout** is a single-player systems-operations roguelike in which the player acts as the on-call engineer for a fictional platform called **Lumen**—a service that keeps a city's events, payments, identity, and communications online. The player inherits a running system, observes its behavior under stress, forms hypotheses, changes live configuration, and learns from the resulting consequences.

The game should feel like an emergency operations room rather than a course, quiz, or monitoring dashboard. The player's job is not to produce the most theoretically elegant RabbitMQ topology. Their job is to keep the customer-facing system alive while accepting tradeoffs: some work may be delayed, some analytics may be lost, some messages may be isolated, and some services may need to be sacrificed temporarily.

The intended learning outcome is emergent. After playing, the player should recognize patterns such as "this workload needs competing consumers," "this event must reach independent subscribers," or "this failed message is poisoning the queue," because they have personally watched those patterns unfold.

### 1.1 Game Loop (Repeatable per Incident)

| Phase | Player activity | Game response |
| --- | --- | --- |
| **Observe** | Watch the control room, alerts, request symptoms, queues, workers, and service health. | The system continues operating without waiting for the player. |
| **Investigate** | Open service cards, inspect queues, trace individual messages, compare timelines, and read logs. | The player gathers evidence but is never shown a "correct answer." |
| **Hypothesize** | Decide what appears to be failing and what intervention might change the behavior. | The game records the player's actions and begins a causal timeline. |
| **Experiment** | Scale workers, pause a subsystem, change request handling, alter retry behavior, or modify routing. | The live simulation changes immediately, including beneficial and harmful side effects. |
| **Stabilize** | Keep critical metrics above thresholds while managing new symptoms. | The incident evolves. A local fix may expose a downstream bottleneck. |
| **Review** | Read a postmortem showing the causal chain, decisions, tradeoffs, and score. | The game explains what happened without grading the player's vocabulary. |

The loop must remain active even while the player is reading an inspector panel. Pausing should be a deliberate player tool rather than the default behavior. A limited **freeze-frame** action may be available in later incidents, but it should consume a resource so investigation itself becomes a tradeoff.

---

## 2. Tech Stack & Architecture

### 2.1 Stack Decision (Confirmed)

| Layer | Technology | Notes |
| --- | --- | --- |
| **Backend** | Go 1.21+ | Simulation, event loop, RabbitMQ orchestration |
| **Message Queue** | RabbitMQ 3.12+ (Docker) | Real queues/exchanges/workers, not simulated |
| **MQ Client** | `github.com/rabbitmq/amqp091-go` | Official Go client, publisher confirms, manual ack |
| **WebSocket** | `github.com/gorilla/websocket` | Real-time state push + player actions |
| **Frontend** | React 18 + TypeScript + Vite | Control room UI, animated system map |
| **Communication** | WebSocket (10Hz state snapshots) | Server authoritative, client renders |

### 2.2 Architectural Principle

**Backend is the Game. Frontend is the View.**

- **No game logic in React** — it only renders state and sends actions.
- **No RabbitMQ access from frontend** — all via backend WebSocket API.
- **Deterministic replay** — backend event log lets the frontend replay any run.
- The UI consumes semantic events and state snapshots. It does not contain the rules for whether a worker overloads a database or whether a requeued message becomes a poison loop. This separation makes it possible to swap the deterministic (or real) engine later.

---

## 3. Project Structure

```
p0-blackout/
├── docker-compose.yml          # RabbitMQ + optional management UI (:15672)
├── PLAN.md                     # This document
├── backend/
│   ├── go.mod
│   ├── main.go                 # Entry: starts tick loop, WS server, RabbitMQ topology
│   ├── internal/
│   │   ├── simulation/
│   │   │   ├── state.go        # GameState (tick, services, queues, workers, objectives)
│   │   │   ├── tick.go         # 100ms tick loop: traffic, routing, worker processing, metrics
│   │   │   ├── traffic.go      # Traffic generator (Poisson/burst) with variance
│   │   │   └── metrics.go      # Health, latency p50/p99, queue depth, success rate
│   │   ├── rabbitmq/
│   │   │   ├── topology.go     # Exchanges, queues, bindings for Lumen services
│   │   │   ├── consumer.go     # Worker pool management (scale, pause, resume)
│   │   │   └── publisher.go    # Gateway → service publishing
│   │   ├── api/
│   │   │   ├── ws.go           # WebSocket hub: broadcast snapshots, handle actions
│   │   │   └── actions.go      # Player action types & handlers
│   │   ├── incident/
│   │   │   ├── stampede.go     # Incident 1 config
│   │   │   ├── falling.go      # Incident 2 config
│   │   │   ├── poison.go       # Incident 3 config
│   │   │   ├── broadcast.go    # Incident 4 config
│   │   │   ├── noise.go        # Incident 5 config
│   │   │   └── blackout.go     # Incident 6 config
│   │   └── campaign/
│   │       ├── state.go        # CampaignState (persists across incidents)
│   │       ├── cascade.go      # Cross-incident consequence calculation
│   │       └── unlocks.go      # Knowledge flags & meta-progression
│   └── pkg/
│       └── events/             # Event log (append-only, postmortem replay)
└── frontend/
    ├── package.json
    ├── vite.config.ts
    ├── index.html
    └── src/
        ├── main.tsx
        ├── App.tsx
        ├── components/
        │   ├── SystemMap.tsx        # Living schematic (SVG/Canvas)
        │   ├── StatusRail.tsx       # Top bar: time, health, latency, budget
        │   ├── Inspector.tsx        # Right rail: node details, controls
        │   ├── EventTape.tsx        # Bottom log with filters
        │   ├── MessageForensics.tsx # Message timeline modal
        │   └── RoutingEditor.tsx    # Progressive binding editor
        ├── hooks/
        │   ├── useGameState.ts      # WS connection, state sync
        │   └── useTick.ts           # Client interpolation for smooth animation
        ├── types/
        │   └── game.ts              # Shared types (matches Go JSON)
        └── styles/
            └── control-room.css     # Dark theme, orange/cyan/green palette
```

---

## 4. Data Flow & Communication

### 4.1 Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      BACKEND (Go)                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Traffic  │→ │ RabbitMQ │→ │ Workers  │→ │ Metrics/   │  │
│  │ Generator│  │ Topology │  │ (pools)  │  │ Objectives │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────┬─────┘  │
│         ↑                    ↑            ↑          │       │
│         └──── Player Actions (WS) ──────────────────┘       │
│                              │                               │
│                    ┌─────────┴─────────┐                     │
│                    │  Campaign State   │                     │
│                    │  (persists across │                     │
│                    │   incidents)      │                     │
│                    └─────────┬─────────┘                     │
│                              │                               │
│                    ┌─────────┴─────────┐                     │
│                    │   Event Log       │                     │
│                    │  (postmortem)     │                     │
│                    └───────────────────┘                     │
└──────────────────────────│──────────────────────────────────┘
                           │ WebSocket (10Hz snapshots)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    FRONTEND (React)                         │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │ System Map │  │ Inspector  │  │ Event Tape │            │
│  │ (animated) │  │ (controls) │  │ (log)      │            │
│  └────────────┘  └────────────┘  └────────────┘            │
│         ▲              ▲              ▲                      │
│         └──────────────┼──────────────┘                      │
│                    Player Actions                            │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 WebSocket Message Protocol

**Server → Client (State Snapshots, ~10Hz):**

```jsonc
{
  "type": "snapshot",
  "tick": 1234,
  "clock": "02:17:34",
  "incident": "stampede",
  "phase": "running",              // running | ending | failed | victory
  "services": [
    {
      "id": "orders",
      "name": "Orders",
      "health": 87,                // 0-100
      "load": 0.72,                // 0.0-1.0
      "latencyMs": { "p50": 120, "p99": 940 },
      "status": "healthy"          // healthy | degraded | stalled | failed
    }
  ],
  "queues": [
    {
      "id": "orders.work",
      "depth": 1432,
      "inFlight": 12,
      "rateIn": 1842,
      "rateOut": 1104,
      "unacked": 127,
      "oldestMs": 4800,
      "deadLetters": 19
    }
  ],
  "workers": [
    {
      "id": "orders-worker-3",
      "queue": "orders.work",
      "state": "busy",             // healthy | busy | stalled | failed | paused
      "processing": "order-4912",
      "ack": "manual"
    }
  ],
  "metrics": {
    "systemHealth": 62,
    "customerSuccess": 81,
    "successRate": 0.86,
    "latencyMs": { "p50": 150, "p99": 1100 },
    "budget": 4600,
    "reputation": 78,
    "technicalDebt": 22,
    "backlog": 1432,
    "deadLetters": 19,
    "trafficFps": 6400
  },
  "objectives": [
    { "id": "health", "label": "System Health", "current": 62, "target": 50, "met": true },
    { "id": "success", "label": "Customer Success", "current": 81, "target": 70, "met": true },
    { "id": "survive", "label": "Survive 8 min", "current": 342, "target": 480, "met": false }
  ],
  "alert": { "level": "warning", "message": "Database capacity at 92%" }
}
```

**Client → Server (Player Actions):**

```jsonc
{
  "type": "action",
  "action": "scale_workers",
  "payload": { "service": "orders", "delta": 2 },
  "timestamp": 178551234
}
```

**Action Registry (all supported):**

| `action` | `payload` | Notes |
|---|---|---|
| `scale_workers` | `{service, delta}` | Add/remove consumers, costs budget |
| `pause_service` | `{service}` | Toggle consumer(s) off |
| `resume_service` | `{service}` | Toggle consumer(s) on |
| `restart_worker` | `{workerId}` | Kill + respawn a worker |
| `set_ack_policy` | `{queue, policy}` | auto \| manual \| reject \| requeue |
| `set_retry_policy` | `{queue, maxAttempts, backoff}` | Retry/backoff config |
| `route_to_dlq` | `{queue}` | Enable failure-isolation queue |
| `set_exchange_type` | `{exchange, type}` | direct \| topic \| fanout |
| `add_binding` | `{exchange, routingKey, queue}` | Routing editor operation |
| `remove_binding` | `{exchange, routingKey, queue}` | Routing editor operation |
| `set_priority` | `{queue, priority}` | Promote/demote queue priority |
| `set_processing_mode` | `{service, mode}` | sync \| async |
| `use_freeze_frame` | `{}` | Pause sim for 10s (limited charges) |
| `toggle_analytics` | `{}` | Analytics subsystem master switch |

### 4.3 Deterministic Simulation Core

```go
type GameState struct {
    IncidentClock    int64           // ms since incident start
    Services         map[string]*Service
    Queues           map[string]*Queue
    Workers          map[string]*Worker
    RoutingBindings  []*Binding
    ResourcePools    map[string]*ResourcePool
    ActiveMessages   map[string]*MessageRecord
    EventLog         []Event
    PlayerActions    []PlayerAction
    Objectives       []Objective
    Campaign         *CampaignState
}
```

Simulation advances in small fixed ticks (100ms). Each tick: incoming traffic → request handling → message publication → routing → consumer work → failures → acknowledgements → queue movement → downstream load → objective metrics. Player actions modify configuration and are recorded with timestamps. The postmortem replays this event log.

---

## 5. The Fictional World — Lumen Platform

Lumen operates a city-scale event and commerce platform. Services are intentionally small and stylized so the architecture remains legible on screen.

| Service | Customer-facing role | Workload | Failure consequence |
| --- | --- | --- | --- |
| **Gateway** | Accepts incoming requests, returns responses | High-volume, latency-sensitive | Timeouts & cascading failures |
| **Identity** | Authenticates users, validates sessions | Small, critical, synchronous | Customers cannot access platform |
| **Orders** | Creates and updates transactions | Bursty work, DB writes | Lost or delayed orders |
| **Payments** | Authorizes money movement | Lower volume, high criticality | Failed checkout, financial risk |
| **Notifications** | Email, SMS, in-app updates | Expensive, delay-tolerant | Customers don't receive updates |
| **Analytics** | Consumes operational/behavioral events | Broad, high-volume, non-critical | Incomplete reporting |
| **Audit** | Records important state transitions | Lower volume, durable, important | Compliance & forensic gaps |

### 5.1 Initial Topology (Incident 1)

- Exchanges: `gateway.events` (topic), `order.events` (topic), `payment.events` (direct)
- Queues: `orders.work` (durable), `analytics.events` (durable), `payments.work` (durable)
- Bindings: `order.created` → `orders.work`, `order.created` → `analytics.events`, `payment.auth` → `payments.work`

### 5.2 Service Introduction & Topology Rollout

The fictional world has seven services, but not all of them have real RabbitMQ topology from the start. Introducing every service's exchanges/queues/consumers up front would (a) bloat the MVP, and (b) dump architecture on the player before the incident that makes it relevant — violating design rule 7 (introduce tools when their behavior becomes necessary).

**Guiding rule:** Every service exists as a *visible, inspectable system-map node with health/success metrics* from run one, but only gains *real RabbitMQ topology + consumers* in the incident where it becomes mechanically relevant. This keeps the fiction coherent (all seven services are present in the ops room) without building premature infrastructure.

**Stub-node behavior:** Until a service gains real topology it is **purely static** — reported `health 100`, `load 0`, `status idle` in every snapshot. It has no hidden failure states and cannot be acted on. It is decoration until its incident.

| Service | Visible node from | Real MQ topology + consumer added | Why that incident |
| --- | --- | --- | --- |
| Gateway, Orders, Payments, Analytics | Incident 1 (MVP) | Incident 1 | §5.1 — the core stampede path |
| Identity | Incident 1 | Incident 2 | Synchronous & critical; the "critical services" concept must exist. Its crash mechanic (Incident 2) needs it live. |
| Notifications | Incident 1 | Incident 3 | The Poison Pill is a notification job — pausing Notifications only carries tradeoff weight once it is real MQ. |
| Audit | Incident 1 | Incident 4 | Durable + compliance; belongs with the "everyone needs to know" broadcast incident. |

**Consequence for later incidents:** Incident 4's "outage broadcast must reach independent services" and Incident 5's "Analytics receives `identity.*` / `notification.*` noise" only become demonstrable once Identity/Notifications/Audit have live topology. Routing keys for these families are added to the topic tree in the same incident the service goes live.

**Backend/UI implication:** The game needs a *service registry*: each `Service` can be marked `visible-only` (stub) vs `fully-wired` (has MQ). The snapshot protocol (PLAN §4.2) already sends all services every tick, so stubs just report idle state. Because the `Topology` type is data-driven (§5.1 / Phase 1 `topology.go`), growing the architecture per incident is simply appending exchange/queue/binding entries to the incident's topology slice — no structural code change.

---

## 6. Core Roguelike Systems

### 6.1 Run Structure

```
Campaign Run = 6 Incidents (fixed order) + optional Chaos Mode
Each Incident = 5-10 minutes real-time
Run State persists across incidents within a campaign
Failure = branch to recovery scenario (see §6.4), not game over
```

### 6.2 Persistent Resources (Carry Across Incidents)

| Resource | Range | Source | Use |
| --- | --- | --- | --- |
| **Budget** | 0–10000 | Start 5000 + incident rewards | Scale workers, emergency tools, upgrades |
| **Reputation** | 0–100 | Customer success, data preservation | Unlocks advanced tools, vendor rates |
| **Technical Debt** | 0–100 | Quick fixes, ignored warnings | Increases failure rates, reduces max workers |
| **Knowledge** | Flags | Discoveries made | Permanent unlocks across runs |

### 6.3 Per-Incident State (Reset Each Incident)

| Metric | Purpose |
| --- | --- |
| **System Health** (0–100%) | Primary survival metric — 0% = incident failed |
| **Customer Success** (0–100%) | Request success rate × latency penalty |
| **Critical Service Uptime** | Payments/Identity/Orders/Audit must stay >70% |
| **Backlog** | Delayed work count — affects next incident |
| **Dead Letters** | Isolated failed messages — manual review cost |
| **Emergency Budget** | Incident-specific spending cap |

### 6.4 Failure Handling: Branch to Recovery Scenario

Failing an incident does **not** end the run. Instead, the player is offered a **Damage-Control Mini-Scenario** (2–3 minutes, reduced scope) that determines whether the campaign continues degraded or collapses. This makes failure a genuine strategic choice with stakes.

- The scenario presents the system in its post-failure state.
- The player performs limited damage control (e.g., restore Payments, clear a populated queue).
- Outcome determines next-incident starting resources.
- Knowledge flags from the failed incident are still granted.

### 6.5 Knowledge Flags (Permanent Unlocks Across Runs)

Earned by **discovering** mechanics through play, not by winning:

| Flag | Unlock |
| --- | --- |
| `discovered_work_queues` | Worker scaling UI |
| `discovered_manual_ack` | Ack policy controls |
| `discovered_dead_letter` | Failure-isolation (DLQ) queue |
| `discovered_broadcast` | Fanout routing editor |
| `discovered_topic_routing` | Topic binding editor, wildcards |
| `discovered_priority_queues` | Priority controls |
| `discovered_freeze_frame` | Freeze-frame charges |

**Key Rule:** Unlocks persist across **failed runs**. Failure teaches what the tool is for.

### 6.6 RNG / Variance (Medium)

Each incident uses **±20–30% variance** per run:
- Traffic curve jitter on peak and ramp timing
- Different poison message variants
- Different crash timing patterns
- Different dependency slowdown severity

This forces adaptation; players cannot fully memorize a single pattern.

**Chaos Mode seed system:** Fixed seed = reproducible crisis. Share seed + fingerprint for community challenges.

---

## 7. Incidents — Full Design

Each incident allows **multiple distinct strategies** with differing tradeoff profiles. No single "correct" topology. Consequences cascade across incidents (§8).

---

### Incident 1 — The Stampede

**Situation:** Traffic climbs from 200 to 10,000 req/s over 5 minutes. The existing order path performs expensive work synchronously.

**Pager:**

> **PAGER — 02:14 UTC** — "Lumen ticket demand is surging ahead of a citywide event. Customer reports mention slow checkouts. Keep core transactions available until the surge passes."

**Objectives (must meet ALL):**
- System Health > 50%
- Customer Success > 70%
- Survive 8:00

**New Tools Unlocked Here:** Queue inspection, worker scaling, subsystem pause, sync/async toggle.

**Strategies & Tradeoffs:**

| Strategy | Mechanism | Benefit | Consequences (this incident) | Cascade |
| --- | --- | --- | --- | --- |
| **Scale conservatively** | Add 2–4 Orders workers | Queue drains, latency stable | DB ~80% capacity; risky if surge extends | +Technical Debt (DB wear); DB fragile in Inc 2 |
| **Scale aggressively** | Add 8–12 workers fast | Queue clears fast | DB saturates → cascade; high cost | +High Technical Debt; DB failure +20% in Inc 2 |
| **Async processing** | Orders fire-and-forget | Gateway latency −90% | Backlog builds; eventual consistency | Backlog carries to Inc 2 as pending orders |
| **Pause Analytics** | Disable Analytics consumer | Frees ~30% DB capacity | Analytics data lost; Reputation −10 | Inc 4: analytics blind to outage patterns |
| **Hybrid (async + 2 workers)** | Balanced | Manageable backlog, stable DB | Moderate cost; some delay | Cleanest cascade; minimal penalties |
| **Sacrifice Notifications** | Pause Notifications | Frees worker slots + DB | No confirmations; Reputation −15 | Inc 3: notification backlog → poison risk |

**Discovery Rewards:** First queue backlog → `discovered_work_queues`. First async toggle → `discovered_async_processing`.

**Postmortem focus:** Where pressure moved, immediate vs eventual work, cost.

---

### Incident 2 — Falling Workers

**Situation:** Database latency spikes 10x. Workers crash mid-processing. Messages in limbo.

**Pager:**

> **PAGER — 03:47 UTC** — "Orders workers are dropping. Some jobs are being lost. Get order completion back above eighty percent before the marketplace opens."

**Objectives:**
- Order Completion > 80%
- Unacked Messages < 200
- Survive 6:00

**New Tools:** Manual ack, worker restart, unacked inspector.

**Strategies & Tradeoffs:**

| Strategy | Mechanism | Tradeoff | Cascade |
| --- | --- | --- | --- |
| **Switch to manual ack** | Ack only after DB commit | No lost messages | Duplicates if worker dies post-commit/pre-ack | +Duplicate rate in Inc 3 (poison harder to detect) |
| **Keep auto-ack, restart fast** | Accept losses, restart quickly | Simple | 15–30% messages lost | Lost orders = Reputation hit; Audit gaps in Inc 6 |
| **Pause Orders, drain first** | Stop new work, let in-flight finish | Zero loss/dup | Gateway queues fill → timeouts | Gateway backlog → Inc 3 starts poisoned |
| **Reduce worker count** | Fewer workers = fewer crashes | Stable unacked | Throughput drops; latency rises | Survives but Reputation tanks |
| **Emergency DB failover** (budget) | Spend 2000 for read replica | Instant fix | Budget drain; replica lag | Replica available rest of campaign |

**Critical Discovery:** Deliberately crash a worker holding a message to observe whether it returns (manual ack) or disappears (auto ack). Costs ~30s throughput but teaches the mechanic.

**Cascade in:** Aggressive Inc 1 scaling → DB failure 2x. Paused Analytics → no DB slowdown visibility. Built backlog → backlog messages at risk of loss.

---

### Incident 3 — The Poison Pill

**Situation:** One corrupted job crashes workers repeatedly. Retry storm forms.

**Pager:**

> **PAGER — 05:12 UTC** — "Something keeps killing the order pipeline. Healthy work is backing up behind it. Whatever it is, keep the line moving."

**Objectives:**
- Healthy throughput > 60% of baseline
- Worker pool > 2 alive
- Survive 5:00

**New Tools:** Retry limit (max attempts), DLQ routing, delayed/exponential backoff, subsystem pause.

**Strategies & Tradeoffs:**

| Strategy | Mechanism | Tradeoff | Cascade |
| --- | --- | --- | --- |
| **Max retries = 3 + DLQ** | Failed → isolation queue | Stops storm; work preserved | DLQ grows; manual review later | DLQ review cost in Inc 6 |
| **Pause Notifications entirely** | Stop poisoned queue consumer | Instant storm stop | All notifications lost; Rep −20 | Inc 4: Notification cold-start delay |
| **Exponential backoff** | Retry 1s,4s,16s,64s… | Time to manual fix | Message holds worker slot during backoff | Worker starvation |
| **Reject & don't requeue** | Drop poison immediately | Cleanest recovery | Message lost forever | Audit gap; compliance penalty in Inc 6 |
| **Route to special worker** | Dedicated quarantine consumer | Isolates blast radius | Costs slot + budget | Proves `discovered_failure_isolation` |

**Poison Variants (random per run):**
- `corrupted_payload` — always fails parsing
- `db_deadlock` — fails only under load
- `external_timeout` — fails calling 3rd party
- `memory_leak` — worker OOMs after 3 processes

**Discovery:** Comparing forensics across 3+ failures reveals pattern → `discovered_dead_letter`.

---

### Incident 4 — Everyone Needs to Know

**Situation:** A database outage requires Identity, Orders, Notifications, and Analytics to react independently.

**Pager:**

> **PAGER — 07:28 UTC** — "Core database is down. Every service needs to know now. If any of them miss the signal, we're blind during the outage."

**Objectives:**
- All 4 services receive outage event within 5s
- No service processes the event >1x
- System Health > 60%
- Survive 4:00

**New Tools:** Exchange type selector (direct/topic/fanout), progressive binding editor, independent subscriber queues.

**Strategies & Tradeoffs:**

| Strategy | Routing | Tradeoff | Cascade |
| --- | --- | --- | --- |
| **Fanout + 4 queues** | One event → 4 copies | Correct; each owns its queue | Slight message overhead | Proves `discovered_broadcast`; clean for Inc 5/6 |
| **Single work queue + 4 consumers** | Competing consumers | Only ONE gets the event | 3 services miss outage → cascades | Catastrophic for Inc 6 |
| **Direct + 4 bindings** | Same key to 4 queues | Works, rigid | Adding 5th service needs config change | TD: routing inflexibility |
| **Topic + wildcards** | `outage.*` → queues | Flexible | Over-engineered now; complexity cost | Unlocks `discovered_topic_routing` early |
| **Hybrid fanout/direct** | Critical fanout; Analytics direct | Optimized | Analytics delayed/partial | Analytics blind spots in Inc 5 |

**Failure mode:** If player attached multiple services to ONE work queue earlier, they see only one service react. Fix requires restructuring bindings.

---

### Incident 5 — Too Much Noise

**Situation:** Analytics receives ALL events (`order.*`, `payment.*`, `identity.*`, `notification.*`). Backlog 100k+. Shared resources strained.

**Pager:**

> **PAGER — 09:15 UTC** — "Analytics is drowning in everything. Its backlog is dragging down the whole platform. Feed it only what it actually needs."

**Objectives:**
- Analytics lag < 30s
- Orders/Payments latency < 500ms
- Survive 6:00

**New Tools:** Topic binding editor with wildcards, message filtering preview, routing-key inspector.

**Strategies & Tradeoffs:**

| Strategy | Binding | Tradeoff | Cascade |
| --- | --- | --- | --- |
| **Precise topic bindings** | `order.completed`, `payment.authorized` only | Signal without noise | Requires knowing exact keys | Proves `discovered_topic_routing` |
| **Pause Analytics** | Stop consumer | Instant relief | Zero analytics data; Rep −15 | Inc 6: flying blind |
| **Rate-limit Analytics** | Max 100 msg/s | Controlled flow | Data sampled; gaps | Statistical blind spots |
| **Separate Analytics cluster** (budget) | New RabbitMQ node | Full isolation | 3000 budget; ops complexity | Infrastructure for Inc 6 |
| **Broad `*.*` (lazy)** | Keep current | No effort | Analytics drowns; shared pool starved | Orders/Payments fail Inc 6 |

**Discovery Mechanic:** Binding editor shows **live preview** — "This binding would match 47% of last hour's traffic." Player experiments safely.

---

### Incident 6 — Blackout (The Gauntlet)

**Situation:** Traffic spike + slow DB + crashing worker + poison message + system-wide outage, all at once.

**Pager:**

> **PAGER — 11:59 UTC** — "Everything at once. Eight minutes. Keep the platform above half health, payments above eighty-five percent, and no critical service dark for more than half a minute. Budget is finite."

**Objectives (ALL for 8:00):**
- System Health > 50%
- Payment Success > 85%
- No critical service down > 30s
- Emergency Budget ≤ 5000

**Constraints:**
- Finite worker slots (max 20 total)
- Limited freeze-frames (3 uses; pauses sim 10s)
- Analytics/Notifications sacrifice penalties doubled
- Audit disable = immediate fail

**Strategy Archetypes:**

| Archetype | Approach | Profile |
| --- | --- | --- |
| **Fortress** | Max workers on Payments/Orders; pause Analytics+Notifications; manual ack + DLQ | High cost, high survival, data loss |
| **Surgeon** | Precise routing, targeted scaling, retry limits, broadcast for outage | Efficient, low cost, requires mastery |
| **Firefighter** | Reactive: freeze-frame to diagnose, emergency fixes, accept losses | Chaotic, adapts to RNG, high skill ceiling |
| **Minimalist** | Bare minimum workers, aggressive async, accept backlog | Low budget, risky, high TD |

**Scoring / Architecture Fingerprint (on success):**

```
SURVIVAL: 8:00/8:00
CUSTOMER: 87% success | p99: 1.2s
CRITICAL: Payments ✓ Orders ✓ Identity ✓ Audit ✓
EFFICIENCY: 12 workers | $2,300 spent
RECOVERY: 3 duplicates | 0 retry storms
DATA: Analytics 40% | Notifications 12k delayed
DEAD LETTERS: 23 (reviewed 5)
FINGERPRINT: SURGICAL / FRUGAL / HIGH SAFETY
```

---

## 8. Consequence Cascade System

### 8.1 Cross-Incident State Transfer

```go
type CampaignState struct {
    Budget        int
    Reputation    int
    TechnicalDebt int
    KnowledgeFlags map[string]bool

    DBFailureRateMultiplier   float64  // 1.0 base, up to 2.5
    WorkerCrashRateMultiplier float64
    BacklogCarryover          int      // Messages carried forward
    DeadLetterCarryover       int
    AnalyticsBlindness        bool     // Missed patterns in Inc 4/5
    NotificationBacklog       int      // Customer trust erosion
    AuditGaps                 int      // Compliance risk in Inc 6
}
```

### 8.2 Example Cascade Chains

```
Inc 1: Aggressive scaling → Inc 2: DB failure rate 2.0x → Inc 3: More crashes → Inc 6: Payments fail

Inc 1: Pause Analytics → Inc 4: No outage visibility → Inc 5: Can't filter Analytics → Inc 6: Flying blind

Inc 2: Auto-ack + losses → Inc 3: Can't distinguish poison from loss → Inc 6: Audit gaps = compliance fail

Inc 3: Pause Notifications → Inc 4: Notification cold → Inc 6: 30s notification downtime = customer fail
```

---

## 9. Scoring & Replayability

Score measures operational quality, not conformity to a prescribed topology.

| Category | Rewards |
| --- | --- |
| **Survival** | Remaining operational through the incident window |
| **Customer success** | Successful requests and acceptable latency |
| **Criticality** | Protecting Payments, Identity, Orders, Audit |
| **Efficiency** | Avoiding unnecessary workers and emergency spend |
| **Recovery quality** | Limiting duplicates, retry storms, uncontrolled backlog |
| **Data preservation** | Avoiding unnecessary analytics/notification loss |
| **Adaptability** | Recovering after an intervention creates a new bottleneck |

A run produces a compact strip:

```
FAST / EXPENSIVE / LOW RETRY SAFETY
Protected: Payments, Orders
Sacrificed: Analytics
Backlog: Moderate
Dead letters: 19
Emergency cost: High
```

**Meta-progression (across runs):** Completed incidents stay unlocked for replay; knowledge flags persist; best fingerprints saved; Chaos seeds unlocked after campaign clear.

---

## 10. Player Controls (Introduced Gradually)

| Control family | Examples | Benefit | Downside |
| --- | --- | --- | --- |
| **Workers** | Add/remove/restart/pause/resume | Throughput & recovery | Cost, downstream load, gaps |
| **Processing mode** | Inline vs hand-off | Async shortens request, absorbs bursts | Eventually consistent, delayed |
| **Acknowledgement** | Auto/manual ack, reject, requeue | Delivery safety | Loss, duplication, retry storm |
| **Retry policy** | Immediate/delayed/max attempts | Transient recovery | Capacity burn, hides poisons |
| **Failure isolation** | Route rejected work to inspection queue | Removes bad work from hot path | Dead letters to review; incomplete work |
| **Subsystem switches** | Pause Analytics/Notifications/Audit | Frees resources | Sacrifices functionality/data |
| **Routing** | Add/remove bindings; direct/topic/broadcast | Sends to right consumers | Misrouting starves or floods |
| **Priorities** | Promote payments/customer work | Protects important during scarcity | Lower priority starves |

**Never** present a modal asking "which exchange type should you use?" Instead, give a routing editor and watch who receives what.

---

## 11. Player Decision Rhythm

Every meaningful action answers three questions:

1. **What is the player trying to change?** e.g., reduce latency or stop repeated failures.
2. **What resource does the change consume?** budget, DB capacity, durability, immediacy, completeness.
3. **What new evidence will the player get?** a queue drains but DB saturation rises; a failed message reappears in another worker.

---

## 12. Evidence & Investigation Mechanics

### 12.1 Control-Room View (System Map)

Each node exposes only high-signal indicators: health state, current load, queue depth / in-flight, processing rate, error pulse, recent latency band. Queue containers fill with colored packets. Workers animate processing; failed workers flicker and stop; red messages visibly loop back on requeue.

### 12.2 Inspectors

Clicking a node opens a drawer with current state, recent events, controls, short history. Non-pedagogical observations:

```
ORDERS / WORKER POOL
Consumers: 3
Received: 1,842 / min
Completed: 1,104 / min
Unacked: 127
Failures: 8 / min
Database writes: 92% capacity
```

Clicking a queue reveals oldest messages, age distribution, delivery count, last destination. Clicking a worker reveals whether a message was acked before failure.

### 12.3 Message Forensics

A message record is the most concrete investigative object:

```
ORDER #4912

02:14:03  published to order.events
02:14:03  routed to orders.work
02:14:03  received by Worker 2
02:14:04  database write started
02:14:04  Worker 2 lost connection
02:14:04  delivery became available again
02:14:05  received by Worker 3
02:14:06  ACK
02:14:06  order completed
```

A poison message may show the same payload failing six times while others succeed. A fanout problem may show Orders receiving an outage event while Notifications never did.

---

## 13. Failure, Recovery, Postmortems

When the system collapses, the game freezes the final architecture and reconstructs the causal chain from the event log.

### Failure Screen (4 sections):
1. **Primary failure** — the resource/service that crossed its terminal threshold.
2. **Causal timeline** — sequence of incident events and player actions.
3. **Amplifiers** — choices that made it worse.
4. **Counterfactuals** — limited observations ("when eight workers were added, queue depth fell but DB saturation rose").

Never say "Correct answer: use a work queue." Say:

> "Your intervention removed the request backlog, but the database became the new bottleneck. The platform failed 37 seconds later."

On success, the postmortem shows architecture, survival duration, customer outcomes, delayed work, dead letters, peak resource usage, emergency cost, and services protected/sacrificed.

---

## 14. Chaos Mode (Post-Campaign)

### 14.1 Configurable Sliders

| Parameter | Range | Effect |
| --- | --- | --- |
| Traffic Peak | 5k–50k req/s | Surge intensity |
| Traffic Variance | 0–50% | Predictability |
| DB Latency Base | 10–500ms | Baseline pressure |
| DB Latency Spike | 1x–100x | Failure severity |
| Worker Crash Rate | 0–20%/min | Reliability |
| Poison Rate | 0–5/hr | Contamination |
| Dependency Availability | 50–100% | External failures |
| Event Volume | 1x–10x | Analytics load |

### 14.2 Survival Durations
- **Sprint**: 3 minutes (high intensity)
- **Standard**: 8 minutes (campaign length)
- **Endurance**: 20 minutes (marathon)

### 14.3 Seed System
- Fixed seed = reproducible crisis
- Share seed + fingerprint = community challenge
- Daily seed = leaderboard

---

## 15. Frontend Implementation Notes

### 15.1 UI Components per Mechanic

| Mechanic | UI Component | Backend Data Needed |
| --- | --- | --- |
| Worker Scaling | Slider + cost preview | Current/max workers, cost, DB load prediction |
| Async Toggle | Switch + consequence modal | Current mode, backlog estimate, latency delta |
| Service Pause | Toggle + warning | Health, dependents, reputation cost |
| Ack Policy | Radio group (auto/manual) | Current policy, unacked, duplicate rate |
| Retry Policy | Number input + backoff curve | Max retries, retry histogram |
| DLQ Routing | Button + queue inspector | Dead letter count, forensics access |
| Routing Editor | Visual graph (nodes=exchanges/queues, edges=bindings) | Topology, live preview |
| Topic Binding | Pattern input + match preview | Routing key samples, % match |
| Priority Queue | Drag-to-reorder list | Starvation risk indicator |
| Freeze Frame | Button + charge counter | Remaining charges, time frozen |
| Message Forensics | Timeline (expandable) | Full message event log |
| Event Tape | Filterable log, color-coded | Structured event stream |
| Postmortem | 4-panel causal view | Full event log + player actions |

### 15.2 Routing Editor — Progressive Disclosure
Start simple (fanout/direct), unlock topic editor in Incident 5. Never present a modal asking which exchange type. Let the player create bindings and watch who receives the messages.

### 15.3 Real-Time Data Flow
```
Backend (100ms tick) → State snapshot → WebSocket (10Hz) → Frontend
                                    ↓
                            Delta compression (changed nodes only)
                                    ↓
                            Client interpolation → 60fps animation
```

### 15.4 Visual Style
Dark, warm, tactile: charcoal panels, oxidized orange warnings, cyan operational signals, pale green healthy states, restrained off-white typeface. Late-night emergency control room, not SaaS analytics. Large typography for stakes; small monospace for evidence.

The system map always shows consequences spatially: adding workers grows the pool and glows the DB; pausing Analytics dims its branch and accumulates a neglected queue; changing a binding visibly re-routes message pulses.

---

## 16. Technical Decisions & Risks

### 16.1 Decisions

| Decision | Choice |
| --- | --- |
| RabbitMQ connection | Single connection, multiple channels (one per consumer pool) |
| Worker scaling | Dynamic consumer count per queue via Qos + new goroutines |
| Sync vs async | Sync: Gateway waits for Orders ack. Async: Gateway acks immediately, publishes to `orders.work` |
| DB simulation | Simulated in Go (latency distribution, capacity, failure injection) — no real DB |
| State serialization | JSON (simple, debuggable; Protobuf later if needed) |
| Forensics storage | Ring buffer in memory (last 10k messages) + persisted to event log |

### 16.2 Risks & Mitigations

| Risk | Mitigation |
| --- | --- |
| RabbitMQ latency/jitter vs 100ms tick | Run locally (Docker), async publisher confirms, batch metrics |
| WebSocket backpressure at 10Hz | Throttle broadcasts, delta updates after initial snapshot |
| Real RabbitMQ non-determinism | Seeded traffic generator, fixed worker timing, log all MQ ops for replay |
| Frontend animation smoothness | Client interpolation between snapshots (100ms → 60fps) |
| Real MQ adds yet-unseen behaviors | Build clean `rabbitmq` package interface; keep simulator swap option open |

---

## 17. Design Rules to Preserve

1. **Never ask a technical multiple-choice question when the system can demonstrate the answer.**
2. **Never make a powerful intervention free.** Every strong tool carries cost, risk, or tradeoff.
3. **Never hide the consequence of a player action.** Show it through the map, metrics, messages, event tape.
4. **Allow several stable architectures.** Score outcomes and tradeoffs, not one topology.
5. **Let failure teach.** A failed run reconstructs the causal chain in operational language.
6. **Keep the system alive while the player thinks.** Investigation is part of the pressure.
7. **Introduce tools when their behavior becomes necessary.** Unlocks feel earned.
8. **Keep RabbitMQ as machinery, not curriculum.** Technology emerges from needs.
9. **No correct-answer grading.** Postmortems explain what happened, not what you should have picked.

---

## 18. Implementation Phases

### Phase 0 — Repo & Infrastructure
- `docker-compose.yml` (RabbitMQ 3.12 + management UI on :15672)
- Go module init; add `amqp091`, `gorilla/websocket`
- Vite + React + TypeScript scaffold
- Minimal backend/frontend hello-world over WebSocket

### Phase 1 — RabbitMQ Topology & Basic Consumer
- Declare Lumen exchanges/queues/bindings (`topology.go`)
- Gateway publisher (`publisher.go`)
- Orders worker pool with manual ack (`consumer.go`)
- Publisher-confirms wired; reconnection logic

### Phase 2 — Simulation Core & 100ms Tick
- `GameState`, `tick.go` loop (traffic → handling → publish → route → consume → fail → ack → metrics)
- Traffic generator with Incident 1 curve + variance (`traffic.go`)
- Metrics & objective evaluation (`metrics.go`)
- Append-only event log (`pkg/events`)

### Phase 3 — WebSocket API & Player Actions
- WS hub: broadcast snapshots at 10Hz (`api/ws.go`)
- Action registry + handlers (`api/actions.go`)
- Scale / pause / async / ack / retry / dlq / routing / priority / freeze actions
- Actions logged to event log

### Phase 4 — Frontend Control Room
- WS client + state hook (`hooks/useGameState.ts`)
- System map with animated pulses (`SystemMap.tsx`)
- Status rail, inspector, event tape (`StatusRail/Inspector/EventTape`)
- Client interpolation for 60fps (`hooks/useTick.ts`)
- Control-room visual theme

### Phase 5 — Incident 1: The Stampede (Playable MVP)
- Traffic curve (200→10k over 5 min, variance), objectives, failure modes
- Worker scaling, async toggle, pause Analytics
- Success postmortem + failure causal timeline
- Deterministic demo mode for visual testing

### Phase 6 — Incident 2: Falling Workers
- Worker crashes, DB latency spikes, unacked tracking
- Manual ack, worker restart, deliberate-crash experiment

### Phase 7 — Incident 3: The Poison Pill
- Poison variants, retry limits, DLQ, backoff, subsystem pause
- Message forensics comparison

### Phase 8 — Incident 4: Everyone Needs to Know
- Broadcast routing, binding editor (fanout/direct), independent queues

### Phase 9 — Incident 5: Too Much Noise
- Topic bindings + wildcards, live match preview, rate limiting

### Phase 10 — Incident 6: Blackout + Scoring
- Combined scenario, freeze-frames, budget, fingerprint scoring

### Phase 11 — Campaign Layer & Cascades
- `CampaignState` persistence, reputation/debt/budget, cascade multipliers
- Recovery scenarios on failure
- Knowledge flags / meta-progression
- Postmortem causal-chain reconstruction

### Phase 12 — Chaos Mode
- Slider/toggle incident builder, seed system, leaderboard export

### Phase 13 — Polish
- Styling, animation refinement, sound, accessibility

---

## 19. MVP Scope (First Playable)

**Includes:** Incident 1 (The Stampede); four services (Gateway, Orders, Payments, Analytics); one order work queue with adjustable workers; sync vs async processing; service pause/resume; queue/worker inspector + message forensics; live traffic/latency/queue depth/health/success metrics; multiple viable strategies; failure causal timeline; success postmortem + score; deterministic demo mode.

**Excludes:** auth, multiplayer, accounts, real-money purchases; all six incidents in first build; tutorial that explains the answer; large dashboard/chart library.

**MVP success test:** A first-time player starts without reading a technical explanation, notices requests slowing, inspects, makes ≥1 intervention, observes a measurable side effect, and either stabilizes or fails with an understandable explanation. If the player can win only by clicking a highlighted hint, the MVP has failed its design goal.

---

## 20. Definition of Done for the Plan

BOYS! the plan is simple. Everything here is included in the plan above:
- what the player is trying to protect,
- what the player can change,
- what each change costs,
- what evidence the player can inspect,
- how the simulation evolves without input,
- how an incident succeeds or fails,
- how failure is explained,
- how the first incident can be implemented without building the entire campaign.

The most important acceptance criterion is experiential:

> **A player should learn by changing a live system and watching it respond—not by selecting a correct answer from a menu.**
