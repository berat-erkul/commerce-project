# ADR-001: Hybrid Saga via dedicated saga-service

- **Status:** Accepted
- **Date:** 2026-05-09
- **Deciders:** Berat (sole maintainer, learning project)

## Context

commerce-lab needs distributed transaction coordination across `order-service`, `payment-service`, and `inventory-service`. The classic order placement flow is multi-step (payment → stock reservation → order completion) and requires compensation when any step fails (e.g., refund the captured payment if stock reservation fails).

Saga is the canonical pattern for this. Two flavors exist:

- **Choreography:** Each service reacts to events from others. No central coordinator. Loose coupling, but the end-to-end flow is invisible — no single place tells you "where is order #123 in its lifecycle?"
- **Orchestration:** A coordinator service drives the flow by issuing commands and reacting to responses. Centralizes complexity but makes the flow legible and debuggable.

Beyond the order placement flow, the system also has post-completion side effects (notifications, analytics, search index updates, loyalty points). These are pure fan-out with no compensation requirement.

A naive choice would be "pick one pattern for the whole project." That misses the real shape of enterprise systems, where both patterns coexist and the choice is made per flow.

## Decision

Adopt a **hybrid saga model** in commerce-lab, with the pattern selected per flow:

| Flow | Pattern | Implementation |
|---|---|---|
| Order placement (payment + stock + completion) | **Orchestration** | Dedicated `saga-service` owns the state machine and emits commands |
| Post-completion fan-out (notifications, analytics, etc.) | **Choreography** | Listeners subscribe to `order.events` independently |

**Orchestration is implemented in a dedicated `saga-service`, not embedded in `order-service`.** This is a deliberate separation:

- `order-service` stays focused on order CRUD and the REST entry point
- `saga-service` becomes a reusable home for future orchestrated flows (refunds, returns, cancellations) without bloating any business service
- The structure mirrors real enterprise practice (Camunda, Temporal, AWS Step Functions are external orchestrators)
- Educationally clearer: "the orchestrator is its own thing" is visible in the topology, not buried in business code

## Topology

```
[REST] POST /orders ──> order-service ──(OrderCreated)──> order.events
                                                              │
                                            ┌─────────────────┼─────────────────┐
                                            ▼                 ▼                 ▼
                                      saga-service    notification-svc    analytics-svc
                                      (orchestrator)   (choreography)     (choreography)
                                            │
                              ProcessPayment cmd  ──> payment.commands
                                            │
                              payment-service ──(PaymentCompleted)──> payment.events
                                            │
                              ReserveStock cmd ──> inventory.commands
                                            │
                              inventory-service ──(StockReserved)──> inventory.events
                                            │
                              saga-service ──(OrderCompleted)──> order.events ──> fan-out
```

## Topic conventions

- `<domain>.commands` — single-consumer (the owning service); imperative payloads
- `<domain>.events` — broadcast; past-tense facts

Planned topics (Week 2):
- `order.events` — `OrderCreated`, `OrderCompleted`, `OrderCancelled`
- `saga.commands` — `StartOrderSaga`
- `payment.commands` — `ProcessPayment`, `RefundPayment`
- `payment.events` — `PaymentCompleted`, `PaymentFailed`, `PaymentRefunded`
- `inventory.commands` — `ReserveStock`, `ReleaseStock`
- `inventory.events` — `StockReserved`, `StockReservationFailed`, `StockReleased`

## Consequences

**Positive:**
- Pattern is matched to flow type rather than forced uniformly
- saga-service is reusable for future orchestrated flows
- Choreography fan-out keeps post-completion concerns decoupled — adding a new listener (e.g., recommendations) requires zero changes to existing services
- Saga state is centralized (`saga_instances` table in saga-service), making "where is this order?" answerable from one place

**Negative / accepted costs:**
- One more service to deploy and operate (saga-service)
- saga-service is itself subject to the dual-write problem (must update `saga_instances` AND emit a command atomically), so it requires its own outbox table — three services × outbox in Week 2. This duplication is intentional; consolidation into a shared `libs/outbox-starter` is deferred to Week 4 (DDD/refactor week). Living with the duplication first is a deliberate learning choice.
- saga-service is a candidate for becoming a "god service" if business logic leaks in. Discipline: it only owns coordination state and command/event translation, never domain rules.

**Anti-pattern explicitly rejected:** Using both choreography AND orchestration for the *same* flow. Hybrid means different patterns for different flows, never both for one flow.

## Alternatives considered

1. **Pure choreography** — rejected: order placement requires compensation logic that becomes opaque without a central coordinator; debugging multi-step failures across 3 services is significantly harder.
2. **Orchestration embedded in `order-service`** — rejected: couples business CRUD to coordination logic; future orchestrated flows would force the same coupling on other services.
3. **External orchestrator (Camunda / Temporal)** — out of scope for this learning project; the goal is to internalize the pattern by building it, not to use a prepackaged one. Will be revisited in a later phase if the project grows.

## References

- Chris Richardson — `microservices.io/patterns/data/saga.html`
- Project plan: `weeks/WEEK-02-SAGA-OUTBOX.md` (Cumartesi/Pazar bölümleri)
