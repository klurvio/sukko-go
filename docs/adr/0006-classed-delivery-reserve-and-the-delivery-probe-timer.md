# ADR-0006: The classed delivery reserve is a lock+counter admission gate, and back-pressure adds a `delivery_probe` timer

**Status**: Accepted
**Date**: 2026-08-22
**Ticket**: feat/go-sdk (Phase 3 — delivery channel)

## Context

`Messages() <-chan Event` hands the caller the raw bounded channel. The founding delivery requirement demands that channel's `AdvisoryHeadroom = 16` capacity be reserved *by class*: 1 slot usable only by `*Terminal`, 15 usable only by a named safety subset (`*CloseError`, `*StateChange`, `*RecoveryInterruptedError`, `*HistoryComplete`, `*ReplayComplete`, `*PossibleGap`, `*Gap`), so an unbounded caller-driven producer (`*PublishAccepted`, a publish-rate event) can never starve a bounded safety producer, and the supervisor's final `*Terminal` is *structurally* deliverable even into a full channel with a hung consumer.

Two Go facts collide with a naive reading. First, a raw `<-chan` gives the SDK no receive hook, so it cannot maintain a true occupancy counter — only `len(ch)`. Second, a plain buffered send takes *any* free slot regardless of the sender's class, so it cannot honour a reserve on its own. And Go offers no primitive to wait for "buffered channel below N": a send parks only at `len == cap`, and a receive is observable only by a completed send. A strict "block at the class ceiling, wake precisely on drain" is therefore not expressible with channel primitives alone.

A pump/ring design (an internal buffer drained by an SDK goroutine that mediates receives, giving the hook) is ruled out by the founding teardown requirement: "Close does not drain the buffer; a consumer that resumes ranging drains it, then observes `*Terminal`, then channel-close." That only holds if the buffer *is* the channel's own buffer, surviving with zero live SDK goroutines — a pump that must outlive `Close` to drain a ring contradicts "every sender has exited" and the `goleak` discipline.

## Decision

**Admission is a lock plus a `reserved` counter; the channel send happens outside the lock.** To send event `ev`:

1. Take the delivery mutex. Compute `ceiling := ceilingFor(class(ev))` — `QueueSize − AdvisoryHeadroom` (496) for data and ordinary advisories, `QueueSize − 1` (511) for the safety subset. If `len(ch) + reserved ≥ ceiling`, this send is over-ceiling (see the wait); otherwise `reserved++`, release the lock, `ch <- ev` (**provably non-blocking** — every reserver had headroom at reservation and the worst-case cross-class total is ≤ 511 < `cap`), then take the lock, `reserved--`, release.

The channel send is never performed under the mutex (§VII). Soundness rests on monotonicity: while the lock is held, `len(ch)` can only *shrink* (a consumer receive) and never grow (all senders serialise through this gate), so a check-then-reserve cannot over-admit. The ceilings are **regions reserved from data, not quotas on the safety subset** — a safety event at `len = 200` uses the ordinary region freely; the reservation only stops *data* from consuming the top 16.

`*Terminal` is not admitted through this gate. The supervisor sends it with `select { case ch <- t: default: }` after every other sender has exited; with all senders quiescent the ceilings guarantee `len(ch) ≤ 511`, so the `default` branch is structurally unreachable. That is a test, not a hope.

**The parked-sender wake is a clock-driven re-probe.** An over-ceiling sender parks on `select` over three cases — a `Clock` timer armed under the purpose name `"delivery_probe"`, the root context, and the epoch context — and on each tick re-checks the ceiling under the lock. The two contexts carry the two distinct discard meanings (root vs epoch), and the send returns *which* one discarded a parked event so the supervisor can tell them apart. Production cost is confined to back-pressure episodes, where a few milliseconds of resume latency is invisible behind a 496-event backlog; tests drive it deterministically via `Advance`. This reuses the existing back-pressure accounting machinery exactly: `blockedOnSend` is raised at park and lowered at completion (the same transition that increments `Stats().BackpressureBlocks`), and `Stats().Blocked` accumulates off `Clock.Now()`.

**Timing-path list amendment.** The closed clock-injectable timing-path list gains `"delivery_probe"`, and the earlier determinism note that "a blocked channel send involves no timer at all" is **superseded** — with the re-probe, a blocked send does involve a timer, which is what makes the back-pressure resume deterministic under the fake clock rather than dependent on a real receive. The list is closed precisely so an unlisted path is a real sleep waiting to happen; this adds the path rather than smuggling it.

## Consequences

- The `*Terminal`-on-every-terminal-path guarantee (SC-014) becomes provable: a unit test fills the channel to a safety ceiling with all senders quiescent and asserts the non-blocking `*Terminal` send lands.
- The admission-matrix tests are deterministic under the fake clock: with the probe timer unadvanced, an over-ceiling send stays parked, so "data blocks / safety succeeds / unbounded-advisory blocks" at reserve-boundary occupancy is a stable assertion rather than a race.
- Resume latency under sustained back-pressure is bounded by the probe interval, not by instantaneous drain detection. This is a real, accepted cost, paid only while a consumer is already behind.
- `class(ev)` is a pure function of the event's Go type — admission by construction. It must stay in sync with the reserve membership above; a completeness test pins the safety subset against that list.

## Alternatives rejected

- **Pump/ring with an SDK-mediated receive hook** — rejected: it contradicts the "Close does not drain the buffer" teardown rule and the zero-live-goroutine teardown, and reintroduces the double-buffering the single-channel model exists to avoid.
- **Poll `len(ch)` on a real sleep** — rejected: §VII/§VIII forbid wall-clock sleeps in the SDK; a fake clock cannot help a bare sleep, and the timing path would be invisible to the source guard.
- **Unclassed reserve** — rejected in an earlier design round: 16 `*PublishAccepted` from a stopped consumer exhausted the whole headroom, and the `*RecoveryInterruptedError` the reserve exists to protect had nowhere to land. Classing closes it: an unbounded producer can never consume a bounded producer's slot.
- **Two channels (data + advisory)** — rejected: the single-channel requirement's whole point is one channel in receive order, so a second channel's overflow policy cannot silently drop the signals that exist to prevent silent drops.
