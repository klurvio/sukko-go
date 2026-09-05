# ADR-0001: Deliver events on a single bounded in-band channel, not callbacks

**Status**: Accepted
**Date**: 2026-08-21
**Ticket**: feat/go-sdk

## Context

Go's goroutine model removes the sync/async split, so there is no `SyncSukkoClient` analog of the Python SDK — there is one client. A live network feed needs a producing goroutine regardless of the surface offered. Surveyed mature Go real-time clients converge on a bounded channel as the delivery spine (Polygon `Output()`, Alpaca internally); centrifuge-go's callback handlers run synchronously on the read loop and its docs warn they deadlock on re-entrant client calls. The SDK's core stance is "never silently drop." The open question was the caller-visible delivery surface and its back-pressure behaviour.

## Decision

Delivery is a single bounded channel `Messages() <-chan Event` carrying a sealed `Event` union (`*Message` dominant; advisory/lifecycle events in-band, in receive order). `Iter(ctx) iter.Seq[Event]` wraps the same channel as sugar — the reason Go 1.23 is the module floor. There is no callback API and no second `Events()` channel. Back-pressure is inherent: a full channel parks a ctx-aware send in the decode goroutine, closing the TCP window and engaging the platform's own slow-client path (3 failed deliveries → 1008 → reconnect+recover); there is no pause/resume capability flag. Control-plane frames (acks, pong, auth, recovery terminators) are handled synchronously in the decode goroutine and never consume channel capacity — only `*Message` plus advisory events cross the channel. `AdvisoryHeadroom = 16` slots are reserved and classed (1 `*Terminal`-only + 15 for a bounded safety subset); the constructor enforces `QueueSize ≥ HistoryLimit + MaxReplayMessages + AdvisoryHeadroom` (floor 216; `DefaultQueueSize = 512`, matching the server's `WS_CLIENT_SEND_BUFFER_SIZE`). No event is dropped while an epoch is live; at epoch teardown and at `Close`, at most one in-flight event *per teardown* may be discarded.

## Consequences

- **Easier**: one caller code path; ordering preserved (a `PossibleGap` lands exactly where the potential loss sits relative to data); drops are structurally impossible, so there is no drops counter (`BackpressureBlocks` is the observable stand-in); the channel *is* the goroutine boundary the read-pump requires anyway.
- **Harder**: consumers must type-switch over the sealed union; the union's unexported marker method means callers cannot forge event types (case-set stays known) but also cannot exhaustively switch at compile time — `*UnknownEvent` absorbs additive server releases. A blocked consumer stalls only *data*: the decode goroutine's pong deadline is suspended under `BackpressureBlocked`, so the server's 1008 remains the intended terminal outcome; a persistently slow consumer is bounded by `MaxBackpressureReconnects = 5` → `ErrConsumerTooSlow` (an honestly-documented arbitrary backstop, consecutive-with-reset).
- **Coupling**: the delivery model drives the whole goroutine topology (ADR-0002) and is frozen in the public surface from `v0.1.0`.

## Alternatives rejected

- **Callback handlers (centrifuge-go)** — synchronous handlers block the read loop and deadlock on re-entrant calls; a second delivery mode for zero added capability.
- **A second `Events()` channel** — no sound overflow policy: blocking wedges the pump on an undrained event channel, dropping silently discards the exact signals that exist to prevent silent drops, unbounded reintroduces unbounded buffering; a split also breaks single receive-order.
- **Drop-on-overflow + counter (Alpaca `bufferFillCallback`)** — violates the no-silent-drop contract; blocking into the server's *recoverable* slow-client path is strictly better.
- **`iter.Seq` as the primary surface** — a pull-model single-consumer construct; concurrent access forces a producing goroutine anyway, collapsing the abstraction. Kept only as sugar.
