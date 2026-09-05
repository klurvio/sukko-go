# ADR-0002: Own concurrency with a supervisor/per-epoch topology, verified by the race detector AND a deterministic clock

**Status**: Accepted
**Date**: 2026-08-21
**Ticket**: feat/go-sdk

## Context

The SDK maintains connection lifecycle, subscription tracking, auth refresh, and pos-recovery concurrently, and must guarantee no goroutine leaks and no data races while every timing path (backoff/jitter, heartbeat, pong, refresh lead+floor, replay floor, recovery deadline, ack timeout, dial, SSE idle, …) behaves deterministically under test. sukko-py mandated deterministic time *as the substitute* for a race detector Python lacks; Go has the real detector. Constitution §VI (concurrency safety) and §VII (its enforcement) govern.

## Decision

One supervisor goroutine owns the connect→run→backoff→reconnect loop. Each connection epoch derives a child context and launches its read/decode + heartbeat goroutines under a per-epoch `errgroup`; epoch teardown (cancel + `Wait`) completes before re-dial — no goroutine outlives its epoch. Stateful flows are single-owner goroutines communicating via channels (auth owner, recovery owner, subscribe serializer), not shared locks — `atomic` for counters, `sync.RWMutex` only for simple read-heavy caches. Every goroutine's first `defer` is the SDK's recover helper (slog + in-band `*InternalError` + epoch failure). `Close(ctx)` is strictly ordered: cancel root → ctx-bounded wait (**no drain**) → close transport → non-blocking-send the final `*Terminal` into its reserved slot → close `Messages()` exactly once from the supervisor after all senders exit. `Client` is single-use. Verification is **both**: every test runs `go test -race` (a run without `-race` is not passing) across the 1.23–1.26 matrix, **and** an injectable `Clock`/`Rand` seam drives all named timing paths — `Clock.Now()` is the only wall-clock source (source-guarded), tests rendezvous via `BlockUntilTimer(name)` before a synchronous `Advance(d)`, and `goleak` asserts no leaks after every lifecycle test. No real `time.Sleep` in tests.

## Consequences

- **Easier**: "no goroutine leaks" and "no data races" become mechanically assertable; single-owner goroutines eliminate lock-ordering across the gap/replay/timer paths; the named-rendezvous clock makes timing-*logic* tests deterministic — a disjoint failure class from what `-race` catches, which is why both are mandatory.
- **Harder**: the `Clock` seam threads through every timing path and needs a hand-rolled fake (~150–250 LOC) because no mainstream fake-clock library exposes per-purpose-name timer registries; `NewClient` retains the client-lifetime ctx, a deliberate deviation from "don't store Contexts in a struct" (narrowed: a lifetime signal only, never substituted for a per-call ctx). The `Close` ordering and reserved-slot `*Terminal` are load-bearing — they were the source of the historical interaction bugs (a discarded `*Terminal`, a carry-over that reordered advisories).
- **Coupling**: the topology presupposes the single-channel delivery model (ADR-0001); the determinism seam stays behind the `Clock`/`Rand` interfaces so a test dep never reaches the public surface.

## Alternatives rejected

- **Shared-lock state (mutex over connection/subscription/auth)** — lock-ordering hazards across recovery paths; goroutine-ownership is the "share memory by communicating" stance.
- **`-race` OR deterministic time** — the two check disjoint failure classes (data races vs timing logic); a real-sleep jitter test is slow and flaky regardless of `-race`.
- **A global "exactly n timers" rendezvous** — couples every timing test to unrelated armed timers and hangs on overshoot; the named per-purpose rendezvous is required.
- **Draining `Messages()` on `Close`** — discards up to `QueueSize` buffered events; the ctx-aware send already unparks the decode goroutine, so the drain is redundant and harmful.
