# ADR-0010: The auth-owner cancels root in its own panic recovery (single-terminator, refined)

**Status**: Accepted
**Date**: 2026-08-25
**Ticket**: feat/go-sdk

## Context

ADR-0009 stated, in passing, that "the owner never calls `terminalSequence` or
cancels root (single-terminator rule)." That was true through increment A: the
auth-owner only ever *recorded* a connected-refresh exhaustion terminal into the
current epoch's first-cause slot, and the supervisor's exit defer was the sole
terminator.

Increment B1 (the pre-dial ensure-token fetch, ADR-0009's increment B) made the
supervisor **synchronously depend on the owner being alive**: `ensureDialToken`
sends a request on an unbuffered channel to the owner and waits for the reply, on
the reconnect path guarded only by `rootCtx`. A concurrency review found the
resulting hazard: `recoverAuthOwner` swallowed any owner panic, logged it, and let
the goroutine die with no terminal routing and no restart. A subsequent owner
panic — from a frame send, a timer arm, or a delivery send that runs inline on the
owner goroutine — would leave a TokenSource client's every future dial parked
forever in `StateReconnecting`, invisible to `-race`, goleak, and the existing
tests. Pre-B1 the blast radius of owner death was "refresh silently stops"; B1
escalated it to "reconnect permanently wedged." ADR-0009's Consequences already
anticipated this: "owner-panic routing [becomes] due" once the refresh logic that
can actually panic lands.

Routing an owner panic to a terminal is constrained by the topology: the owner
holds no reference that can drive `terminalSequence`, and its only pre-existing
terminate mechanism — the epoch first-cause slot — is **nil between epochs**, which
is exactly when a B1 pre-dial fetch runs. So the epoch-slot mechanism cannot cover
a panic during the dial rendezvous, backoff, or first dial.

## Decision

**On an owner panic, `recoverAuthOwner` records the cause and cancels root; the
supervisor discriminates that cause into `StateError`. The owner still never calls
`terminalSequence`.**

- `recoverAuthOwner` builds an `*InternalError{Op: "auth-owner", …}`, records it via
  `setErrIfNil`/`setTerminalCauseIfNil` (cause recorded **first**), then calls
  `rootCancel()`. It performs no state transition. Canceling root unblocks the
  supervisor's *current* wait — the ensure-token rendezvous (both selects watch
  `rootCtx`), a `backoffWait`, a `dial`/`reconnectDial`, or an epoch read — exactly
  as a caller `Close` does.
- The supervisor's four "root canceled → stop" branches in `run()` route through a
  new `stopTrigger()` helper instead of a hardcoded `triggerCloseCalled`.
  `stopTrigger` returns `triggerTerminalFailure` (→ `StateError`) when a failure
  cause is already recorded (`c.err != nil`), and `triggerCloseCalled` (→
  `StateClosed`) otherwise. This is the same "discriminate on cause, not outcome"
  rule the epoch and dial paths already follow.
- The discriminator is unambiguous: `Close` and lifetime-cancel never write
  `c.err`; the `WithReconnect(false)` downgrade writes only `terminalCause` via
  `applyOutcome` and never reaches these root-canceled branches; `applyOutcome`'s
  own terminal `setErr` and the supervisor's panic-recover both write `c.err` but
  then exit without re-entering the loop. So `rootCtx canceled && c.err != nil` at
  one of the four sites uniquely means an owner panic.

Two happens-before edges make it deterministic (both verified in review): the
`ownerWg.Go` closure's `Done` fires only after `recoverAuthOwner`'s defer returns,
so the supervisor reaches `terminalSequence` (after `ownerWg.Wait()`) only once the
cause is set — `*Terminal{Err: nil}` for an owner panic is impossible; and the
owner writes `c.err` under `mu` *before* `rootCancel()`, so the supervisor's
`stopTrigger` read (under `mu`, caused by observing the cancel) always sees it.

**Single-terminator, restated:** the owner never calls `terminalSequence` — the
supervisor's exit defer remains the sole terminator. An owner panic merely
*triggers* that defer via `rootCancel`, exactly as `Close` triggers it. Only the
"owner never cancels root" half of ADR-0009's aside is superseded.

## Consequences

- An owner panic is now a proper terminal: `Err()` and `*Terminal.Err` carry the
  `*InternalError`, and `State()` is `StateError` — not a silent wedge, and not the
  `StateClosed`/`Err() == nil` of a clean stop.
- Two recorded statements of the old invariant were stale and are updated: `run()`'s
  launch comment (refreshed in this change) and ADR-0009's parenthetical (superseded
  here — ADR-0009 stays as written per the never-edit rule).
- A narrow, benign race remains: an owner panic that lands *concurrently* with an
  independent caller `Close` can report `StateError`/`Err() == *InternalError` for a
  caller who called `Close` (the panic's `c.err` write wins the `stopTrigger` read).
  This is a deliberate, documented deviation from the "caller's stop wins" doctrine —
  a real internal failure did occur, `Close()` itself still returns nil, and
  deterministic ordering is impossible for genuinely concurrent independent events.
  It mirrors the pre-existing supervisor-panic shape.

## Alternatives rejected

- **Record only into the epoch first-cause slot (reuse the exhaustion mechanism).**
  Rejected: the slot is nil between epochs, which is precisely when a B1 pre-dial
  fetch (and thus a panic on that path) runs — it would not unwedge the supervisor's
  rendezvous.
- **A dedicated `ownerDone` channel the supervisor selects on.** Rejected: it fixes
  only the rendezvous wedge, not the terminal semantics, and adds a second
  owner-death signal parallel to the `rootCtx` cancel every other supervisor wait
  already honors — the kind of parallel path §XV forbids. `rootCancel` unwedges
  every wait with one mechanism.
- **Have the owner emit the state transition itself** (`transition(triggerTerminalFailure)`
  before or after `rootCancel`). Rejected: emitting before the cancel can park the
  owner forever on a full delivery channel (recreating the wedge); emitting after
  races the supervisor's own transition (nondeterministic final state). Recording
  the cause and letting the supervisor emit keeps "helpers record, supervisor emits."
