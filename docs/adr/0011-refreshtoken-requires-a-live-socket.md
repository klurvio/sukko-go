# ADR-0011: RefreshToken requires a live socket (reverses the queue-on-reconnect behavior)

**Status**: Accepted
**Date**: 2026-08-25
**Ticket**: feat/go-sdk

## Context

`RefreshToken(ctx)` shipped with a "best-effort" contract: while disconnected it
returned `nil` and queued the refresh (via the owner's `pending` flag), which the
`authEpochUp` signal then retried on the next connection. A test
(`TestRefreshWantedWhileDisconnectedSendsOnReconnect`) and a doc comment pinned
this "a refresh wanted while disconnected is not lost" behavior.

Two problems surfaced while designing `Escalate` (which requires
`*NotConnectedError` when not connected, per §XV — "a call that sometimes sends a
frame and sometimes does not is the dual-purpose value §XV forbids"):

1. **The queued refresh is functionally redundant.** On reconnect, the dial's
   handshake already re-authenticates using the per-dial credential-store read
   (which `UpdateToken`, a `TokenSource` fetch, and B1's pre-dial fetch all keep
   current). Re-sending an `auth` frame after the handshake changes nothing.
2. **§XVIII inconsistency.** `RefreshToken` and `Escalate` are the same kind of
   imperative — "send an `auth` frame now on the live socket." Having one queue
   silently while the other returns `*NotConnectedError` is the cross-method
   inconsistency §XVIII forbids.

Prior-art research (Ably `authorize`, Centrifugo `setToken`, Socket.IO
`socket.auth`) confirms the industry norm: a credential *update* is client STATE
applied on the next (re)connect — never an imperative that errors offline — and
that state role is exactly `UpdateToken`'s. No mainstream SDK exposes a "send a
refresh frame right now, error if offline" call; where an explicit imperative
exists it is connection-bound. So the honest split is: `UpdateToken` = the offline
state path; `RefreshToken`/`Escalate` = live-socket imperatives.

The original method×state design table already specified `*NotConnectedError` for
`RefreshToken` while disconnected; the shipped queue behavior had diverged from it.
No ADR recorded the queue decision, so this is a superseding-by-code reversal, not
an amendment to a recorded decision.

## Decision

**`RefreshToken(ctx)` returns `*NotConnectedError{Op: "RefreshToken"}` in any state
other than `connected` (and `ErrClosed` once closed); it enqueues an `auth` send
only while connected.** The documented offline path is `UpdateToken(jwt)` +
reconnect, whose next dial re-authenticates.

`authEpochUp` and the owner's `pending` flag are **retained**: a PROACTIVE refresh
whose timer fires during a reconnect backoff still cannot send, is remembered, and
is retried on the next connection — this keeps the refresh *schedule* alive across
a drop (the auth_ack that answers the retried send re-arms the proactive timer,
which the reconnect handshake alone does not do for a static-JWT client). That path
is now `authEpochUp`'s sole trigger and is covered by
`TestProactiveRefreshDuringBackoffRetriesOnReconnect`.

## Consequences

- `RefreshToken` and `Escalate` share one not-connected contract (§XVIII); the
  offline story is uniform ("use `UpdateToken` + reconnect").
- A caller that previously relied on `RefreshToken` succeeding while disconnected
  now receives `*NotConnectedError`. This is the correct signal — the credential is
  already reapplied by the reconnect dial, so nothing was actually lost by not
  queuing.
- `TestRefreshWantedWhileDisconnectedSendsOnReconnect` is replaced by
  `TestRefreshTokenWhileReconnectingReturnsNotConnected` (the new contract) plus
  `TestProactiveRefreshDuringBackoffRetriesOnReconnect` (the retained
  `authEpochUp` mechanism, driven by its real trigger).

## Alternatives rejected

- **Keep the queue-on-reconnect behavior.** Rejected: redundant (the dial
  re-authenticates), §XVIII-inconsistent with `Escalate`, and divergent from the
  method×state contract. "Correctness over pattern" — shipped-but-redundant code is
  not precedent to preserve.
- **Also remove `authEpochUp`** (extend the "retry is redundant" reasoning to the
  proactive path). Rejected: unlike a caller refresh, a proactive refresh retried
  on reconnect re-arms the expiry schedule via its `auth_ack` — real value for a
  static-JWT client whose handshake carries no `auth_ack`. Kept and tested.
