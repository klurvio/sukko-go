# ADR-0009: TokenSource has two invocation contexts; only the connected-refresh 5-strike is terminal

**Status**: Accepted
**Date**: 2026-08-24
**Ticket**: feat/go-sdk

## Context

`TokenSource func(context.Context) (Token, error)` is caller code the SDK calls
on its own schedule to obtain fresh credentials. The founding auth requirement gives it two invocation
contexts with **opposite** failure semantics, and the naive "exhaustion →
terminal" reading (including an earlier design steer that routed it through
`classifyDial → causeTokenSourceExhausted`) contradicts that requirement:

- **Connected refresh**: the owner fetches a token for the `auth`
  frame; after `MaxTokenSourceAttempts` (5) **consecutive** failures the client
  **transitions to the terminal error state** with `ErrTokenSourceFailed` rather
  than running with a credential it knows is dead.
- **Reconnect dial**: a `TokenSource` failure is **non-terminal** —
  "the SDK keeps backing off and re-attempting the token, and never consumes a
  doomed handshake … The SDK must not manufacture its own terminal state." A
  reconnect that dialed with a known-expired token would earn a handshake 401,
  which the close policy classifies as terminal — permanently killing a client
  over a transient token-endpoint blip.

`TokenSource` must be invoked **only from the auth-owner goroutine**, bounded by
`WithTokenSourceTimeout` — never on the decode/read path, so a slow
token endpoint cannot stall `Messages()`.

## Decision

**One owner-local consecutive-failure counter; the terminal is gated on the
connected-refresh context, not on the counter alone.**

- The **auth-owner** is the sole `TokenSource` caller. A fetch is bounded by the
  injectable `"token_source"` timer (the same timer+watcher shape as
  `reconnectDial`, reused, not re-hand-rolled).
- The counter is incremented on **every** fetch failure (either context) and
  reset on any fetch **success** (not on `auth_ack` — it counts token-source
  failures, not auth outcomes). Every failure surfaces `*TokenSourceError{Attempt}`.
- **Connected-refresh** fetch failures retry on the refresh backoff
  (`computeBackoffDelay`, floored by `RefreshMinInterval`). When such a failure is
  the 5th consecutive, the owner terminates the client by recording
  `lookupInternalCausePolicy(causeTokenSourceExhausted)` (→ `classTerminal`,
  `cause: ErrTokenSourceFailed`, `surface: nil`) into the **current epoch's
  first-cause slot** and cancelling that epoch — reusing the heartbeat-timeout
  mechanism. The owner holds a mutex-guarded `currentEpoch` reference (set in
  `runEpoch` after `newEpoch`, cleared in its teardown defer). A stale/nil
  reference makes the record a no-op — the counter stays at Max and the reconnect
  path's fetch failures take over, which stays coherent with the non-terminal reconnect rule. The owner never calls
  `terminalSequence` or cancels root (single-terminator rule).
- **Reconnect-dial** fetch happens **before** `transport.Open`, only when
  `tokenSource != nil` (static clients keep the untouched store-read path). The
  fetch is a reply-chan request from the supervisor to the owner; a failure is
  returned as the dial error from `acquireConn`, and flows through the EXISTING
  machinery — redacted at the single source, `classifyDial`'s non-HandshakeError
  fallthrough → **reconnect-class → backoff**, `Connect` returning the wrapped
  error. **No classifier change, non-terminal forever** — exactly the reconnect rule
  above. The `causeTokenSourceExhausted` policy row therefore has exactly one
  consumer: the connected-refresh 5-strike.
- **`Token.Expiry`** is a second proactive-arm input alongside `auth_ack.exp`
  (ADR-0004): `armProactive` takes the earlier known instant. A successful fetch
  stores the expiry and arms immediately — closing the FACT-1 gap (no handshake
  `auth_ack`) for `TokenSource` clients, which otherwise have no initial schedule.
- Owner delivery emissions (`*TokenSourceError`) use a new `ownerSurface` =
  `send(rootCtx, ownerCtx, …)`, NOT `surface()` (`rootCtx, rootCtx`): the latter
  would hang `ownerWg.Wait()` on a terminal exit with a full channel. On the 5th
  strike the terminal is **recorded before** the attempt-5 `*TokenSourceError` is
  emitted, so a parked emit cannot delay the terminal. `*TokenSourceError` is the
  may-block data region, never the reserve.

**Increment order:** A = the connected-refresh path (fetch→send, counter, retry
arming, exhaustion→epoch-slot, `ownerSurface`, and the deferred floor-at-send from
the arming increment, which is owed here because the send path is reworked),
tested with `WithToken + WithTokenSource` (both compose, so the dial reads the
static token while the refresh path exercises `TokenSource`). B = the pre-dial
ensure-token reply-chan + `Token.Expiry`-from-first-fetch for `TokenSource`-only
clients, which changes the dial path and gets its own review.

## Consequences

- The two paths share a counter but not a terminal: the client can sit at
  counter ≥ Max across a long reconnect without dying, then terminate the instant
  a connected refresh fails again — matching the founding auth requirement exactly.
- The owner↔epoch coupling (`currentEpoch`) is new surface; it is the same
  first-cause-slot mechanism the heartbeat-timeout already uses and tests.
- `UpdateToken`/`Escalate` precedence (a caller-set credential
  wins for the next auth, then `TokenSource` resumes) is an override-pending flag
  that skips exactly one fetch; landed with this work or explicitly deferred with
  a comment, never silently dropped.

## Alternatives rejected

- **Route reconnect exhaustion to terminal via `classifyDial → causeTokenSourceExhausted`**
  (the earlier design steer). Rejected: reconnect fetch
  failures must be non-terminal indefinitely; only the connected-refresh path
  terminates.
- **A dedicated fatal-slot the backoff wait selects on.** Rejected: the epoch
  first-cause slot is the single existing termination mechanism and is tested;
  adding a second termination channel is the kind of parallel path §XV forbids.
- **Invoke `TokenSource` from the dial/decode path** to avoid the reply-chan.
  Rejected: a slow token endpoint must never stall `Messages()`, which
  owner-only invocation guarantees and a decode-path call would violate.
