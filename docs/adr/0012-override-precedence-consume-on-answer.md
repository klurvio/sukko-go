# ADR-0012: Caller-credential override is consumed on the auth ANSWER, generation-guarded

**Status**: Accepted
**Date**: 2026-08-25
**Ticket**: feat/go-sdk

## Context

The founding auth requirement: a credential set by `UpdateToken` (or supplied to `Escalate`)
wins the next auth over a configured `TokenSource`; thereafter the `TokenSource`
resumes. B1 (ADR-0009) deferred this as an "override-pending" flag with a loud
comment; this ADR records how it actually lands, because three concurrency-review
rounds showed the naive shapes are subtly wrong in ways the race detector cannot
see.

The credential is presented on two asynchronous paths — a connected `auth` frame
(answered later by `auth_ack`/`auth_error`) and a dial handshake (its "answer" is
handshake success) — and a caller (`UpdateToken`/`Escalate`) can race the owner at
any point. The failure modes found and rejected:

- **Consume on send-success (or dial attempt).** A frame that leaves but whose
  epoch dies unanswered, or a dial that fails before a handshake, then loses the
  caller's credential to a resumed `TokenSource` fetch — the exact B1 clobber,
  resurfacing in the failure window.
- **Clear the flag unconditionally on consume.** An `UpdateToken` that races the
  consume (bumping the credential) has its override wrongly cleared, losing it to
  the next fetch.
- **`commitEscalation` unconditional.** `Escalate` is fire-and-forget, so its ack
  lands a round-trip after a sequential `UpdateToken`; an unconditional commit
  overwrites the later `UpdateToken`, violating latest-caller-wins in the common
  case.
- **A stale coalesced `authEpochUp`** from a dead epoch, processed after the next
  dial armed its override, consumes that override on the *previous* epoch's
  handshake — premature consume, credential lost if the new dial fails.

## Decision

**One invariant: the override is consumed only when the presenting auth is
ANSWERED — never merely attempted — and every clear is generation-guarded at the
caller-action moment. Consumption may be deferred, but is never lost.**

- The store carries `overridePending` and a monotonic `overrideGen`. `setToken`
  (`UpdateToken`) sets the flag and bumps the gen. `setTokenFromSource` (a
  `TokenSource` fetch) no-ops under a pending override — a fetch never clobbers a
  caller credential. `peekOverride` reads without clearing; `clearOverrideIfGen`
  clears iff the gen still matches; `commitEscalationIfGen` writes + clears iff the
  gen matches.
- **Connected refresh**: peek the override, present it, record `flightOverride` on
  send-success — then clear it in the poke handler on the `auth_ack`/`auth_error`
  answer. An unanswered epoch death resets the flag WITHOUT clearing, so the
  override survives to the reconnect.
- **Dial**: `dialFetch` peeks and records `dialOverride`; the `authEpochUp`
  (handshake-success) signal consumes it. A failed dial (no `authEpochUp`) leaves
  it pending; the reconnect re-presents it. A **stale coalesced `authEpochUp` is
  drained inside the dial rendezvous before replying** — the supervisor is the sole
  `authEpochUp` sender and is parked in the rendezvous, so any queued signal
  provably predates this dial and cannot be mis-attributed to it.
- **Escalation**: the override generation is captured at the `Escalate` **call**
  (not send — an escalation can queue behind a flight for a full round trip), rides
  the escalate box, and the ack commits via `commitEscalationIfGen`. So
  `UpdateToken`-then-`Escalate` commits (the escalation is newer; a static client's
  pre-existing stale flag is also cleared here); `Escalate`-then-`UpdateToken`
  no-ops the commit (the `UpdateToken` is newer and survives).
- **Connect ordering** (a bug the `RefreshToken`/`Escalate` state checks exposed):
  the supervisor reports a first-dial SUCCESS to `Connect` only AFTER the
  `→connected` transition, so `Connect` returning nil guarantees `State()` is
  Connected and an immediate `RefreshToken`/`Escalate` cannot race into
  `*NotConnectedError`.

## Consequences

- Every constructible race degrades to *re-presentation of the override on the next
  auth*, which is idempotent and converges — the "deferred, never lost" property.
- A committed escalation and a `TokenSource` fetch both write via non-override
  paths, so a `TokenSource` resumes immediately after either. A static client's
  `overridePending` is inert (no fetch reads it) except to clear on an escalation
  commit.
- New owner state (`flightOverride`/`dialOverride`/`inFlightJWTGen`/`escalationGen`)
  is owner-goroutine-local; the store's three new methods are leaf-locked, short,
  and never nested with the client mutex.

## Alternatives rejected

- **Consume on attempt / send-success** — loses the credential on the failure
  window (see Context).
- **A single unconditional clear / commit** — loses a racing or later caller
  action; the generation guard is what makes both latest-caller-wins.
- **Make the epoch-up dial-attributable via a sequence number** instead of draining
  — more machinery for the same guarantee the structural drain already provides.
