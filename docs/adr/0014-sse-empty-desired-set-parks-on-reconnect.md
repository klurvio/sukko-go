# ADR-0014: An SSE stream parks (does not dial) when the desired set is empty on a reconnect

**Status**: Accepted
**Date**: 2026-08-29
**Ticket**: feat/go-sdk (Phase 11 SSE review finding)

## Context

SSE is receive-only: the desired channel set is the subscription, named in the
stream-open URL (`GET /sse?channels=…`), and the gateway rejects an empty set with
`400 INVALID_REQUEST`. On SSE the client mutates the desired set synchronously in
`Subscribe`/`Unsubscribe` (the async serializer queue is unusable — the dial reads the
set synchronously; see the S3a caller-side branch).

This creates a hole the WebSocket transport does not have: a legal `Unsubscribe` while
disconnected can empty the desired set, and the *next reconnect dial* then runs the
`validateSSEChannels` pre-check, gets `ErrInvalidSSEChannels`, and — because that error
is (correctly, for a first dial) classified terminal — bricks the client into
`StateError` for a legal API call. A race variant: `Unsubscribe(A)` then `Subscribe(B)`
while reconnecting, with the backoff timer firing between the two calls, terminates the
client on timing alone. On WebSocket, unsubscribing everything leaves the socket alive
and re-subscribable; the terminal outcome is an SSE-only divergence (§XVIII).

The first `Connect()` with an empty set is a genuine caller error (nothing to stream)
and correctly stays terminal — this ADR governs only the reconnect case.

## Decision

On a **reconnect** dial (a prior epoch ran), an empty desired set on SSE **parks** the
client — it does not dial, does not terminate — in a disconnected state, and re-dials
only when a `Subscribe` repopulates the desired set. A `Close` (or lifetime-context
cancel) while parked tears down cleanly. The first-dial empty-set case is unchanged: it
remains a terminal `ErrInvalidSSEChannels`, surfaced synchronously to `Connect`.

## Consequences

- **Easier**: the client is reusable across an unsubscribe-all, mirroring WebSocket
  (§XVIII); the `Unsubscribe(A)`+`Subscribe(B)`-while-reconnecting race resolves to
  "park through the transient-empty window, dial with the union" instead of a
  timing-dependent terminal; no SSE-only per-call error surprises.
- **Harder**: the supervisor gains a park-until-signalled path on the SSE reconnect
  leg — a wait on a "desired changed" signal (raised by the caller-side `Subscribe`
  mutation) or the root context. The state machine reports the parked client as
  disconnected/reconnecting, not connected and not errored.
- Bounded blast radius: WebSocket is untouched (its dial never pre-checks channels).

## Alternatives rejected

- **Clean-stop on reconnect** (empty desired ⇒ `Err()==nil`, `Messages()` closes):
  ends the client on a legal unsubscribe-all, diverging from WebSocket, and makes the
  `Unsubscribe(A)`+`Subscribe(B)` race randomly kill the client on backoff timing.
- **Refuse the last unsubscribe** (reject an `Unsubscribe` that would empty desired on
  SSE): adds a surprising per-call error absent on WebSocket (§XVIII), and forces an
  awkward ordering on a legitimate swap-all-channels sequence.
- **Keep terminal** (the S3a-as-built behavior): bricks the client into `StateError`
  for a legal API call — the defect this ADR closes.
