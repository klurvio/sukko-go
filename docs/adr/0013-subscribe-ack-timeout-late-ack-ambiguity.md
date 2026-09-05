# ADR-0013: The subscribe AckTimeout accepts a bounded late-ack ambiguity

**Status**: Accepted
**Date**: 2026-08-26
**Ticket**: feat/go-sdk

## Context

The subscribe serializer keeps one subscribe/unsubscribe outstanding because the
wire's `subscription_ack` carries **no correlation id** — attribution
relies on single-outstanding. To stop a withheld ack from stranding the queue
forever, the serializer bounds the outstanding request with `AckTimeout` (= the
configured `RecoveryDeadline`): on timeout it emits a `*ProtocolError` and releases
the slot so the next queued request proceeds.

Releasing on timeout re-introduces attribution ambiguity that single-outstanding
otherwise removed: after the timeout releases flight N and a **same-kind** flight
N+1 is sent, N's *late* ack can arrive while N+1 is outstanding. With no correlation
id the SDK cannot tell N's ack from N+1's, so it could reconcile N's grant against
N+1's requested set — fabricating denials or discarding real ones, the exact failure
the serializer exists to prevent.

Two mechanisms bound this, but neither fully removes it:
- **Gen-stamped release pokes** stop a *stale poke* (whose flight the timeout already
  cleared) from releasing the next flight — this closes the concurrency race
  (Finding 1) but not the ack-attribution ambiguity, which lives in the ack itself.
- **A subset guard**, applied SYMMETRICALLY to both ack kinds, drops an ack whose
  channel list is not a subset of the outstanding flight's channels. For a
  `subscription_ack`, `subscribed` ⊆ the request's channels (the contract:
  `SubscriptionAck.subscribed` = the channels *this* operation subscribed, filtered
  ones excluded). For a non-forced `unsubscription_ack`, `unsubscribed` ⊆ the
  flight's granted subset (the channels it actually sent an `unsubscribe` for). A
  legit ack always passes; a late ack whose channels differ from the successor's is
  rejected on either side.

The residual: a late ack for N whose granted channels ARE a subset of N+1's request
(overlapping requests), or a **full denial** (empty `subscribed`, which is a subset
of everything), still mis-attributes. `count` (a connection-wide total) is unusable
for attribution.

## Decision

**Accept the bounded residual ambiguity. Ship AckTimeout with gen-stamped pokes and
the subset guard; do not add SDK-side machinery beyond them.** Stranding the queue
forever on a withheld ack is worse than a rare, bounded mis-attribution that only
occurs across a timeout with overlapping same-kind requests still queued.

The **root fix is a wire correlation id** — a client-chosen request id echoed on
`subscription_ack`/`subscribe_error` (filed upstream). That is a platform
protocol change owned by the contract's repo, not the SDK: a **§XVI cross-repo
note**. If it lands, single-outstanding and `ErrSubscribeQueueFull` become removable
and this ambiguity disappears.

## Consequences

- A caller can, in the narrow window above, observe a `*SubscriptionResult` whose
  `NotGranted` is inaccurate, or a contradictory `*SubscriptionResult` + timeout
  `*ProtocolError` pair for one logical request. SDK state stays consistent (a
  granted channel is really granted); only the reported diff can be wrong, and only
  transiently — the next clean ack corrects it.
- The subset guard adds one O(requested) check per ack; a legit ack is never
  rejected.
- The gen-stamped poke (`subFlight.gen`, `subPoke chan uint64`, release iff the
  poke's gen matches the current flight) also subsumes the epoch-reset stale-signal
  concern — a stale poke self-identifies — so the reconnect slice's poke handling is
  hygiene, not correctness.

## Alternatives rejected

- **No timeout (single-outstanding forever).** Rejected: a withheld ack permanently
  wedges the entire subscribe surface — strictly worse than a bounded transient
  mis-attribution.
- **A full SDK-side correlation scheme** (e.g. tagging outbound frames and matching
  on a synthesized key). Rejected: the wire echoes nothing back to match against, so
  any scheme is a guess; the honest fix is the wire id (upstream #10).
