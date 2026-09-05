# ADR-0008: The client heartbeat-timeout close uses code 4000, direction-disambiguated

**Status**: Accepted
**Date**: 2026-08-23
**Ticket**: feat/go-sdk

## Context

The founding design chose **4999** for the
client-initiated heartbeat-timeout close, reasoning that the SDK should allocate
its own close codes from the **top** of the RFC 6455 private-use range
(4000–4999) while the platform allocates `force_disconnect = 4000` from the
bottom, so "opposite ends cannot collide." `defaults.go` and `policy.go` encoded
that: `CloseCodeHeartbeatTimeout = 4999`, and a comment declaring "a local 4000
is deliberately absent."

A Phase-5 code review (slice 4, heartbeat) checked this against the sources the
SDK constitution makes authoritative and found the choice diverges from all of
them:

- **The vendored contract** (`testdata/contracts/client-ws.asyncapi.yaml:50`)
  pins one row: `4000 | force_disconnect / heartbeat timeout | ... server-sent =
  operator force-disconnect; client-sent = client-initiated close after
  heartbeat (pong) timeout. Clients MUST use the close direction to
  disambiguate.` The contract already solves collision **by direction**, and it
  requires the client to send **4000**, not 4999.
- **Both sibling SDKs** (§XVIII) do exactly that. `sukko-py`
  (`src/sukko/constants.py`): `HEARTBEAT_TIMEOUT = 4000 # LOCAL`,
  `FORCE_DISCONNECT = 4000 # REMOTE`, with a comment "`@sukko/sdk` uses 4000 for
  a client-initiated heartbeat-timeout close … the number is identical, so the
  SDK MUST track direction to tell them apart." `sukko-py` states it mirrors
  `@sukko/sdk`, so `sukko-js` matches too.

So 4999 was a lone deviation, its collision-avoidance rationale made moot by the
direction disambiguation the contract and siblings already rely on, and — because
`CloseCodeHeartbeatTimeout` is an **exported** constant now made wire-visible for
the first time by slice 4 — shipping it would be a future semver break and the
"silently matched drift" the SDK constitution §I forbids.

## Decision

**The client heartbeat-timeout close uses code 4000, direction `local`**, sharing
the numeric value with the server's `force_disconnect` (direction `remote`) and
distinguished from it by close direction — matching the contract (§I) and both
sibling SDKs (§XVIII). `CloseCodeHeartbeatTimeout = 4000`. The close policy carries
both rows, keyed by `{code, direction}`: `{4000, remote}` → terminal (operator
force-disconnect), `{4000, local}` → reconnect (heartbeat timeout). The
remote-side value is named `closeCodeForceDisconnect = 4000` so neither 4000 is a
bare literal (§I). This **supersedes the earlier top-down-allocation
statement**, now stale.

## Consequences

- The classification the heartbeat path already relied on is unchanged — the
  policy table was written symbolically (`{CloseCodeHeartbeatTimeout,
  directionLocal}`), so only the constant's value moved.
- Direction tracking in the transport is load-bearing, exactly as the contract's
  "MUST use the close direction to disambiguate" requires; this is the same
  reason `transport_ws.go` labels close frames by direction.
- Two contract-derived tests that pinned the 4999 choice were corrected to assert
  the 4000/direction rule (`TestClosePolicyDisambiguates4000ByDirection`,
  `TestDefaultsByValue/close codes`) — the tests followed the contract, not the
  implementation (ADR-0005).
- Later client-originated codes (auth-failed, subscription-failed) should follow
  the siblings' **bottom-up** allocation (`sukko-py` uses 4001/4002), not the
  retired top-down scheme, when those phases land.

## Alternatives rejected

- **Keep 4999 + file an upstream contract issue.** Rejected: there is nothing to
  file — the contract and both shipped siblings already agree on 4000-by-direction;
  4999 is this SDK's error, not the contract's. Keeping it would leave sukko-go
  emitting a code the contract does not define for the event, out of step with its
  own ecosystem, to preserve a rationale the direction disambiguation makes
  unnecessary.
- **Keep the "top-down allocation" scheme for all SDK-originated codes.** Rejected:
  it is not what the siblings do, so it breaks cross-SDK parity (§XVIII) for every
  future client code, not just this one.
