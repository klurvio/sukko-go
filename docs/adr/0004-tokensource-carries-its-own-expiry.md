# ADR-0004: The caller-supplied credential callback returns a struct that carries its own expiry

**Status**: Accepted
**Date**: 2026-08-22
**Ticket**: feat/go-sdk

## Context

The SDK refreshes long-lived credentials itself: proactively from `auth_ack.exp`, reactively on `auth_error`, single-flight, floored by `RefreshMinInterval`. Fresh credentials come from caller code — the SDK never mints a token. The shape of that callback is public API, frozen by semver from `v0.1.0`, and it decides where refresh timing comes from.

Two mature Go ecosystems answer the same question, and both put expiry **on the credential**: `golang.org/x/oauth2.TokenSource` returns `*Token{AccessToken, Expiry, …}` with `Valid()`, and caching/early-refresh live in wrappers (`ReuseTokenSource`, `ReuseTokenSourceWithExpiry`); `aws-sdk-go-v2`'s `CredentialsProvider.Retrieve(ctx)` returns `Credentials{CanExpire, Expires}`, with `CredentialsCache` supplying concurrency and caching. oauth2's known wart is the absent `context.Context`; AWS fixed that and kept the expiry.

The SDK's draft signature was `func(ctx) (string, error)` — ctx-first (correct: the callback is invoked from SDK timers with no caller ctx to inherit, and `WithTokenSourceTimeout` needs something to bound) but returning a bare string. That made **`auth_ack.exp` the SDK's only source of expiry truth**, including its `exp == 0` "no expiry" case, where no refresh timer is armed at all and an actually-expiring token is discovered only reactively via `auth_error` — a round-trip and a failed operation later.

## Decision

The callback returns a small exported struct:

```go
type Token struct {
    Value  string
    Expiry time.Time // zero => unknown; fall back to auth_ack.exp
}
type TokenSource func(ctx context.Context) (Token, error)
```

A **zero `Expiry` means "unknown"** and preserves the previous behaviour exactly — the SDK arms refresh from `auth_ack.exp` as before — so the default costs callers nothing. A **non-zero `Expiry` is caller-authoritative** and gives the refresh scheduler a second input that survives `exp == 0`. When both are known, **the earlier instant wins**: whichever side expects expiry sooner is the safe one to schedule against.

Everything else about the auth flow is unchanged: ctx-first invocation from the auth-owner goroutine only (never the decode path), bounded by `WithTokenSourceTimeout` (10s), failures retried on the capped refresh backoff with a typed in-band `*TokenSourceError` per attempt and terminal `ErrTokenSourceFailed` after `MaxTokenSourceAttempts`, non-terminal during reconnect (never consume a doomed handshake). The SDK deliberately does **not** adopt oauth2's caching wrappers: `RefreshMinInterval` is the SDK-side rate floor and single-flight is SDK-owned.

## Consequences

- **Easier**: a caller that already knows its credential's lifetime (an IdP response, an `oauth2.Token`, AWS `Credentials`) can hand it over directly, and refresh is armed without waiting for a server round-trip — including the `exp == 0` deployment, where the string-returning shape armed nothing at all. Adapting an existing `oauth2.TokenSource` becomes a two-line wrapper that carries `Expiry` through instead of discarding it.
- **Harder**: one more exported type on the v0.1.0 surface, and a documented precedence rule ("earlier wins") that the refresh tests must pin on both sides — caller-earlier and server-earlier.
- **Coupling**: the refresh scheduler now takes two inputs; its tests must cover zero-`Expiry` (server-driven, the default path), non-zero with `exp == 0` (caller-driven — the case that motivated this), and both-known.
- **Timing**: taken pre-`v0.1.0` deliberately. As a func-signature change it is breaking, so the cheap moment to decide was before the first tag, not after.

## Alternatives rejected

- **`func(ctx) (string, error)`** (the draft) — smallest surface, but it discards information the caller usually has and leaves `exp == 0` with no armed refresh. The saving is one struct; the cost is a permanently degraded refresh path.
- **`oauth2.TokenSource` verbatim** — would import an external type into the public API of a near-zero-dep SDK, and drags `AccessToken`/`TokenType`/`RefreshToken` fields that have no meaning on this wire (a JWT string is the whole credential; there is no token type, no refresh token, no scope).
- **`Token` with `CanExpire bool` (AWS shape)** — a second field to express what a zero `time.Time` already expresses unambiguously.
- **oauth2-style caching wrappers** — the SDK already owns single-flight and a rate floor; a second caching layer would be two mechanisms for one job (§XV).
