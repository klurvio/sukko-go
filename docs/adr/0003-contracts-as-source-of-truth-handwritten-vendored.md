# ADR-0003: Derive every type and behaviour from the authoritative contracts — hand-written wire structs, vendored pinned contracts

**Status**: Accepted
**Date**: 2026-08-21
**Ticket**: feat/go-sdk

## Context

sukko-go is one of several Sukko SDKs (py, js) that must share a behavioural contract while staying idiomatic to their own language. Constitution §I mandates that every public type and runtime behaviour derive from the authoritative AsyncAPI v1.4.0 + gateway OpenAPI contracts — never from the server-internal `ws/internal/shared/protocol` package, and never copied from a sibling SDK's current behaviour, which lags the contract (sukko-py's own Direct-degrade clause was proven wrong against server source). AsyncAPI-3.0→Go codegen is immature. The SDK's CI has no checkout of the platform repo, so "test against the contract" needs a referent that lives inside this repo.

## Decision

Wire structs are hand-written, not generated. The server→client union (18 receive types) is dispatched by a two-pass decode (peek `type` from a small envelope struct, then decode the concrete type) over stdlib `encoding/json`; `Message.Data` stays `json.RawMessage` so callers decode payloads into their own types (via `DataAs[T]`). Wire `type` consts, JSON field names, error codes, close codes, and limits match the contracts exactly — struct tags spell the wire names, Go names stay idiomatic. A contract-coverage test asserts every AsyncAPI message type has a typed Go representation. The published contract documents (the AsyncAPI and the SDK-relevant OpenAPI slice) are vendored into `testdata/contracts/` at a pinned SHA-256 checksum with a documented copy-record-verify refresh procedure; CI never reaches across the module boundary. This is explicitly not the "protocol copy" §I forbids — that clause targets the server-internal Go package, not the published contract document. When contract and a sibling SDK's behaviour disagree, the contract wins and the drift is filed upstream, never silently matched. sukko-py is consulted as reference implementation, never copy-source.

## Consequences

- **Easier**: the public `Event` union and the wire shape evolve independently (wire structs stay unexported); the coverage test fails loudly on a platform v1.5.0; the pinned checksum converts fixture-staleness "diligence" into a mechanical, loud failure (the `go.sum` idea applied to a document); the runtime dependency count stays at one (`github.com/coder/websocket`), stdlib for the rest.
- **Harder**: hand-written structs must track contract quirks by hand — absent `truncated` must decode to `false`, `gap.last_pos` is required-at-decode but legally present-and-empty, `message`/`replay_message` normalize to one `*Message` + `Source`; refreshing a vendored contract is a manual step. Cross-repo drift becomes upstream filings the team owes rather than local workarounds.
- **Coupling**: every downstream layer decodes through the typed message set (no `map[string]any` pass-through); the parity obligation binds this SDK to the contract, not to sibling SDKs.

## Alternatives rejected

- **AsyncAPI→Go codegen** — the 3.0→Go toolchain is immature and would fight the contract's quirks (omitempty zero-decode, required-but-empty `last_pos`, the 18-type envelope union, the message/replay_message normalization).
- **Copying sibling-SDK behaviour** — §I forbids it and it proved unsafe: sukko-py's Direct-degrade clause waits for a frame the Direct server never sends (silent data loss; filed upstream).
- **Importing/wrapping the platform's internal protocol package** — §I forbids a cross-module dependency on the platform repo; that package is server-internal, unexported by Go's own `internal/` rule.
- **Fetching the contract over the network in CI** — non-hermetic and non-reproducible; would redden a sukko-go build on an unrelated platform-repo docs edit.
- **A faster JSON codec (json-iterator / go-json)** — zero-dep wins for an SDK; the hot path already crosses a channel per message, and payload speed is the caller's choice via `RawMessage`.
