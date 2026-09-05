# ADR-0005: Conformance test expectations derive from the contract, never from the implementation

**Status**: Accepted
**Date**: 2026-08-22
**Ticket**: feat/go-sdk (Phase 2 — wire codec)

## Context

ADR-0003 makes the vendored contracts the source of truth and requires "a contract-coverage test [that] asserts every AsyncAPI message type has a typed Go representation." That test was built (`conformance_test.go`), passed, and the wire codec still shipped 10 of 26 message types broken against the real gateway: every payload-bearing client→server frame was flat where the server requires the payload nested under `data`; `auth_ack`/`auth_error` were flat and `auth_ack` carried an invented `mode` field; `replay_complete` read `count` where the wire field is `messages_replayed`; `reconnect_ack` dropped the required `message`. All five gates were green throughout.

The coverage test only compared **type-name sets** — it never checked frame **shape**. Shape was checked by golden fixtures, and the fixtures had been hand-authored from the SDK's own structs. A struct and a fixture written to match it agree by construction: the round-trip proved the code consistent with itself and said nothing about whether either matched the server. Careful field-modeling reasoning (pointer-vs-plain for absent≠zero) was applied throughout and was locally sound — and still wrong against the contract in ten places, because a plausible zero (`auth_ack.exp == 0` → "never expires") reads as a real value at every layer above. Self-referential fixtures cannot surface that; only an external referent can.

## Decision

Conformance expectations are transcribed from the contract's own `examples:` blocks, never from the SDK's structs. `contract_examples_test.go` parses the vendored AsyncAPI with a real YAML parser (`gopkg.in/yaml.v3`, a test-only dependency outside the runtime graph the one-dependency guard measures), walks both `components.schemas.*.examples` (server→client frames) and `components.messages.*.examples` (client→server frames, wrapped in `payload`), loads each example into the wire struct the SDK uses for that type, re-encodes, and requires the result to carry exactly what the contract's example carried — dropped field and invented field both fail. Every type in either registry must be covered by at least one contract example or be named in `skippedExamples` with a written reason (`subscribeWithHistory`: the dual-mode payload §IX forbids; `heartbeatExample`: the one frame whose `data` the contract marks optional). A skipped type still accounts for itself in the coverage check — a skip cannot silently buy coverage. Lexical/name-only checks (the type-set coverage test) are retained as a second layer but are never the shape authority. Any hand-written test table that asserts wire bytes (e.g. the send-encode table) has its expected strings transcribed from the contract examples, not from the structs.

## Consequences

- A platform release that changes a frame's shape fails `TestContractExamplesRoundTrip` by name, the same way a new type fails the coverage test — shape drift becomes as loud as vocabulary drift.
- The YAML dependency enters `go.sum` with a small transitive test-tree tail (`kr/text`, `kr/pretty`, `creack/pty`), all indirect and pruned from consumers. Accepted as the cost of not hand-rolling a parser — an approximate parser would be the same class of error this test exists to catch.
- Fixtures under `testdata/fixtures/valid/` remain, but only as decode-dispatch and optional-field vehicles; they are no longer the shape authority and must stay transcribed from the contract (their error codes and enum values must come from the contract's enums, not be invented).
- The rule generalizes beyond this SDK: wherever a test's expectation and its subject share an author, the test proves consistency, not correctness. New conformance surfaces (the OpenAPI slice, future protocol versions) must find an external referent before they are trusted.

## Alternatives rejected

- **Keep hand-authored fixtures under stricter review** — rejected: the fixtures that shipped the bug were written carefully by someone reasoning correctly about field modeling. Review cannot catch a fixture and a struct that were made to agree; only a referent neither of them authored can.
- **Generate the wire structs from the AsyncAPI** — rejected per ADR-0003: the toolchain cannot express the absent-vs-zero distinctions the contract's quirks demand. Deriving *test expectations* from the contract while keeping the structs hand-written gets the external referent without the generator.
- **Trust the type-name coverage test alone** — rejected: it is precisely what passed while ten frames were mis-shaped. Vocabulary coverage is necessary and not sufficient.
