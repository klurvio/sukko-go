# Sukko Go SDK Engineering Principles

These are the engineering principles that govern this SDK's codebase — the SDK
adaptation of the [Sukko platform engineering principles](https://github.com/sukko-dev/sukko/blob/main/docs/engineering-principles.md).
Code comments cite them by section: a comment like `// per §VI` refers to a
section of one of the two documents. Each heading below carries a parenthetical
cross-reference (e.g. `(§VII)`) naming the platform section it adapts — use it
to map between the two numberings; a cited section that does not exist below
(e.g. §XV–§XVIII) always refers to the platform document. Architecture
decisions are recorded separately in [`docs/adr/`](adr/).

This is the published form of the project's internal engineering rules; the
two are kept in sync on every amendment.

> **Shared across all Sukko SDKs.** Principles **I** (contracts), **XI**
> (language quality bar), and **XII** (prior-art research) are the *shared
> Sukko SDK constitution* — they hold identically for `sdk-go`, `sdk-py`,
> `sdk-js` (`@sukko/sdk`), and any future SDK. Each SDK keeps the same
> **behavioral contract** while being **idiomatic to its own language**; a new
> SDK adopts these same principles, adapted to its ecosystem. The remaining
> principles here are this repo's Go-specific adaptations of the same platform
> values.

## I. Contracts are the single source of truth (§XII/§XVI/§XVII)

Every public type and every runtime behavior MUST derive from Sukko's **authoritative API
contracts** — never from the platform's server-internal protocol package, and never copied from a
sibling SDK's current behavior (which may lag). The two contracts, referenced explicitly:

- **AsyncAPI** — [`ws/docs/asyncapi/client-ws.asyncapi.yaml`](https://github.com/sukko-dev/sukko/blob/main/ws/docs/asyncapi/client-ws.asyncapi.yaml)
  in the platform repo — the WebSocket client protocol (message `type`s, payload schemas,
  error/close codes, auth bindings, channel format).
- **OpenAPI** — [`ws/docs/openapi/gateway.openapi.yaml`](https://github.com/sukko-dev/sukko/blob/main/ws/docs/openapi/gateway.openapi.yaml)
  in the platform repo — the REST surface (publish, auth, SSE, push), status/error codes, and
  payload limits.

Pinned copies are vendored in [`testdata/contracts/`](../testdata/contracts/) so the test suite is
hermetic. Wire `type` consts, JSON field names, error codes, close codes, and limits MUST match the
contracts **exactly** (`message` not `data`; `auth` not `auth_refresh`) — struct tags spell the
wire names, Go names stay idiomatic. A **contract-coverage test** asserts every AsyncAPI message
type has a typed Go representation. When the contract and an existing SDK's behavior disagree,
**the contract wins** and the drift is filed upstream (never silently matched). Deliberate
divergences from `@sukko/sdk` (header-default auth, back-pressure, gap recovery) MUST be
documented.

## II. Defense in depth (§II)

Validate at every boundary. Decode every server message through the typed message set and reject
malformed input (e.g. a `gap` missing its required `last_pos`) — never `map[string]any`
pass-through. Diff subscribe grants and surface not-granted channels. Constructor config MUST be
validated (functional options and/or a `Config` struct with a `Validate()` seam): required fields
present, cross-field invariants hold (e.g. queue size ≥ history limit + max replay), and the
constructor returns an error and **fails fast** — never silently defaults into wrong state.

## III. Error handling — no silent failures (§III)

Every contract error/close/HTTP code maps to a **typed error** — sentinel `errors.New` values and
structured error types, matchable with `errors.Is`/`errors.As`. All errors wrapped with context via
`fmt.Errorf("operation: %w", err)`. `Publish()` while not connected returns `ErrNotConnected`
immediately — never a silent no-op. A truncated recovery returns a `RecoveryInterruptedError`,
never a bare disconnect. `not_available` (Direct backend) is a **typed capability signal**, not a
retryable error. Ignored errors carry an explicit comment explaining why. No raw transport
internals leak to callers.

## IV. Graceful degradation (§IV)

Reconnect uses **exponential backoff + jitter, capped**. Where the SDK holds no `last_pos` cursor
for a channel, pos-recovery cannot be anchored → degrade to naive resubscribe (no retry loop) and
emit the single coalesced `PossibleGap` data-loss signal (§IX) — cursor state, never a backend name.
Handshake 429 (`TENANT_LIMIT_EXCEEDED`) backs off — never hammers. Optional features (SSE, push)
are edition-gated with typed errors, never half-initialized state.

## V. Structured logging — library-quiet, no secrets (§V, §IX)

Use **`log/slog`** with structured attributes, not string interpolation:
`logger.Info("connected", "transport", "ws", "tenant", tenant)`. The SDK is **library-quiet by
default**: it logs through a no-op/discard handler unless the caller supplies a `*slog.Logger` via
options. The SDK MUST NEVER call `slog.SetDefault`, configure any global logger, or emit at package
scope — the application owns log configuration. **Credentials MUST never reach a log record or an
error string** — every such string passes through `redact.go` (value-based masking of the
registered token/api-key/push-key + pattern masking of `token=`/`api_key=`/`Authorization`/
`X-API-Key`). Redaction is asserted by test, not assumed "by construction."

## VI. Concurrency safety (§VII)

The platform's goroutine rules, applied to a client library:

- **Goroutine ownership over shared locks** — stateful flows (connection lifecycle, subscription
  tracking, auth refresh) are owned by a single goroutine communicating via channels; `sync.RWMutex`
  is acceptable only for simple read-heavy caches, `atomic` for counters/flags.
- **Tracked lifecycle** — every goroutine is launched under a `sync.WaitGroup` or `errgroup.Group`
  and MUST exit on `ctx.Done()` or close. **No goroutine leaks**: `Close(ctx)` cancels the context,
  waits for all goroutines (bounded by the caller's ctx, with **no drain** — the ctx-aware delivery
  send makes the unpark provable), closes the transport, non-blocking-sends the final `*Terminal`
  into its dedicated reserved slot, then closes `Messages()` exactly once from the supervisor — in
  that order. Per reconnect epoch, the connection-scoped goroutines (read pump,
  heartbeat) are torn down and recreated — an outer supervisor owns the reconnect loop.
- **`context.Context` threads through every blocking/networked call** — `Connect`, `Subscribe`,
  `Publish`, `Close`, refresh. Cancellation is honored promptly, never swallowed.
- **`sync.Once` guards close** — transport close and stop-channel close execute exactly once; a
  double close is a panic, not a no-op.
- **Recover at goroutine entry points** — every spawned goroutine's first `defer` is the SDK's own
  small recover helper (log-and-surface, never a bare inline `recover()` that swallows panics).
- **Channels match usage** — data channels buffered to throughput; fan-out/delivery sends never
  block on feature work; signal channels are `chan struct{}` closed by exactly one owner.
- The delivery pipeline (decode → buffer → `Messages()`) is the **hot path**: feature work (auth
  refresh, subscription tracking, callbacks) MUST NOT block it, and no lock is held across a
  pipeline send.

## VII. Race detector AND deterministic time (§VIII)

The Python SDK's principles note "Python has no `-race`; deterministic time is the substitute."
**Go has the real race detector back — so this SDK mandates BOTH, not either:**

1. **All tests run with `go test -race ./...`** — the race detector is the primary automated
   enforcement of §VI. A test run without `-race` MUST NOT be considered passing.
2. **An injectable clock seam** (`clock.go`: `Clock` / sleep / RNG interface) drives every timing
   path — backoff, jitter, heartbeat interval, pong timeout, replay floor, recovery deadline.
   Timing tests use a fake clock — **never real `time.Sleep` in tests**.

Unit tests run against an in-process fake transport/server. Test coverage is mandatory: one
assertion per client→server message, per server→client event/error, per close code, plus
heartbeat-timeout, cancellation/close-drain, back-pressure overflow, and recovery. Tests are
table-driven for multiple cases; edge cases (empty, nil, max, error paths, cancellation) covered.

## VIII. Security (§IX)

Credentials sent via **request headers by default** (`Authorization: Bearer` / `X-API-Key`);
query-param auth is opt-in and its leak surface is closed by redaction (§V). JWT refresh issues a
new token (never extends). Auth refresh is single-flight (`singleflight` or owned-goroutine
equivalent) with a configurable floor (default 30s) to avoid an `auth_error`→refresh loop. Edition
gates (SSE/publish = Pro; push = Enterprise) surface a typed `EditionRequiredError`, never a raw
403. TLS for all external endpoints; validate untrusted server input before acting on it (§II).

## IX. Simplicity — explicit modes (§XV)

No implicit mode detection, no dual-purpose values, no silent fallback. The `auth` message is
dual-purpose on the wire (refresh vs escalation) — the mode-switch is explicit off the SDK's
**owned credential state**, not runtime-detected. **Degraded recovery is driven by the SDK's own
cursor state**: at reconnect, every channel **granted in the epoch that just ended** for which the
SDK **holds no `last_pos` cursor** cannot be anchored, and those channels MUST be reported as
**exactly one coalesced `PossibleGap{Channels}` per reconnect** — one signal, never one per channel,
and none at all when the set is empty. The SDK never names, guesses, or infers a backend; it states
a fact about state it owns (a cursor it either holds or does not). The set is that **granted
snapshot**, never the desired set — a permanently-denied channel would otherwise emit a false
data-loss signal on every reconnect forever; never-granted channels have their own surfaced signal
(the grant diff / `PendingSubscriptions()`) and MUST NOT appear in any `PossibleGap`. This is
*stricter* than the earlier "wait for `reconnect_error: not_available`" rule, which was disproved
against server source: `not_available` is emitted only when the backend is nil, so a Direct
reconnect returns `reconnect_ack{status:"completed", messages_replayed:0}` and a client waiting for
the error frame would never degrade — silent data loss. `reconnect_error: not_available` is still
handled as a **typed capability signal** (§III) and degrades identically; it is the rarer additional
trigger, never the only one. The wire
`gap` (`last_pos` required) and the synthetic `PossibleGap` (channel-only) are **distinct types** —
one value never means two things. If explaining a behavior needs "unless" twice, redesign it.

## X. Cross-repo awareness (§XVI)

Sibling repos: [`sukko`](https://github.com/sukko-dev/sukko) (platform + contracts),
[`sdk-py`](https://github.com/sukko-dev/sdk-py) (Python SDK — the reference implementation),
[`sdk-js`](https://github.com/sukko-dev/sdk-js) (TS SDK), [`cli`](https://github.com/sukko-dev/cli)
(operator CLI — NOT this SDK's domain), [`docs`](https://github.com/sukko-dev/docs) (docs site —
its SDK reference pages are CI-generated from source). Contract drift found while building is
filed upstream, not worked around here.

## XI. Language quality bar — idiomatic, robust, performant, secure (SHARED)

**Applies to every Sukko SDK, adapted to its language.** The implementation MUST be **idiomatic to
Go and its ecosystem** — not a transliteration of a sibling SDK. Beyond idiom, every change MUST
clear five bars:

- **Idiomatic** — Effective Go: `gofmt`-clean, passes `go vet` and `golangci-lint`; errors as
  values (`errors.Is`/`As`, `%w` wrapping); `context.Context` first parameter on blocking calls;
  small accept-interfaces-return-structs API; functional options; no `init()` side effects; prefer
  the stdlib and one small WS dep over frameworks. Exported API is minimal, documented (GoDoc), and
  semver-stable.
- **Robust** — typed errors and no silent failures (§III), graceful degradation (§IV), a clean
  goroutine lifecycle with no leaks (§VI), input validated at every boundary (§II). Edge cases
  (empty, nil, max, error paths, cancellation) are covered by tests (§VII).
- **Performant** — the message-delivery pipeline is the hot path: no needless allocation or
  re-serialization, back-pressure over unbounded buffering, feature work never blocks delivery.
  Use the fast path the language offers (preallocated buffers, `atomic` over locks on counters,
  zero-allocation decode where practical).
- **Secure** — credentials never appear in logs, error strings, or serialized state (§V/§VIII);
  header-default auth; TLS for external endpoints; validate untrusted server input before acting
  on it.
- **No dead code** — every code path, capability branch, exported symbol, and message/struct field
  MUST be reachable and exercised. A discovered dead or unreachable path MUST be **removed** (or
  made reachable), never merely guarded around; a degenerate never-taken branch is a bug, not
  defensive coding. No stub/no-op implementations ship — unbuilt work stays out of scope, not empty
  scaffolding. Enforced as part of definition-of-done by the compiler (unused imports/locals),
  `go vet`, and `golangci-lint` (`unused` / staticcheck U1000).

The implementations share the same **behavioral contract**; they need not share code shape.
Correctness over pattern: if a sibling SDK does it wrong, fix it there too — don't copy the defect.

## XII. Prior-art & industry research — mandatory, every change (SHARED)

**Before designing any feature, fixing any bug, or making any improvement**, research how the
problem is already solved — **on the internet, not from memory** (docs and training data go stale).
This mirrors the platform principles' §XI. Research and briefly document:

1. **The common industry pattern** — how established real-time clients solve it: Pusher, Ably,
   Socket.IO, Phoenix Channels, Centrifugo, PubNub, and the platform's own prior art.
2. **Failure modes & edge cases** mature implementations handle (reconnect storms, token races,
   back-pressure, partial recovery, ordering, idempotency).
3. **Language-ecosystem norms** — for Go: `context` and goroutine-lifecycle patterns, WS library
   trade-offs (`coder/websocket` vs `gorilla/websocket`), `net/http` SSE practice, module/semver
   conventions, API design norms of mature Go clients (redis/go-redis, centrifuge-go, polygonws).
4. **Where and why this SDK deviates** from the common pattern.

"Not invented here" solutions to already-solved problems are forbidden. A change without this
research is incomplete — cite the sources (PR description or a code comment).

## XIII. Decision Records

Durable engineering decisions are recorded as Architecture Decision Records in
[`docs/adr/`](adr/) — any choice that is likely to be challenged, expensive to reverse, or needed
by a future contributor MUST be recorded at the moment it is made. Accepted ADRs are never edited —
they are superseded by new ones. ADRs capture the decision, its context and consequences, and the
rejected alternatives.

Planning artifacts are ephemeral — there are no per-feature specification or plan documents in the
repository; the durable outputs of design work are ADRs, the vendored contracts (§I), and committed
documentation.

## Governance

When platform-side behavior and this document disagree, the **contracts** win — precedence is
**contracts > ADRs > illustrative guidance**. The numbered principles above are amended, not
overridden. The platform-wide rules live in the
[platform engineering principles](https://github.com/sukko-dev/sukko/blob/main/docs/engineering-principles.md).
