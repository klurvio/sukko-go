# Phase 0 Research: Sukko Go SDK

**Branch**: `feat/go-sdk` (repo `sukko-go`) | **Date**: 2026-08-20

Prior-art record per the shared SDK constitution §XII (research how established real-time services solve the same problem before designing). The behavioral contract is inherited from `sukko-py` (whose own research.md D1–D6 grounds the recovery/auth/back-pressure semantics against AsyncAPI v1.4.0 + the gateway OpenAPI); this record covers only the **Go-idiomatic surface decisions** — where Go's model forces or permits a different shape than Python's.

## Prior-art survey — how established Go real-time clients deliver messages

Surveyed 2026-08-20 (web-verified unless marked):

| Client | Delivery surface | Back-pressure / slow-consumer story |
|---|---|---|
| **NATS (`nats.go`)** | Three surfaces: async `Subscribe(cb)`, `SubscribeSync` (pull), **`ChanSubscribe` (channel)** | For `ChanSubscribe`, "the pending limit is inherent in the buffer size of the channel you define. If the subscription is unable to send a message onto the channel, it will know the channel is filled and will report a slow consumer." Default pending buffers are large (500k msgs / 64MB) with an async error callback on overflow. Synadia's own guidance: `SubscribeSync` allocates a large per-subscription queue; async/channel modes are the better default. |
| **centrifuge-go** (Centrifugo Go SDK) | **Callback handlers** (`OnPublication`, etc.) | Handlers are "called synchronously by the SDK and block the connection read loop"; the docs explicitly warn you must not block inside handlers and cannot issue blocking client requests from handler code — "this will result in a deadlock." The callback model pushes the concurrency problem onto the caller. |
| **Polygon.io (`polygon-io/client-go/websocket`)** | **`Output()` — "returns the output queue"**: a channel the caller ranges over | Internal buffered output queue; caller drains at its own pace. |
| **Alpaca (`alpaca-trade-api-go`)** | User callbacks fed from an **internal buffered channel** | "The client uses a buffered channel to queue incoming messages… when the buffer fills, messages are **dropped** and `bufferFillCallback` is invoked" — drop-on-overflow with an observability hook, plus `WithReconnectSettings(limit, delay)`. |

**Synthesis**: mature Go market-data/real-time clients converge on **a bounded channel as the delivery spine** (NATS ChanSubscribe, Polygon Output, Alpaca internally), with the design axis being what happens at the boundary: NATS signals slow-consumer, Alpaca drops+notifies, centrifuge-go avoids the buffer by making the caller's handler the buffer (and inherits the deadlock foot-gun). None uses `iter.Seq` as the primary surface (all predate Go 1.23; no major client has migrated).

Sources: [nats.go slow consumers](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers), [Synadia on SubscribeSync memory](https://www.synadia.com/blog/go-nats-subscribesync-memory-use), [ChanQueueSubscribe discussion](https://github.com/nats-io/nats.go/discussions/1084), [centrifuge-go README](https://github.com/centrifugal/centrifuge-go), [polygonws pkg.go.dev (`Output()`)](https://pkg.go.dev/github.com/polygon-io/client-go/websocket), [Alpaca streaming docs](https://deepwiki.com/alpacahq/alpaca-trade-api-go/3.6-real-time-streaming).

## D1 — Delivery model: one bounded in-band event channel; `iter.Seq` as sugar; no callbacks

- **Decision**: primary surface `client.Messages() <-chan Event` — a **single bounded channel** carrying a sealed `Event` union (`*Message` dominant; advisory/lifecycle events in-band, in receive order). `Iter(ctx) iter.Seq[Event]` wraps the same channel for range-over-func ergonomics. The read-pump's send into the channel is **blocking**: full channel → pump stops reading the socket → TCP window closes → the platform's own slow-client path (3 failed deliveries → 1008) engages, then reconnect+recover. No callback registration API.
- **Why channel over `iter.Seq` as primary**: an iterator is a pull-model, single-consumer construct; a network feed needs a producing goroutine regardless, and "when you need concurrent access… you must add a goroutine anyway, and once you do, the iterator abstraction collapses" (the channel IS the goroutine boundary). Channels are Go's one naturally-concurrent iteration primitive; the iterator's raw-speed advantage (benchmarks show function iterators ~2 orders of magnitude faster than channel iteration) is irrelevant when every element already crosses a goroutine boundary from the read-pump. Prior art agrees: every surveyed client that offers a caller-drained surface offers a channel. `iter.Seq` earns its keep as *sugar* — `for ev := range client.Iter(ctx)` reads better and bakes in ctx-cancellation — which is also why 1.23 is the module floor.
- **Why no callbacks**: centrifuge-go documents the failure mode we'd inherit — synchronous handlers block the read loop and deadlock on re-entrant calls. sukko-py offered callbacks only as sugar over the stream; in Go the channel is already the ergonomic minimum, so callbacks add a second delivery mode (§XV cost) for zero capability.
- **Why ONE channel (messages + advisory events in-band), not a Messages/Events pair** — the contested sub-decision: a second `Events()` channel has no good overflow policy. If it's bounded+blocking, an app that only drains data wedges the read-pump on an un-drained event channel; if it drops on overflow, we silently drop *exactly the signals that exist to prevent silent drops* (`PossibleGap`, forced-unsubscribe) — violating the SDK's core stance; if it's unbounded, we've reintroduced unbounded buffering. In-band delivery also preserves **ordering**: a `PossibleGap` lands in-stream precisely where the potential loss sits relative to the data, which is the property sukko-py's FR-006 "single caller code path" clause encodes. Cost: consumers type-switch (`switch ev := ev.(type)`) — idiomatic Go, and the sealed union keeps the case-set known. Alpaca's drop+`bufferFillCallback` was considered and rejected: Sukko's contract forbids silent drop, and "drop with a counter" is strictly worse than blocking into the server's own slow-client machinery, which the platform already turns into a *recoverable* event (1008 → reconnect-replay on Kafka).
- **Back-pressure capability flag deleted (delta from Python/JS)**: sukko-py's transport model carries `can_pause_receive` because WHATWG WebSockets auto-drain (browser/undici can't stop reading). In Go **every** transport can stop reading — a blocked channel send stops the pump; `net/http` SSE reads stop when you stop calling `Read`. The capable/incapable branch has no incapable side, so it's gone (§XV: no dead modes). Documented consequence: a fully-blocked pump also stops processing pongs/acks — **by design**; the intended terminal state of a persistently-slow consumer is the server's 1008, surfaced and recovered, not client-side buffering heroics.

## D2 — WebSocket library: `github.com/coder/websocket` (single runtime dep)

- **Decision**: `coder/websocket` (the renamed, Coder-maintained `nhooyr.io/websocket` — one project, not two data points). REST/SSE/push over stdlib `net/http`; JSON over stdlib `encoding/json`. **Runtime deps = 1.**
- **Why**: it is the context-native option — `Read(ctx)`, `Write(ctx)`, dial with ctx — matching an SDK whose every public method is ctx-first; it handles concurrent writes safely (we write from user goroutines *and* the heartbeat/recovery goroutines); it's the current community recommendation for new projects (websocket.org's Go guide). `gorilla/websocket` (what the Sukko *server* uses; also centrifuge-go's choice) is the documented fallback — it's battle-tested but pre-context (deadline-based) and requires a caller-managed write mutex; adopting it would add lock plumbing the coder API makes unnecessary. **Rejected**: `golang.org/x/net/websocket` (legacy, discouraged); wrapping the platform's internal WS code (FR-012 forbids it — the SDK must not depend on the platform repo).
- **Dependency stance**: Go's stdlib HTTP client is production-grade (unlike Python, which needed `httpx`), so the Python "minimal = 3" story becomes "minimal = 1" here. SSE framing (`event:`/`id:`/`: keepalive`) is ~50 lines over `bufio.Scanner` — no dep justified.

## D3 — Protocol types: hand-written structs + contract-coverage test (not codegen), stdlib JSON

- **Decision**: hand-write the wire structs; dispatch the server→client union on the envelope `type` field via a two-pass decode (peek `type` from a small envelope struct, then decode into the concrete type); `Message.Data` stays `json.RawMessage` (callers decode payloads into their own types — no forced `map[string]any`). SC-003 asserts every AsyncAPI v1.4.0 message type is represented, round-trip.
- **Why**: same rationale as sukko-py D2 — AsyncAPI-3.0→Go codegen is immature, and a generator would fight the contract's quirks (omitempty `truncated` must decode absent→false; `gap.last_pos` required-at-decode; the `subscribe` payload's two mutually-exclusive modes). Hand-written + a coverage test keeps FR-012 honest. **`encoding/json` over a faster codec** (`json-iterator`, `go-json`, `easyjson`): zero-dep wins for an SDK; the hot path already crosses a channel per message, and callers who need speed decode `RawMessage` with whatever they like. Revisit only with a measured bottleneck.
- **Consequence for the sealed union**: `Event` is an interface with an unexported method; all concrete event types live in package `sukko` — callers can exhaustively type-switch but not forge new event types, keeping "one value never means two things" (§XV) enforceable.

## D4 — Concurrency topology: supervisor + per-epoch goroutine set (the TaskGroup translation)

- **Decision**: one supervisor goroutine owns the connect→run→backoff→reconnect loop; each connection epoch derives a child context and launches read-pump + heartbeat + recovery timers under a per-epoch `errgroup`; epoch teardown (`cancel` + `Wait`) completes before re-dial. The recovery state machine (gap coalescing, replay floors, deadlines) is a **single-owner goroutine fed by channels** — the platform §VII design preference ("share memory by communicating"), eliminating lock ordering across the gap/replay/timer paths. `Close(ctx)`: cancel root → wait (ctx-bounded) → `sync.Once`-guarded sender-side close of `Messages()` → drain.
- **Why**: this is the direct Go analog of sukko-py's per-epoch `asyncio.TaskGroup` (structured concurrency: nothing outlives its scope), and it's what makes "no goroutine leaks" assertable — `goleak` after every lifecycle test replaces Python's "no orphaned tasks / Task was destroyed" checks. Prior art: Alpaca's reconnect loop and centrifuge-go's internal goroutines follow the same supervisor shape.

## D5 — Determinism + race: both, not either

- **Decision**: `go test -race` mandatory everywhere (§VIII of the platform constitution, adopted verbatim) **and** the injectable `Clock`/`Rand` seams from sukko-py carry over for every timing path (backoff/jitter, heartbeat, pong timeout, 30s refresh floor, 10s/channel replay floor, 10s recovery deadline).
- **Why**: the Python spec mandated deterministic time *as the substitute* for a race detector Python lacks. Go has the real detector — but the detector finds data races, not timing-logic bugs; a jitter or coalescing-window test that really sleeps is slow and flaky regardless of `-race`. So the two mechanisms check disjoint failure classes and both are required (this is the §VII/§VIII inversion the spec's Resolved Decisions records). `goleak` (Uber, test-only dep) completes the triad for lifecycle leaks.

## D6 — Config: functional options + fail-fast constructor

- **Decision**: `NewClient(url string, opts ...Option) (*Client, error)`; options set an internal config struct; the constructor validates everything (URL, credentials, and the carried-over invariant `QueueSize ≥ HistoryLimit + MaxReplayMessages(100)`) and returns an error — never a half-configured client.
- **Why**: functional options are the dominant Go SDK convention (NATS `nats.Connect(url, opts...)`, Alpaca `stream.NewStocksClient(feed, opts...)`, coder/websocket `DialOptions`) and keep the zero-value path short while making every knob discoverable. A raw exported `Config` struct was rejected: it can't distinguish "unset" from zero, which breaks layered defaults for numeric knobs. Fail-fast validation mirrors platform §I (invalid config = immediate failure, no silent degradation).

## D7 — Errors: sentinels + typed structs, `errors.Is/As`, redaction

- **Decision**: sentinel `Err…` values for conditions callers branch on; typed structs carrying the contract's machine codes for conditions callers inspect; everything wrapped `fmt.Errorf("op: %w", err)`. Credential redaction (§IX) applied at the wrap boundary for JWT/API-key **and** push subscriber keys (`p256dh_key`/`auth_secret`), because Go's `*url.Error` embeds the full request URL — the query-param auth opt-in would otherwise leak tokens into error strings and logs, exactly the trap sukko-py found in `httpx`/`websockets`.
- **Why**: `errors.Is/As` is the Go-native translation of Python's exception hierarchy; a `*CloseError{Code, Direction}` carrying **close direction** is the mitigation for the 4000-overlap drift (filing #1) — same solution sukko-py shipped via `ConnectionClosed.rcvd/.sent`.

## D8 — Module floor Go 1.23; matrix to current

- **Decision**: `go 1.23` directive; CI matrix 1.23/1.24/1.25/1.26.
- **Why**: 1.23 is the newest version any required feature needs — `iter.Seq`/range-over-func (1.23) is the binding constraint; `log/slog` (1.21) and generics (1.18) come free. The platform repo's Go 1.26 floor is a server prerogative; a client SDK maximizes adoption breadth. Two-releases-back is Go's own support window; 1.23 gives adopters a full extra year of slack while still unlocking the iterator surface.

## Carried-forward pins and drift (verified 2026-08-20)

- **AsyncAPI version**: v1.4.0 confirmed (`ws/docs/asyncapi/client-ws.asyncapi.yaml:5`).
- **`last_pos` keying**: tenant-prefixed — **inherited resolved pin** (sukko-py T010b, from server source: `handleReconnect` → `ChannelTopic` keyed by full channel); NOT reopened. The AsyncAPI's bare-suffix examples remain the drift (filing #4).
- **All five upstream filings still unfixed** against current contracts: #1 4000 overlap (asyncapi close-code table unchanged), #2 OpenAPI "default 1MB" at `gateway.openapi.yaml:320` (64KB authoritative), #3 no list-push-subscriptions endpoint, #4 bare-suffix `last_pos` examples at `client-ws.asyncapi.yaml:749`, #5 no `Retry-After` on any 429. Carried in the spec's Cross-Repo; nothing re-filed.

## Sources

- [NATS Docs — Slow Consumers](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers)
- [Synadia — Why Go NATS SubscribeSync Subscriptions Can Use Significant Memory](https://www.synadia.com/blog/go-nats-subscribesync-memory-use)
- [nats.go discussion #1084 — ChanQueueSubscribe slow consumer](https://github.com/nats-io/nats.go/discussions/1084)
- [centrifuge-go README (callback concurrency + deadlock warnings)](https://github.com/centrifugal/centrifuge-go)
- [polygonws package docs — `Output()`](https://pkg.go.dev/github.com/polygon-io/client-go/websocket)
- [Alpaca Go SDK — real-time streaming (buffered channel + bufferFillCallback)](https://deepwiki.com/alpacahq/alpaca-trade-api-go/3.6-real-time-streaming)
- [websocket.org — Go WebSocket guide: coder/websocket vs Gorilla](https://websocket.org/guides/languages/go/)
- [Go 1.23 iterators (`iter.Seq`) overviews](https://www.bytesizego.com/blog/go-123-iterators), [channels-vs-iterators comparison](https://github.com/ajnavarro/go-channels-vs-iterators), [why channels for streaming despite iterator speed](https://medium.com/@yangxin.sz.ch/why-i-chose-channels-over-iterators-for-streaming-llm-responses-in-go-55182de064de)
- `../sukko-py/specs/in-progress/feat/python-sdk/spec.md` + `research.md` (behavioral reference, D1–D6, T010b pin)
- `../sukko/ws/docs/asyncapi/client-ws.asyncapi.yaml` (v1.4.0), `../sukko/ws/docs/openapi/gateway.openapi.yaml`
