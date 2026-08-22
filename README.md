# sukko-go

Go client SDK for the [Sukko](https://github.com/klurvio/sukko) real-time platform — real-time
market-data / event subscription for backend services, data pipelines, and edge consumers.

- **Channel + iterator delivery** with inherent back-pressure — a bounded stream, no unbounded
  buffering, no silent drops.
- **Full gap recovery** — reconnect-with-replay, advisory `gap` → live `replay`, and `history`.
- **Automatic single-flight auth** — proactive + reactive token refresh, API-key → JWT escalation.
- **WebSocket + SSE** transports, **REST publish**, **push** subscription management.
- **`context.Context`-driven** lifecycle, typed errors (`errors.Is`/`errors.As`), pull-based `Stats()`.
- Built to the AsyncAPI v1.4.0 + gateway OpenAPI contracts.

> 🚧 **Pre-release.** The API shown below is the target design; the implementation is in progress.
> `go get` claims the module path, and symbols land as the SDK is built — so expect the surface to be
> incomplete until `v0.1.0` is tagged. Design rationale lives in the package documentation and in the
> commit history; the behavioural contract is the [AsyncAPI](https://github.com/klurvio/sukko) client
> protocol, which this SDK follows rather than any sibling SDK's current behaviour.

```bash
go get github.com/klurvio/sukko-go
```

Requires Go 1.23+.

## Quickstart

```go
package main

import (
	"context"
	"log"

	sukko "github.com/klurvio/sukko-go"
)

type Trade struct {
	Price float64 `json:"price"`
}

func main() {
	ctx := context.Background()

	client, err := sukko.NewClient("wss://gateway.example.com/ws", sukko.WithToken("<jwt>"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	if err := client.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if err := client.Subscribe(ctx, []string{"acme.trades"}); err != nil {
		log.Fatal(err)
	}

	for event := range client.Messages() { // live + recovered messages arrive here
		msg, ok := event.(*sukko.Message)
		if !ok {
			continue // advisory events (e.g. *sukko.PossibleGap) ride the same stream
		}
		trade, err := sukko.DataAs[Trade](msg) // typed payload decode
		if err != nil {
			log.Printf("decode %s: %v", msg.Channel, err)
			continue
		}
		log.Printf("%s %+v", msg.Channel, trade)
	}
}
```

Prefer a range-over-func iterator? `for event := range client.Iter(ctx) { ... }` is equivalent and
cancels with the context.

## Publishing

```go
client.Publish(ctx, "acme.trades", data)     // over the WS connection (fire-and-forget)
client.RESTPublish(ctx, "acme.trades", data) // REST — no WS needed (Pro)
```

## Auth

Credentials are sent via **request headers by default** (`Authorization: Bearer` / `X-API-Key`);
query-param auth is opt-in. Long-lived tokens are refreshed automatically (supply a token source). An
API-key connection can **escalate** to a JWT mid-session, re-subscribing the newly-permitted channels.

## Channels

Channels are `"{tenant}.{suffix}"` (the tenant is the segment before the first dot; the suffix is an
opaque dotted remainder).

```go
sukko.BuildChannel("acme", "trades.btc") // -> "acme.trades.btc"
sukko.ParseChannel("acme.trades.btc")    // -> {Tenant: "acme", Suffix: "trades.btc"}
```

## Recovery

On a Kafka backend the SDK recovers missed messages automatically: it reconnects with exponential
backoff + jitter and replays from the last position, turning advisory `gap` notices into live
`replay`s — all surfaced through the same `Messages()` stream. The delivery guarantee is
**at-least-once within the replay window, best-effort beyond**. On the Direct backend (no positions),
a disconnect surfaces a `*PossibleGap` event per channel so data loss is never silent.

## Errors

Every failure is a typed Go error — sentinels matchable with `errors.Is` (`ErrNotConnected`,
`ErrEditionRequired`, …) and typed structs via `errors.As` (`*RecoveryInterruptedError`,
`*PayloadDecodeError`, `*CloseError`, …). Credentials never appear in an error string or log record.

## License

MIT — see [LICENSE](LICENSE).
