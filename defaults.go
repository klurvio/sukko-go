package sukko

import "time"

// This file holds the SDK's exported defaults and bounds. They are public API
// frozen from v0.1.0 and the single source for every call site (§I) — no
// literal timeouts or sizes anywhere else in the package.
//
// Several track a server-side setting, named in the comment on each. When a
// server default moves, the corresponding assertion in defaults_test.go is
// where that shows up.

// Capacity and size bounds.
const (
	// DefaultQueueSize is the delivery channel's capacity in events. It matches
	// the server's WS_CLIENT_SEND_BUFFER_SIZE and clears the construction floor
	// (DefaultHistoryLimit + MaxReplayMessages + AdvisoryHeadroom) with headroom,
	// which is what lets the zero-config quickstart construct without a capacity
	// knob.
	DefaultQueueSize = 512

	// SubscribeQueueDepth is the bound on the subscribe serializer's request queue.
	// Subscribe/Unsubscribe enqueue here and return immediately; a full queue
	// yields ErrSubscribeQueueFull. The serializer keeps one request outstanding at
	// a time (the wire's subscription_ack carries no correlation id), so this bounds
	// how many caller requests may await their turn.
	SubscribeQueueDepth = 64

	// DefaultHistoryLimit is the default number of records a History call
	// requests. It tracks the server's WS_HISTORY_MAX_LIMIT *default*.
	DefaultHistoryLimit = 100

	// MaxHistoryLimit is the validation ceiling for WithHistoryLimit — the
	// server's own validated upper bound for WS_HISTORY_MAX_LIMIT. Validation
	// deliberately uses this rather than DefaultHistoryLimit: a default is not a
	// maximum, and clamping to it would make a deployment that raised the server
	// limit unusable from this SDK.
	MaxHistoryLimit = 1000

	// MaxReplayMessages is the contract's bound on a single replay window. The
	// server enforces it, so it is not a client knob; the SDK uses it to size
	// the capacity floor.
	MaxReplayMessages = 100

	// AdvisoryHeadroom is the slice of the delivery channel reserved for
	// advisory and lifecycle events, so a saturating burst of data can never
	// starve the signals that explain it. One slot is reserved exclusively for
	// the final *Terminal event; the other 15 serve a bounded safety subset.
	// It is a constant, not a runtime quantity, because the floor it appears in
	// is validated at construction time.
	AdvisoryHeadroom = 16

	// DefaultMaxPublishSize bounds a published payload in bytes. It must match
	// the server's GATEWAY_MAX_PUBLISH_SIZE. It also derives the WebSocket
	// transport's read limit: the library's own 32 KiB default would close a
	// legal 64 KB inbound message with 1009.
	DefaultMaxPublishSize = 64 * 1024

	// DefaultMaxBackpressureReconnects is how many consecutive
	// back-pressure-induced reconnect cycles are tolerated before the client
	// terminates with ErrConsumerTooSlow. The count is consecutive and resets on
	// any healthy epoch, so a bursty-but-recovering consumer is not terminated.
	// It is an arbitrary-but-tunable backstop rather than an industry-derived
	// number — no surveyed real-time client supplies one — and
	// WithMaxBackpressureReconnects raises it.
	DefaultMaxBackpressureReconnects = 5

	// MinChannelParts is the minimum number of dot-separated parts in a channel:
	// "{tenant}.{suffix}" is at least two.
	MinChannelParts = 2

	// MaxTokenSourceAttempts is how many consecutive TokenSource failures are
	// tolerated on the connected refresh path before the client terminates with
	// ErrTokenSourceFailed. The count is consecutive and resets on any success. A
	// TokenSource failure DURING a reconnect is non-terminal (FR-005): the client
	// keeps backing off rather than dialing a credential it knows is dead.
	MaxTokenSourceAttempts = 5
)

// Timing defaults. Every one of these is driven through the injectable Clock,
// so tests advance them deterministically rather than sleeping.
const (
	// DefaultDialTimeout bounds a *reconnect* dial. It does not bound
	// Connect(ctx) — that call is bounded by the caller's own context.
	DefaultDialTimeout = 10 * time.Second

	// DefaultHeartbeatInterval is how often the client sends an app-level
	// heartbeat. It sits comfortably under the gateway's 60s
	// GATEWAY_IDLE_TIMEOUT so an idle-but-healthy connection is never reaped.
	DefaultHeartbeatInterval = 30 * time.Second

	// DefaultPongTimeout is how long the client waits for the pong reply before
	// closing locally with CloseCodeHeartbeatTimeout. It must stay below
	// DefaultHeartbeatInterval so a timeout is attributable to one heartbeat.
	DefaultPongTimeout = 10 * time.Second

	// DefaultRefreshMinInterval is the floor between auth sends, tracking the
	// gateway's GATEWAY_AUTH_REFRESH_RATE_INTERVAL. It is what stops an
	// auth_error from driving a refresh loop.
	DefaultRefreshMinInterval = 30 * time.Second

	// DefaultRefreshLead is how far ahead of a credential's expiry a proactive
	// refresh is armed. It is clamped so it never schedules inside
	// RefreshMinInterval.
	DefaultRefreshLead = 60 * time.Second

	// DefaultReplayFloor is the per-channel floor between live replay requests,
	// tracking the server's WS_REPLAY_RATE_LIMIT_INTERVAL. The automatic
	// gap-to-replay path honors it rather than provoking replay_rate_limited.
	DefaultReplayFloor = 10 * time.Second

	// DefaultRecoveryDeadline is how long the client waits for a recovery
	// terminator before surfacing a truncated recovery — 2× the server's
	// WS_REPLAY_TIMEOUT of 5s, for margin.
	DefaultRecoveryDeadline = 10 * time.Second

	// DefaultSSEIdleTimeout is how long an SSE stream may deliver no bytes at
	// all — neither a record nor a ": keepalive" comment — before the client
	// treats it as dead. It is 2× the gateway's 45s SSE_KEEPALIVE_INTERVAL.
	DefaultSSEIdleTimeout = 90 * time.Second

	// DefaultTokenSourceTimeout bounds a single TokenSource call. The callback
	// is caller code invoked from the SDK's own timers, so it is bounded rather
	// than trusted to return.
	DefaultTokenSourceTimeout = 10 * time.Second
)

// WebSocket close codes the SDK originates.
//
// Code 4000 is reused for two meanings disambiguated by DIRECTION, exactly as
// the AsyncAPI contract's close-code table and both sibling SDKs (@sukko/sdk,
// sukko-py) define it: a REMOTE 4000 is the operator's force-disconnect, a LOCAL
// 4000 is the client's own heartbeat-timeout close. Numeric parity with the
// contract wins over any private allocation scheme (§I); the close-policy table
// keeps the two 4000 rows apart by direction (§XVIII: matches the siblings).
const (
	// CloseCodeHeartbeatTimeout is sent (LOCAL) when no pong arrives within
	// PongTimeout. It shares its value (4000) with the server's REMOTE
	// force-disconnect and is told apart from it by close direction — the
	// contract's rule, mirrored by @sukko/sdk and sukko-py.
	CloseCodeHeartbeatTimeout = 4000
)

// BackoffConfig parameterises reconnect backoff. Attempt n (n = 0 for the first
// reconnect after an epoch ends) waits:
//
//	base  = min(Initial × Multiplier^n, Max)
//	delay = base × (1 + Jitter × (2r − 1))    clamped to [0, Max]
//
// where r is exactly one Rand.Float64() draw in [0,1) taken after base is
// computed — one draw per attempt, and none at all when Jitter is zero. The
// formula is pinned this precisely so a seeded Rand yields an assertable delay
// sequence; "exponential with jitter" alone cannot be tested.
//
// The attempt counter resets on a *successful* re-handshake, never on a failed
// dial, so a flapping connection keeps backing off.
type BackoffConfig struct {
	// Initial is the base delay for the first reconnect attempt.
	Initial time.Duration
	// Max caps both the exponential base and the jittered result.
	Max time.Duration
	// Multiplier is the exponential factor applied per attempt; it must exceed 1.
	Multiplier float64
	// Jitter is the proportional spread applied to the base delay, in [0,1].
	// Zero disables jitter entirely and draws no randomness.
	Jitter float64
}

// DefaultBackoff is the reconnect backoff applied when WithBackoff is not given.
var DefaultBackoff = BackoffConfig{
	Initial:    500 * time.Millisecond,
	Max:        30 * time.Second,
	Multiplier: 2.0,
	Jitter:     0.2,
}
