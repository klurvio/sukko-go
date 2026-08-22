package sukko

// This file holds the SDK's enumerated types. Their string forms are public API
// from v0.1.0, and several are contract values: PushPlatform mirrors the gateway's
// closed push-subscribe enum, and HistorySource mirrors `history_complete.source`.
// Each enum is an int-backed named type with an explicit invalid zero, so an unset
// field is recognisable rather than silently valid; ConnectionState is the one
// exception, because a client genuinely starts disconnected.

// ConnectionState is the client's observable lifecycle state, reported by
// Client.State and carried on in-band state-change events.
//
// The six values form a closed set. Two are resting states a client can
// terminate in: StateClosed after a clean stop (Close, or a reconnect-class
// outcome with reconnect disabled), and StateError after a terminal failure.
// They are distinguished by Client.Err, which is nil for a clean stop.
type ConnectionState int

const (
	// StateDisconnected is the initial state after NewClient, before Connect.
	// It is the zero value because a client legitimately starts here; it is not
	// re-entered once an epoch has ended.
	StateDisconnected ConnectionState = iota
	// StateConnecting is set when Connect is called, while the first dial and
	// handshake are in flight.
	StateConnecting
	// StateConnected is set once the handshake succeeds and delivery is live.
	StateConnected
	// StateReconnecting is set while the supervisor is backing off and
	// re-dialling after a reconnect-class epoch termination.
	StateReconnecting
	// StateError is the terminal state for a failure the SDK will not retry.
	// Client.Err is non-nil in this state.
	StateError
	// StateClosed is the terminal state for a clean stop — Close, lifetime-ctx
	// cancellation, or a reconnect-class outcome with reconnect disabled.
	// Client.Err is nil in this state.
	StateClosed
)

// String returns the state's stable lowercase form, e.g. "reconnecting".
func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateError:
		return "error"
	case StateClosed:
		return "closed"
	default:
		return unknownEnum
	}
}

// MessageSource says how a delivered Message reached the caller. It replaces a
// boolean history flag so one field carries one meaning: a record is live,
// replayed from a gap or reconnect window, or part of a history response.
type MessageSource int

const (
	// The zero value is deliberately invalid — a decoded Message always carries
	// a real source, so an unset field must not masquerade as a valid one.
	_ MessageSource = iota
	// SourceLive marks a message delivered from the live stream. Only live
	// messages advance the per-channel recovery cursor.
	SourceLive
	// SourceHistory marks a message delivered as part of a history response,
	// terminated by a history-complete event.
	SourceHistory
	// SourceReplay marks a message delivered inside a replay or reconnect-replay
	// window. Replayed records may duplicate live ones — the delivery guarantee
	// is at-least-once within the window — so de-duplication is the caller's.
	SourceReplay
)

// String returns the source's stable lowercase form, e.g. "replay".
func (s MessageSource) String() string {
	switch s {
	case SourceLive:
		return "live"
	case SourceHistory:
		return "history"
	case SourceReplay:
		return "replay"
	default:
		return unknownEnum
	}
}

// HistorySource reports where a completed history response was served from.
// The values mirror the contract's `history_complete.source` enum.
type HistorySource int

const (
	// The zero value is deliberately invalid — the contract marks source
	// required, so a decoded history-complete always carries one.
	_ HistorySource = iota
	// HistorySourceCache means the response was served entirely from cache.
	HistorySourceCache
	// HistorySourceKafka means the response was served entirely from Kafka.
	HistorySourceKafka
	// HistorySourceMixed means the response combined cached and Kafka-sourced records.
	HistorySourceMixed
)

// String returns the source's stable lowercase form, e.g. "mixed".
func (s HistorySource) String() string {
	switch s {
	case HistorySourceCache:
		return "cache"
	case HistorySourceKafka:
		return "kafka"
	case HistorySourceMixed:
		return "mixed"
	default:
		return unknownEnum
	}
}

// Edition names a licensed platform edition. The SDK sets it from its own
// knowledge of which gate an operation crossed, never from a response body:
// REST publish and SSE require Pro, push requires Enterprise.
type Edition int

const (
	// The zero value is deliberately invalid — an edition error always names a
	// real edition, set from the gate the operation crossed.
	_ Edition = iota
	// EditionPro is required for REST publish and the SSE transport.
	EditionPro
	// EditionEnterprise is required for push subscription management.
	EditionEnterprise
)

// String returns the edition's stable lowercase form, e.g. "enterprise".
func (e Edition) String() string {
	switch e {
	case EditionPro:
		return "pro"
	case EditionEnterprise:
		return "enterprise"
	default:
		return unknownEnum
	}
}

// AuthMode distinguishes the two purposes the wire's single auth message
// serves. The SDK chooses the mode from credential state it owns rather than
// inferring it at runtime, so one value never means two things.
type AuthMode int

const (
	// The zero value is deliberately invalid — an auth exchange always has a mode.
	_ AuthMode = iota
	// AuthRefresh replaces an existing credential with a fresh one. Granted
	// subscriptions are preserved.
	AuthRefresh
	// AuthEscalation supplies a JWT on an API-key connection, widening
	// permissions. Unlike a refresh it re-subscribes the newly-permitted delta.
	AuthEscalation
)

// String returns the mode's stable lowercase form, e.g. "escalation".
func (m AuthMode) String() string {
	switch m {
	case AuthRefresh:
		return "refresh"
	case AuthEscalation:
		return "escalation"
	default:
		return unknownEnum
	}
}

// PushPlatform names the delivery platform of a push subscription. The values
// are contract values — the gateway's push-subscribe endpoint declares a closed
// enum — so a named type keeps a typo a compile error rather than a 400.
type PushPlatform int

const (
	// The zero value is deliberately invalid — a push subscription always names
	// a platform, and the contract's enum is closed.
	_ PushPlatform = iota
	// PlatformWeb is a Web Push subscription, identified by endpoint and keys.
	PlatformWeb
	// PlatformAndroid is an FCM subscription, identified by a device token.
	PlatformAndroid
	// PlatformIOS is an APNs subscription, identified by a device token.
	PlatformIOS
)

// String returns the platform's wire form, e.g. "android".
func (p PushPlatform) String() string {
	switch p {
	case PlatformWeb:
		return "web"
	case PlatformAndroid:
		return "android"
	case PlatformIOS:
		return "ios"
	default:
		return unknownEnum
	}
}

// TransportKind selects the transport a Client uses. The choice is explicit:
// the SDK never infers a transport from the URL scheme.
type TransportKind int

const (
	// The zero value is deliberately invalid. NewClient applies
	// TransportWebSocket when no transport option is given; an invalid zero keeps
	// an explicitly mis-set field distinguishable from an unset one.
	_ TransportKind = iota
	// TransportWebSocket is the bidirectional WebSocket transport and the default.
	TransportWebSocket
	// TransportSSE is the receive-only SSE transport: it subscribes via
	// connect-time channels, publishes over REST, and cannot subscribe live.
	TransportSSE
)

// String returns the transport's stable lowercase form, e.g. "websocket".
func (k TransportKind) String() string {
	switch k {
	case TransportWebSocket:
		return "websocket"
	case TransportSSE:
		return "sse"
	default:
		return unknownEnum
	}
}

// unknownEnum is the form every enum degrades to for its invalid zero or an
// out-of-range value. String is reached from log lines and error messages on
// failure paths, so it must stay total — never panic, never index past a table.
const unknownEnum = "unknown"
