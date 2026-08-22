package sukko

// This file holds the SDK's enumerated types and its exported default constants.
//
// Every enum is a string-backed named type whose underlying value is its wire or
// log form. String backing is deliberate: several of these are contract values
// (PushPlatform mirrors the gateway's closed push-subscribe enum, HistorySource
// mirrors `history_complete.source`, MessageSource is carried on delivered
// messages), so the type encodes and decodes as the right JSON string with no
// conversion table to drift. The zero value is the empty string, which is not a
// valid member of any of these sets — an unset field is recognisable rather than
// silently valid — with ConnectionState the one exception, noted below.

// ConnectionState is the client's observable lifecycle state, reported by
// Client.State and carried on in-band state-change events.
//
// The six values form a closed set. Two are resting states a client can
// terminate in: StateClosed after a clean stop (Close, lifetime-ctx
// cancellation, or a reconnect-class outcome with reconnect disabled), and
// StateError after a terminal failure. They are distinguished by Client.Err,
// which is nil for a clean stop.
type ConnectionState string

const (
	// StateDisconnected is the initial state after NewClient, before Connect.
	// It is not re-entered once an epoch has ended.
	StateDisconnected ConnectionState = "disconnected"
	// StateConnecting is set when Connect is called, while the first dial and
	// handshake are in flight.
	StateConnecting ConnectionState = "connecting"
	// StateConnected is set once the handshake succeeds and delivery is live.
	StateConnected ConnectionState = "connected"
	// StateReconnecting is set while the supervisor is backing off and
	// re-dialling after a reconnect-class epoch termination.
	StateReconnecting ConnectionState = "reconnecting"
	// StateError is the terminal state for a failure the SDK will not retry.
	// Client.Err is non-nil in this state.
	StateError ConnectionState = "error"
	// StateClosed is the terminal state for a clean stop. Client.Err is nil in
	// this state, which is what distinguishes it from StateError.
	StateClosed ConnectionState = "closed"
)

// String returns the state's stable lowercase form, e.g. "reconnecting".
func (s ConnectionState) String() string { return string(s) }

// MessageSource says how a delivered Message reached the caller. It replaces a
// boolean history flag so one field carries one meaning: a record is live,
// replayed from a gap or reconnect window, or part of a history response.
type MessageSource string

const (
	// SourceLive marks a message delivered from the live stream. Only live
	// messages advance the per-channel recovery cursor.
	SourceLive MessageSource = "live"
	// SourceHistory marks a message delivered as part of a history response,
	// terminated by a history-complete event.
	SourceHistory MessageSource = "history"
	// SourceReplay marks a message delivered inside a replay or reconnect-replay
	// window. Replayed records may duplicate live ones — the delivery guarantee
	// is at-least-once within the window — so de-duplication is the caller's.
	SourceReplay MessageSource = "replay"
)

// String returns the source's stable lowercase form, e.g. "replay".
func (s MessageSource) String() string { return string(s) }

// HistorySource reports where a completed history response was served from.
// The values mirror the contract's `history_complete.source` enum.
type HistorySource string

const (
	// HistorySourceCache means the response was served entirely from cache.
	HistorySourceCache HistorySource = "cache"
	// HistorySourceKafka means the response was served entirely from Kafka.
	HistorySourceKafka HistorySource = "kafka"
	// HistorySourceMixed means the response combined cached and Kafka-sourced records.
	HistorySourceMixed HistorySource = "mixed"
)

// String returns the source's stable lowercase form, e.g. "mixed".
func (s HistorySource) String() string { return string(s) }

// Edition names a licensed platform edition. The SDK sets it from its own
// knowledge of which gate an operation crossed, never from a response body:
// REST publish and SSE require Pro, push requires Enterprise.
type Edition string

const (
	// EditionPro is required for REST publish and the SSE transport.
	EditionPro Edition = "pro"
	// EditionEnterprise is required for push subscription management.
	EditionEnterprise Edition = "enterprise"
)

// String returns the edition's stable lowercase form, e.g. "enterprise".
func (e Edition) String() string { return string(e) }

// AuthMode distinguishes the two purposes the wire's single auth message
// serves. The SDK chooses the mode from credential state it owns rather than
// inferring it at runtime, so one value never means two things.
type AuthMode string

const (
	// AuthRefresh replaces an existing credential with a fresh one. Granted
	// subscriptions are preserved.
	AuthRefresh AuthMode = "refresh"
	// AuthEscalation supplies a JWT on an API-key connection, widening
	// permissions. Unlike a refresh it re-subscribes the newly-permitted delta.
	AuthEscalation AuthMode = "escalation"
)

// String returns the mode's stable lowercase form, e.g. "escalation".
func (m AuthMode) String() string { return string(m) }

// PushPlatform names the delivery platform of a push subscription. The values
// are contract values — the gateway's push-subscribe endpoint declares a closed
// enum — so a named type keeps a typo a compile error rather than a 400.
type PushPlatform string

const (
	// PlatformWeb is a Web Push subscription, identified by endpoint and keys.
	PlatformWeb PushPlatform = "web"
	// PlatformAndroid is an FCM subscription, identified by a device token.
	PlatformAndroid PushPlatform = "android"
	// PlatformIOS is an APNs subscription, identified by a device token.
	PlatformIOS PushPlatform = "ios"
)

// String returns the platform's wire form, e.g. "android".
func (p PushPlatform) String() string { return string(p) }

// TransportKind selects the transport a Client uses. The choice is explicit:
// the SDK never infers a transport from the URL scheme.
type TransportKind string

const (
	// TransportWebSocket is the bidirectional WebSocket transport, and the
	// default NewClient applies when no transport option is given.
	TransportWebSocket TransportKind = "websocket"
	// TransportSSE is the receive-only SSE transport: it subscribes via
	// connect-time channels, publishes over REST, and cannot subscribe live.
	TransportSSE TransportKind = "sse"
)

// String returns the transport's stable lowercase form, e.g. "websocket".
func (k TransportKind) String() string { return string(k) }
