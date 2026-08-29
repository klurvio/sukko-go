package sukko

import "encoding/json"

// Event is the sealed union delivered on Messages().
//
// It is an interface with an unexported marker method, so the member set is
// closed to this package: a caller's `switch ev := ev.(type)` runs against a
// known roster (FR-001a's dispatch table), and no external type can join it. The
// dominant member is *Message; everything else is an advisory or lifecycle
// signal that flows in-band, in receive order, on the same channel — so a
// *PossibleGap arrives exactly where the potential loss sits relative to the
// data, rather than on a second channel whose overflow policy could drop the
// very signals that exist to prevent silent drops.
//
// Several members are also errors (the typed structs of FR-010): one
// `case *ReplayError:` yields a value errors.As matches, with no parallel
// wrapper hierarchy.
type Event interface {
	// isEvent seals the union. It is unexported so only this package's types
	// can implement it; it has no behavior.
	isEvent()
}

// ─── data ───

// Message is a delivered record — the union's dominant member.
//
// Source discriminates where the record came from: SourceLive for a normal
// delivery, SourceHistory for a record from a history response, SourceReplay for
// one delivered inside a reconnect-replay window. There is no separate replayed
// type and no History bool; the discriminator carries that distinction, so a
// caller handles one shape.
//
// Data is the raw payload, left undecoded on the hot path. DataAs[T] decodes it
// into a caller type; Pos is an opaque cursor, stored and echoed but never
// parsed or compared.
//
// Mid is the stable message identity: the same value on every copy of a message
// — live, history, and replay — so a caller can deduplicate a record it already
// saw (for example a reconnect-replay overlap) or key idempotent processing on
// it. Opaque like Pos, but distinct in kind: Pos anchors recovery, Mid names the
// message. Empty on a server that predates the field.
type Message struct {
	Channel string
	Seq     int64
	TS      int64
	Pos     string
	Mid     string
	Data    json.RawMessage
	Source  MessageSource
}

func (*Message) isEvent() {}

// ─── recovery signals ───

// Gap reports that records were missed on a channel, per the server's own gap
// frame. LastPos may be empty on a backend that keeps no cursors — a real
// data-loss signal that cannot anchor a replay, which the recovery engine
// handles by pairing it with a PossibleGap rather than issuing replay{from_pos:""}.
type Gap struct {
	Channel string
	FromSeq int64
	ToSeq   int64
	LastPos string
	TS      int64
}

func (*Gap) isEvent() {}

// PossibleGap reports channels where loss may have occurred and no recovery is
// possible, because the SDK holds no cursor to replay from.
//
// It is emitted once per reconnect carrying every such granted channel — one
// signal, not one per channel, so a caller iterates a slice rather than
// correlating N events. It covers the whole Direct backend (no cursor ever
// arrives), a Kafka channel that saw no live message before the drop, an
// un-anchorable gap (empty last_pos), and every SSE reconnect. It is not emitted
// when that set is empty.
type PossibleGap struct {
	Channels []string
}

func (*PossibleGap) isEvent() {}

// ─── subscription lifecycle ───

// SubscriptionResult reports the outcome of a subscribe as a grant diff.
//
// The gateway silently drops unauthorized channels, so NotGranted — the
// requested channels absent from Granted — is the only place a refusal is
// visible. Retaining that denied set is what the escalation delta re-subscribes.
type SubscriptionResult struct {
	Requested  []string
	Granted    []string
	NotGranted []string
	Count      int
}

func (*SubscriptionResult) isEvent() {}

// Unsubscribed reports removed channels.
//
// Count is a pointer so a present zero ("you now have none") is distinct from
// absent (the server omits it on a forced unsubscribe). Forced marks an
// unsolicited server-initiated removal — a credential downgrade, say — which the
// caller did not ask for and must not have resolve a pending request of its own.
type Unsubscribed struct {
	Channels []string
	Count    *int
	Forced   bool
}

func (*Unsubscribed) isEvent() {}

// ─── publish / auth / recovery outcomes ───

// PublishAccepted reports that a publish was accepted. It names the channel from
// the ack, so a caller with several publishes in flight can attribute it.
//
// Mid is the stable message identity the gateway assigned to the accepted
// message — the same value subscribers receive on Message.Mid, so a publisher
// can correlate its publish with the delivered copy (this is the WS-path twin of
// PublishResult.Mid). Empty for a multi-topic fan-out publish or a gateway that
// predates the field.
type PublishAccepted struct {
	Channel string
	Mid     string
}

func (*PublishAccepted) isEvent() {}

// Resumed reports a completed reconnect-replay window and how many records it
// replayed — which may legitimately be zero on a cursorless backend, a success
// rather than a failure.
type Resumed struct {
	MessagesReplayed int
}

func (*Resumed) isEvent() {}

// Authenticated reports a completed credential operation.
//
// Mode distinguishes a refresh from an escalation — the SDK's own owned state,
// never inferred from the frame. Exp is the new expiry as a unix timestamp; zero
// means the credential does not expire.
type Authenticated struct {
	Exp  int64
	Mode AuthMode
}

func (*Authenticated) isEvent() {}

// HistoryComplete terminates a history response. Source reports where the
// records came from; Truncated is true when the response was cut short.
type HistoryComplete struct {
	Channel   string
	Count     int
	Source    HistorySource
	Truncated bool
}

func (*HistoryComplete) isEvent() {}

// ReplayComplete terminates a replay response. MessagesReplayed may be zero;
// Truncated is true when delivery was cut short.
type ReplayComplete struct {
	Channel          string
	MessagesReplayed int
	Truncated        bool
}

func (*ReplayComplete) isEvent() {}

// ─── connection lifecycle ───

// StateChange reports a connection-state transition, per FR-009's state machine.
type StateChange struct {
	From ConnectionState
	To   ConnectionState
}

func (*StateChange) isEvent() {}

// Terminal is the final event on every terminal path, delivered immediately
// before Messages() closes.
//
// Err carries the termination cause and is nil only for a caller-initiated stop
// (Close or lifetime-context cancellation). It is delivered by a non-blocking
// send into a channel slot reserved for it alone, after every other sender has
// exited — so it lands even on a full channel with a hung consumer, and cannot
// be starved, discarded, or raced. Client.Err() reads nil for both clean-stop
// classes; the two differ on the WithReconnect(false) path, where Terminal.Err
// carries the close cause while Err() stays nil.
type Terminal struct {
	Err error
}

func (*Terminal) isEvent() {}

// UnknownEvent carries a server frame whose type the SDK does not recognize.
//
// It is surfaced rather than dropped so a server newer than its clients does not
// go silently unheard — the forward-compatibility rule that lets an old client
// talk to a new server. Raw is the frame verbatim.
type UnknownEvent struct {
	Type string
	Raw  json.RawMessage
}

func (*UnknownEvent) isEvent() {}

// ─── error-shaped members ───
//
// These are the typed error structs of FR-010, defined in errors.go. Adding the
// marker here — rather than beside each struct — keeps the union's membership
// legible in one place: this file is the roster, errors.go is the error
// behavior. Each still implements error there.

func (*AuthError) isEvent()                {}
func (*PublishError) isEvent()             {}
func (*HistoryError) isEvent()             {}
func (*ReplayError) isEvent()              {}
func (*ReconnectError) isEvent()           {}
func (*ProtocolError) isEvent()            {}
func (*RecoveryInterruptedError) isEvent() {}
func (*CloseError) isEvent()               {}
func (*HandshakeError) isEvent()           {}
func (*EditionRequiredError) isEvent()     {}
func (*InternalError) isEvent()            {}
func (*TokenSourceError) isEvent()         {}
