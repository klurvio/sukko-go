package sukko

import (
	"encoding/json"
	"errors"
	"fmt"
)

// The wire layer: the message types the contract defines, and the decoder that
// turns bytes into them.
//
// These structs are hand-written rather than generated. The contract has quirks
// a generator fights — an absent field that must not become a zero, a required
// field that legitimately arrives empty, two message types that normalize onto
// one public shape — and the AsyncAPI-to-Go toolchain is not mature enough to
// express them. They are also unexported: the wire shape and the public event
// shape evolve on different schedules, and coupling them would make every
// contract addition a breaking change for callers.
//
// Field modeling follows one rule: a pointer where an absent value means
// something different from the zero value, a plain type where it does not. The
// cases that matter are called out individually — each is a place where getting
// it wrong produces a plausible-looking value that is quietly false.

// Wire type constants. Every type string the contract defines appears exactly
// once, here, and is referenced everywhere else (§I).
const (
	// client → server
	typeSubscribe   = "subscribe"
	typeUnsubscribe = "unsubscribe"
	typePublish     = "publish"
	typeReconnect   = "reconnect"
	typeHeartbeat   = "heartbeat"
	typeAuth        = "auth"
	typeHistory     = "history"
	typeReplay      = "replay"

	// server → client
	typeMessage           = "message"
	typeReplayMessage     = "replay_message"
	typeGap               = "gap"
	typeSubscriptionAck   = "subscription_ack"
	typeUnsubscriptionAck = "unsubscription_ack"
	typePublishAck        = "publish_ack"
	typePublishError      = "publish_error"
	typeReconnectAck      = "reconnect_ack"
	typeReconnectError    = "reconnect_error"
	typePong              = "pong"
	typeError             = "error"
	typeSubscribeError    = "subscribe_error"
	typeUnsubscribeError  = "unsubscribe_error"
	typeAuthAck           = "auth_ack"
	typeAuthError         = "auth_error"
	typeHistoryComplete   = "history_complete"
	typeHistoryError      = "history_error"
	typeReplayComplete    = "replay_complete"
)

// ─── client → server ───

// Every client→server frame is an envelope: a `type` naming the operation and a
// `data` object carrying its payload. The server parses that envelope first
// (its ClientMessage type) and only then unmarshals `data` against the
// operation's own schema — so a flat frame does not fail validation, it fails to
// parse, and comes back as `invalid_request` with nothing to point at.
//
// `heartbeat` is the sole exception, and only because the contract marks its
// `data` optional rather than required.

type wireSubscribe struct {
	Type string           `json:"type"`
	Data subscribePayload `json:"data"`
}

// subscribePayload carries the multi-channel form. The contract's second mode
// (a single `channel` plus an inline `history` request) is deliberately not
// represented: History covers that capability with a simpler surface, and
// modeling both here would make `channels` and `channel` dual-purpose, which
// §IX forbids. The omission is recorded against the contract's own example in
// contract_examples_test.go rather than left implicit.
type subscribePayload struct {
	Channels []string `json:"channels"`
}

type wireUnsubscribe struct {
	Type string             `json:"type"`
	Data unsubscribePayload `json:"data"`
}

type unsubscribePayload struct {
	Channels []string `json:"channels"`
}

type wirePublish struct {
	Type string         `json:"type"`
	Data publishPayload `json:"data"`
}

// publishPayload nests the caller's own payload under a second `data`. The
// doubling is the contract's, not an accident of modeling: the outer `data` is
// the envelope the server routes on, the inner one is opaque application content
// the SDK never inspects.
type publishPayload struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type wireReconnect struct {
	Type string           `json:"type"`
	Data reconnectPayload `json:"data"`
}

// reconnectPayload resumes a session.
//
// LastPos is keyed by the full tenant-prefixed channel — a bare suffix key
// silently replays nothing, which is the worst possible outcome for a recovery
// request. The contract marks it required, and there is no omitempty, but the
// type alone cannot guarantee the field is well-formed: a nil map marshals as
// `null`, an initialized-but-empty one as `{}`. The server accepts both, so the
// obligation is the caller's — the client that builds this frame must construct
// the map (empty, not nil) to state "I have nothing to resume from" rather than
// leave the server to read a null. That construction lands with the client in
// Phase 3; here the field is only carried.
type reconnectPayload struct {
	ClientID string            `json:"client_id"`
	LastPos  map[string]string `json:"last_pos"`
}

// wireHeartbeat carries no payload. The contract lists only `type` as required
// for this frame and defines `data` as an empty object, so the minimal form is
// valid — this is the one send frame where omitting `data` is correct.
type wireHeartbeat struct {
	Type string `json:"type"`
}

type wireAuth struct {
	Type string      `json:"type"`
	Data authPayload `json:"data"`
}

type authPayload struct {
	Token string `json:"token"`
}

type wireHistory struct {
	Type string         `json:"type"`
	Data historyPayload `json:"data"`
}

type historyPayload struct {
	Channel string `json:"channel"`
	Limit   int    `json:"limit"`
}

type wireReplay struct {
	Type string        `json:"type"`
	Data replayPayload `json:"data"`
}

type replayPayload struct {
	Channel string `json:"channel"`
	FromPos string `json:"from_pos"`
}

// ─── server → client ───

// wireMessage is a delivered record.
//
// Pos is a plain string with omitempty, and the distinction between absent and
// empty is handled by never storing an empty cursor rather than by a pointer:
// the SDK's rule is that an empty cursor anchors nothing, so "" and absent lead
// to the same behavior. History marks a record as part of a history response;
// its absence decodes to false, which is correct — a record that says nothing
// about history is not a history record.
type wireMessage struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel"`
	Seq     int64           `json:"seq"`
	TS      int64           `json:"ts"`
	Pos     string          `json:"pos,omitempty"`
	History bool            `json:"history,omitempty"`
	Mid     string          `json:"mid,omitempty"`
	Data    json.RawMessage `json:"data"`
}

// wireReplayMessage is structurally a message delivered inside a replay window.
// It normalizes onto the same public type with a different Source, so a caller
// handles one shape rather than two that differ only in provenance.
type wireReplayMessage struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel"`
	Seq     int64           `json:"seq"`
	TS      int64           `json:"ts"`
	Pos     string          `json:"pos,omitempty"`
	Mid     string          `json:"mid,omitempty"`
	Data    json.RawMessage `json:"data"`
}

// wireGap advises that records were missed.
//
// LastPos is required by the contract and the server always sends it — but on a
// backend that keeps no cursors it arrives empty. That is not a malformed frame,
// and treating it as one would discard a real data-loss signal; it means
// "records were lost and there is nothing to replay from".
type wireGap struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	LastPos string `json:"last_pos"`
	FromSeq int64  `json:"from_seq"`
	ToSeq   int64  `json:"to_seq"`
	TS      int64  `json:"ts"`
}

// wireSubscriptionAck reports which channels were granted. Channels the server
// filtered are simply absent — there is no denial list — so the grant diff is
// the only place a refusal is visible.
type wireSubscriptionAck struct {
	Type       string   `json:"type"`
	Subscribed []string `json:"subscribed"`
	Count      int      `json:"count"`
}

// wireUnsubscriptionAck reports removed channels.
//
// Count is a pointer because a present zero is legal and distinct from absent:
// the server omits it on a forced unsubscribe, while zero means "you now have
// none". Forced marks a server-initiated removal, as follows a credential
// downgrade — the caller did not ask, and needs to know.
type wireUnsubscriptionAck struct {
	Type         string   `json:"type"`
	Unsubscribed []string `json:"unsubscribed"`
	Count        *int     `json:"count,omitempty"`
	Forced       bool     `json:"forced,omitempty"`
}

type wirePublishAck struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Channel string `json:"channel"`
	Mid     string `json:"mid,omitempty"`
}

// wirePublishError carries no channel, so a publish failure cannot be attributed
// to one of several in-flight publishes. That is a contract limitation the SDK
// surfaces rather than papering over with a guess.
type wirePublishError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// wireReconnectAck accepts a resume.
//
// A cursorless backend answers with status "completed" and zero replayed — that
// is success, not an error, and a client waiting for a failure to tell it
// recovery was unavailable would wait forever while losing the gap silently.
type wireReconnectAck struct {
	Type             string `json:"type"`
	Status           string `json:"status"`
	MessagesReplayed int    `json:"messages_replayed"`
	Message          string `json:"message"`
}

type wireReconnectError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type wirePong struct {
	Type string `json:"type"`
	TS   int64  `json:"ts"`
}

// wireError is the contract's generic error frame, overloaded for two unrelated
// purposes: connection-level failures and per-channel replay rejections. The
// only disambiguator the contract offers is the presence of Channel, which is
// what the dispatch below keys on — a stateless rule, rather than inferring the
// meaning from whether a replay happens to be in flight.
type wireError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Channel string `json:"channel,omitempty"`
	Message string `json:"message"`
}

// wireSubscribeError reports a rejected subscribe. It carries no channel: the
// contract defines only type, code and message, so a failed multi-channel
// subscribe cannot be attributed to one of the requested channels. That is a
// contract limitation the SDK surfaces rather than papering over with a guess.
type wireSubscribeError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type wireUnsubscribeError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// wireAuthAck accepts a credential.
//
// This and wireAuthError are the only two server→client frames the contract
// nests under `data`; every other receive type is flat. The asymmetry is the
// contract's, and it is load-bearing: read flat, Exp decodes to zero, which the
// SDK defines below as "never expires" — so a nested frame read flat would
// disarm the refresh scheduler on every successful refresh and kill the
// connection at the real expiry. A wrong shape here does not fail loudly; it
// produces a plausible value with the opposite meaning.
type wireAuthAck struct {
	Type string      `json:"type"`
	Data authAckData `json:"data"`
}

// authAckData carries the new expiry. Exp is a unix timestamp, and zero means
// the credential does not expire — a real case the refresh scheduler must
// handle, not a missing value.
//
// The contract defines no other field here. In particular there is no auth
// *mode* on the wire: the SDK knows which credential it presented, so mode is
// local state, and reading it from a frame the server never sends would make it
// permanently empty.
type authAckData struct {
	Exp int64 `json:"exp"`
}

type wireAuthError struct {
	Type string        `json:"type"`
	Data authErrorData `json:"data"`
}

// authErrorData distinguishes why a credential was refused. Code drives the
// retry decision — `rate_limited` is worth backing off on, `tenant_mismatch`
// never will be — so losing it collapses every refusal into one indistinguishable
// failure.
type authErrorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// wireHistoryComplete terminates a history response. Source is required by the
// contract and reports where the records came from. Truncated absent decodes to
// false, which is correct: a response that says nothing about truncation was
// not truncated.
type wireHistoryComplete struct {
	Type      string `json:"type"`
	Channel   string `json:"channel"`
	Count     int    `json:"count"`
	Source    string `json:"source"`
	Truncated bool   `json:"truncated,omitempty"`
}

// wireHistoryError reports a failed history request. Unlike subscribe_error and
// unsubscribe_error, this one does carry a channel — the contract marks it
// required, because a history request names exactly one channel.
type wireHistoryError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Channel string `json:"channel"`
	Message string `json:"message"`
}

// wireReplayComplete terminates a replay response.
//
// The count field is `messages_replayed` here and `count` on history_complete.
// The contract names them differently and the platform's own source carries a
// comment pointing at the asymmetry, so it is deliberate rather than drift — but
// it reads as a typo, and "correcting" it silently decodes every replay as zero
// messages delivered with truncated false, which is indistinguishable from an
// empty range.
type wireReplayComplete struct {
	Type             string `json:"type"`
	Channel          string `json:"channel"`
	MessagesReplayed int    `json:"messages_replayed"`
	Truncated        bool   `json:"truncated,omitempty"`
}

// ─── decoding ───

// envelope is pass one of the decode: enough to learn the type, and nothing
// more. Decoding twice costs one extra scan of a small frame and buys a decoder
// that never has to guess.
type envelope struct {
	Type string `json:"type"`
}

// decodeRegistry maps every server-sent wire type to a constructor for its
// struct.
//
// It is the referent for the contract-completeness test: that test parses the
// vendored AsyncAPI, enumerates the message types it defines, and asserts each
// has an entry here. That is what makes a platform release adding a message
// type a loud test failure rather than a frame the SDK silently ignores.
var decodeRegistry = map[string]func() any{
	typeMessage:           func() any { return &wireMessage{} },
	typeReplayMessage:     func() any { return &wireReplayMessage{} },
	typeGap:               func() any { return &wireGap{} },
	typeSubscriptionAck:   func() any { return &wireSubscriptionAck{} },
	typeUnsubscriptionAck: func() any { return &wireUnsubscriptionAck{} },
	typePublishAck:        func() any { return &wirePublishAck{} },
	typePublishError:      func() any { return &wirePublishError{} },
	typeReconnectAck:      func() any { return &wireReconnectAck{} },
	typeReconnectError:    func() any { return &wireReconnectError{} },
	typePong:              func() any { return &wirePong{} },
	typeError:             func() any { return &wireError{} },
	typeSubscribeError:    func() any { return &wireSubscribeError{} },
	typeUnsubscribeError:  func() any { return &wireUnsubscribeError{} },
	typeAuthAck:           func() any { return &wireAuthAck{} },
	typeAuthError:         func() any { return &wireAuthError{} },
	typeHistoryComplete:   func() any { return &wireHistoryComplete{} },
	typeHistoryError:      func() any { return &wireHistoryError{} },
	typeReplayComplete:    func() any { return &wireReplayComplete{} },
}

// sendRegistry maps every client-originated type to a constructor for its
// struct. The SDK does not decode these frames; it emits them.
//
// It mirrors decodeRegistry so conformance can check both directions the same
// way: load the contract's own example of a frame into the struct the SDK would
// emit, then compare. A send struct that cannot round-trip the contract's
// example is a frame the server will reject — and without a registry to
// enumerate, there is nothing to drive that check from.
var sendRegistry = map[string]func() any{
	typeSubscribe:   func() any { return &wireSubscribe{} },
	typeUnsubscribe: func() any { return &wireUnsubscribe{} },
	typePublish:     func() any { return &wirePublish{} },
	typeReconnect:   func() any { return &wireReconnect{} },
	typeHeartbeat:   func() any { return &wireHeartbeat{} },
	typeAuth:        func() any { return &wireAuth{} },
	typeHistory:     func() any { return &wireHistory{} },
	typeReplay:      func() any { return &wireReplay{} },
}

// decodeFrame turns one server frame into its wire struct.
//
// Three outcomes, and the difference between them is the SDK's
// forward-compatibility story:
//
//   - A known type decodes to its struct.
//   - An unknown type is reported as unknown, not as an error. A server may be
//     newer than its clients, and a client that failed on an unrecognized frame
//     could never be deployed ahead of a server upgrade.
//   - A known type that will not decode is a protocol error, surfaced rather
//     than dropped so contract drift is visible instead of silent.
//
// Frames that are not JSON at all are the one case that returns a bare error:
// nothing can be said about them beyond that they were unreadable.
//
// Deferred (§II): this decode checks that a known type unmarshals, not that its
// required fields are present. A `gap` missing `last_pos`, or a `message`
// missing `channel`, decodes to a zero value here rather than being rejected —
// the case §II names explicitly. Presence validation is deliberately left to the
// Phase-3 wire→event mapping, where an absent required field becomes a
// ProtocolError with a public event to attach it to; validating per-field here,
// before that layer exists, would duplicate the mapping's knowledge of which
// fields each type requires. The real server always emits these fields, so
// nothing is exposed in the meantime — but the deferral is recorded rather than
// left implicit. Tracked as T024a in the Phase-3 task list.
func decodeFrame(data []byte) (decoded any, unknownType string, err error) {
	var env envelope
	if e := json.Unmarshal(data, &env); e != nil {
		return nil, "", fmt.Errorf("sukko: frame could not be read as a typed envelope: %w", e)
	}
	if env.Type == "" {
		return nil, "", errors.New("sukko: frame has no type field")
	}

	construct, known := decodeRegistry[env.Type]
	if !known {
		return nil, env.Type, nil
	}

	target := construct()
	if e := json.Unmarshal(data, target); e != nil {
		return nil, "", &ProtocolError{
			Type:    env.Type,
			Message: "frame did not decode into its declared type",
			Cause:   e,
		}
	}
	return target, "", nil
}

// classifyErrorFrame turns a generic error frame into a typed error.
//
// The contract overloads this one frame for connection-level failures and
// per-channel replay rejections, and offers exactly one disambiguator: the
// presence of a channel. Keying on that is stateless — the alternative,
// deciding by whether a replay is currently in flight, would give the same frame
// two meanings depending on when it arrived.
func classifyErrorFrame(frame *wireError) error {
	if frame.Channel != "" {
		return &ReplayError{Code: frame.Code, Channel: frame.Channel, Message: frame.Message}
	}
	return &ProtocolError{Type: typeError, Code: frame.Code, Message: frame.Message}
}
