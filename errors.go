package sukko

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// This file holds the SDK's error surface: sentinels for conditions a caller
// branches on, typed structs for failures that carry detail, and the bridge
// that ties them together.
//
// The two are parallel mechanisms — errors.Is for "which condition is this",
// errors.As for "give me the detail" — so every struct with a corresponding
// sentinel implements Is. Without that bridge, several sentinels would be
// exported symbols nothing could ever match: ErrNotAvailable in particular is
// only ever carried as a code on a typed struct.
//
// Every message is prefixed "sukko: " so it stays attributable once it has been
// wrapped a few layers deep in a caller's own error chain.

// Sentinel errors. Match with errors.Is; several are also reachable through the
// typed structs below.
var (
	// ErrNotConnected is returned by an operation that requires a live
	// connection. Publish returns it rather than silently discarding a message.
	ErrNotConnected = errors.New("sukko: not connected")

	// ErrClosed is returned by any method called after Close. A Client is
	// single-use: the delivery channel closes exactly once, so a closed client
	// cannot be revived.
	ErrClosed = errors.New("sukko: client is closed")

	// ErrEditionRequired means the operation needs a higher platform edition.
	ErrEditionRequired = errors.New("sukko: operation requires a higher edition")

	// ErrNotAvailable means position-anchored recovery is unavailable — the
	// Direct backend keeps no cursor. It is a capability signal, not a retryable
	// failure: the SDK degrades to a naive resubscribe and reports the gap.
	ErrNotAvailable = errors.New("sukko: pos-anchored recovery is not available")

	// ErrAlreadyConnected is returned by Connect on a client that is already
	// connecting or connected.
	ErrAlreadyConnected = errors.New("sukko: already connected")

	// ErrConsumerTooSlow terminates a client whose consumer never caught up,
	// after MaxBackpressureReconnects consecutive back-pressure cycles. It is
	// reported rather than looping silently, so the cause is visible.
	ErrConsumerTooSlow = errors.New("sukko: consumer too slow")

	// ErrUnsupportedByTransport means the chosen transport cannot perform the
	// operation — SSE cannot publish over the stream or subscribe live.
	ErrUnsupportedByTransport = errors.New("sukko: operation not supported by this transport")

	// ErrHistoryInProgress means a History call is already outstanding. History
	// is single-flight so one caller's window cannot exhaust the delivery
	// channel that another is relying on.
	ErrHistoryInProgress = errors.New("sukko: a history request is already in flight")

	// ErrHistoryLimitExceeded means a History call asked for more records than the
	// client's configured HistoryLimit. Checked locally before the send, like the
	// publish payload pre-checks — the server would also reject it, but with nothing
	// to correlate the rejection to on a fire-and-forget request.
	ErrHistoryLimitExceeded = errors.New("sukko: history limit exceeds the configured maximum")

	// ErrSubscribeQueueFull means the subscribe serializer's queue is full.
	// Subscribe-class requests are serialized because the contract carries no
	// request id, so an ack cannot otherwise be attributed to its request.
	ErrSubscribeQueueFull = errors.New("sukko: subscribe queue is full")

	// ErrPayloadTooLarge means a publish payload exceeds the configured maximum.
	// Checked locally before the write: on WebSocket, publish is fire-and-forget,
	// so a server rejection would arrive with nothing to correlate it to.
	ErrPayloadTooLarge = errors.New("sukko: publish payload exceeds the maximum size")

	// ErrPayloadNotObject means a publish payload does not marshal to a JSON
	// object, which the contract requires. Also a local pre-check.
	ErrPayloadNotObject = errors.New("sukko: publish payload must marshal to a JSON object")

	// ErrTokenSourceFailed terminates a client after MaxTokenSourceAttempts
	// consecutive TokenSource failures. Reconnecting past this point would
	// consume handshakes with a credential the SDK knows it cannot refresh.
	ErrTokenSourceFailed = errors.New("sukko: token source failed repeatedly")

	// ErrInsecureTransport is returned by NewClient for a ws:// or http:// URL
	// without WithInsecureTransport, and unconditionally when query-parameter
	// auth is combined with a non-TLS URL. Redaction removes credentials from
	// logs; it cannot remove them from the wire.
	ErrInsecureTransport = errors.New("sukko: refusing to use a non-TLS URL")

	// ErrPublishRequiresJWT means publishing was attempted with an API key
	// alone. Checked locally from the SDK's own credential state.
	ErrPublishRequiresJWT = errors.New("sukko: publishing requires a JWT, not an API key")

	// ErrEmptyToken means UpdateToken or Escalate was called with an empty string.
	// Supplying no credential where one is required is a caller mistake, not a no-op.
	ErrEmptyToken = errors.New("sukko: a non-empty token is required")
)

// CodeNotAvailable is the wire code the server sends when position-anchored
// recovery is unavailable. It is the one code the not_available bridge keys on,
// so it is named rather than written as a literal at each comparison.
const CodeNotAvailable = "not_available"

// NotConnectedError reports an operation attempted without a live connection.
// It names the operation, so a caller with several in flight can tell which one
// was refused.
type NotConnectedError struct {
	// Op is the method that was refused, e.g. "Publish".
	Op string
}

func (e *NotConnectedError) Error() string {
	return fmt.Sprintf("sukko: %s: not connected", e.Op)
}

// Is bridges to ErrNotConnected.
func (e *NotConnectedError) Is(target error) bool { return target == ErrNotConnected }

// EditionRequiredError reports an operation refused by an edition gate. The
// required edition comes from the SDK's own knowledge of which gate the call
// crossed — the gateway's 403 body carries only a code — so the field is always
// populated rather than being a permanently-zero value.
type EditionRequiredError struct {
	// Code is the gateway's body error code, e.g. "EDITION_LIMIT".
	Code string
	// RequiredEdition is the edition the operation needs.
	RequiredEdition Edition
}

func (e *EditionRequiredError) Error() string {
	return fmt.Sprintf("sukko: %s: operation requires the %s edition", e.Code, e.RequiredEdition)
}

// Is bridges to ErrEditionRequired.
func (e *EditionRequiredError) Is(target error) bool { return target == ErrEditionRequired }

// Recovery kinds name which recovery a *RecoveryInterruptedError describes. They
// are the single source of the strings, referenced instead of literals (§I).
const (
	// RecoveryKindReplay is a live gap→replay (or reconnect-replay) window.
	RecoveryKindReplay = "replay"
	// RecoveryKindHistory is a History request window.
	RecoveryKindHistory = "history"
)

// RecoveryInterruptedError reports a recovery that ended without its terminator
// — a replay or history window cut short by a deadline, a mid-recovery close, or
// an epoch ending. It exists so a truncated recovery is never surfaced as a bare
// disconnect, which would leave the caller believing the window completed.
type RecoveryInterruptedError struct {
	// Kind is the recovery that was interrupted: RecoveryKindReplay or
	// RecoveryKindHistory.
	Kind string
	// Channel is the affected channel.
	Channel string
}

func (e *RecoveryInterruptedError) Error() string {
	return fmt.Sprintf("sukko: %s recovery interrupted on channel %s", e.Kind, e.Channel)
}

// AuthError reports a server-side auth failure carrying a contract error code.
type AuthError struct {
	// Code is the contract's auth error code, e.g. "invalid_token".
	Code string
	// Message is the server's human-readable description, when the frame
	// carried one. The wire frame always may, so dropping it would discard
	// diagnostics the server took the trouble to send.
	Message string
}

func (e *AuthError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sukko: auth failed: %s: %s", e.Code, e.Message)
	}
	return "sukko: auth failed: " + e.Code
}

// PublishError reports a server-side publish rejection.
type PublishError struct {
	// Code is the contract's publish error code, e.g. "message_too_large".
	Code string
	// Message is the server's human-readable description, when present.
	Message string
}

func (e *PublishError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sukko: publish rejected: %s: %s", e.Code, e.Message)
	}
	return "sukko: publish rejected: " + e.Code
}

// HistoryError reports a server-side history rejection. It carries the channel
// because a history request names exactly one channel, and the contract marks
// the field required on this frame.
type HistoryError struct {
	// Code is the contract's history error code, e.g. "history_disabled".
	Code string
	// Channel is the channel whose history request failed.
	Channel string
	// Message is the server's human-readable description, when present.
	Message string
}

func (e *HistoryError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sukko: history failed on channel %s: %s: %s", e.Channel, e.Code, e.Message)
	}
	return fmt.Sprintf("sukko: history failed on channel %s: %s", e.Channel, e.Code)
}

// ReplayError reports a server-side replay rejection. It carries the channel
// because the recovery engine is per-channel: an error that could not name its
// channel could not be attributed to the replay it belongs to, and Scenario 4's
// documented fallback ("call History for that channel") would have no argument.
type ReplayError struct {
	// Code is the contract's replay error code, e.g. "replay_rate_limited".
	Code string
	// Channel is the channel whose replay failed.
	Channel string
	// Message is the server's human-readable description, when present.
	Message string
}

func (e *ReplayError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sukko: replay failed on channel %s: %s: %s", e.Channel, e.Code, e.Message)
	}
	return fmt.Sprintf("sukko: replay failed on channel %s: %s", e.Channel, e.Code)
}

// Is bridges to ErrNotAvailable, but only for the not_available code — a
// rate-limited or out-of-range replay is a different condition and must not
// masquerade as "this backend cannot recover".
func (e *ReplayError) Is(target error) bool {
	return target == ErrNotAvailable && e.Code == CodeNotAvailable
}

// ReconnectError reports a server-side reconnect rejection.
type ReconnectError struct {
	// Code is the contract's reconnect error code.
	Code string
	// Message is the server's human-readable description, when present.
	Message string
}

func (e *ReconnectError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sukko: reconnect failed: %s: %s", e.Code, e.Message)
	}
	return "sukko: reconnect failed: " + e.Code
}

// Is bridges to ErrNotAvailable on the not_available code only, for the same
// reason as ReplayError.
func (e *ReconnectError) Is(target error) bool {
	return target == ErrNotAvailable && e.Code == CodeNotAvailable
}

// ProtocolError reports a frame the SDK could decode as JSON but not accept —
// a known message type whose payload violates the contract, or an unknown error
// code within a known type. It is surfaced rather than dropped so a
// contract-drift bug is visible instead of silent.
type ProtocolError struct {
	// Type is the wire message type the frame claimed to be.
	Type string
	// Code is the error code carried by the frame, when it had one.
	Code string
	// Message describes what was wrong with the frame.
	Message string
	// Cause is the underlying decode failure, when there was one.
	Cause error
}

func (e *ProtocolError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("sukko: protocol error on %s frame: %s: %v", e.Type, e.Message, e.Cause)
	}
	return fmt.Sprintf("sukko: protocol error on %s frame: %s", e.Type, e.Message)
}

// Unwrap exposes the underlying decode failure.
func (e *ProtocolError) Unwrap() error { return e.Cause }

// HandshakeError reports a failed WebSocket upgrade or SSE stream open. A
// handshake failure carries an HTTP status and a body code and no close code at
// all, which is why it is its own type rather than a CloseError.
type HandshakeError struct {
	// Status is the HTTP status of the failed upgrade.
	Status int
	// Code is the body error code, when the response carried one.
	Code string
	// Message is the body message, when the response carried one.
	Message string
	// RetryAfter is the parsed Retry-After header, or nil when absent. A
	// pointer because a present zero ("retry immediately") is a legitimate
	// value distinct from "the server said nothing".
	RetryAfter *time.Duration
}

func (e *HandshakeError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("sukko: handshake failed with %d: %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("sukko: handshake failed with %d: %s", e.Status, e.Message)
}

// RateLimitError reports a rate-limited REST or push call. It is a return value
// only and never delivered in-band: the caller made the call and is receiving
// the answer, so enqueueing it as well would report one failure twice.
type RateLimitError struct {
	// RetryAfter is the parsed Retry-After header, or nil when absent.
	RetryAfter *time.Duration
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter != nil {
		return fmt.Sprintf("sukko: rate limited, retry after %s", *e.RetryAfter)
	}
	return "sukko: rate limited"
}

// CloseError reports a WebSocket close. Direction is part of the type because
// several codes are legal from both ends and mean different things: a local
// 1000 is the caller's own Close, a remote 1000 is the server closing normally,
// and only one of those is worth reconnecting.
type CloseError struct {
	// Code is the WebSocket close code.
	Code int
	// Direction records whether the SDK or the server initiated the close.
	Direction closeDirection
	// Reason is the close reason, when the frame carried one.
	Reason string
}

func (e *CloseError) Error() string {
	return fmt.Sprintf("sukko: connection closed (code %d, %s): %s", e.Code, e.Direction, e.Reason)
}

// PayloadDecodeError reports a payload that would not decode into the caller's
// type. It names the channel and wraps the decoder's own error, so the public
// contract stays an SDK type while the detail remains reachable.
type PayloadDecodeError struct {
	// Channel is the channel the message arrived on.
	Channel string
	// Cause is the underlying decode failure.
	Cause error
}

func (e *PayloadDecodeError) Error() string {
	return fmt.Sprintf("sukko: decoding payload on channel %s: %v", e.Channel, e.Cause)
}

// Unwrap exposes the underlying decode failure.
func (e *PayloadDecodeError) Unwrap() error { return e.Cause }

// InternalError reports a panic recovered inside an SDK goroutine. It is
// surfaced in-band as well as logged: the SDK is quiet by default, so a
// log-only report would be invisible to a caller who never wired a logger, and
// a recovered panic that nobody hears about is a silent failure.
type InternalError struct {
	// Op is the SDK operation that panicked.
	Op string
	// Value is the recovered panic value, rendered as a string.
	Value string
	// Stack is the stack trace captured at recovery.
	Stack string
}

func (e *InternalError) Error() string {
	return fmt.Sprintf("sukko: internal error in %s: %s", e.Op, e.Value)
}

// TokenSourceError reports one failed TokenSource call. It is surfaced per
// attempt so a caller sees credential trouble as it happens, rather than only
// learning of it when the client terminates with ErrTokenSourceFailed.
type TokenSourceError struct {
	// Attempt is the 1-based attempt number within the current run of failures.
	Attempt int
	// Cause is the error the TokenSource returned, or the timeout that bounded it.
	Cause error
}

func (e *TokenSourceError) Error() string {
	return fmt.Sprintf("sukko: token source failed (attempt %d): %v", e.Attempt, e.Cause)
}

// Unwrap exposes the caller's own error.
func (e *TokenSourceError) Unwrap() error { return e.Cause }

// parseRetryAfter interprets a Retry-After header value.
//
// The header has two legal encodings, and the SDK honors both: a proxy, CDN or
// ingress in front of the gateway can legitimately set either, and a value the
// SDK cannot read is a backoff hint thrown away.
//
// It returns nil when the header is absent or unparseable, which is distinct
// from a zero duration: "the server said nothing" and "the server said retry
// immediately" are different instructions, and collapsing them into 0 would
// silently answer "retry now" with a full exponential backoff. That distinction
// is the reason the field is a pointer.
//
// now is passed in rather than read from the clock so the HTTP-date form is
// computed against the SDK's own time source.
func parseRetryAfter(value string, now time.Time) *time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	// Delta-seconds: a non-negative integer count of seconds.
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return nil // not a legal value; treat it as absent
		}
		d := time.Duration(seconds) * time.Second
		return &d
	}

	// HTTP-date: an absolute instant. A date already in the past means "retry
	// now", which is a real instruction rather than a malformed one, so it
	// clamps to zero instead of being discarded.
	if at, err := http.ParseTime(value); err == nil {
		d := max(at.Sub(now), 0)
		return &d
	}

	return nil
}
