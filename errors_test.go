package sukko

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The sentinel/struct bridge is what makes errors.Is and errors.As genuinely
// parallel mechanisms. Without it, several sentinels would be exported symbols
// no caller could ever match — ErrNotAvailable in particular is only ever
// carried as a code on a typed struct, so nothing would return or wrap it.
// This table asserts both directions for every pair, plus the negatives that
// keep a code-conditional bridge from matching too broadly.
func TestSentinelStructBridge(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		err      error
		sentinel error
		as       func(error) bool
	}{
		{
			name:     "edition required",
			err:      &EditionRequiredError{Code: "EDITION_LIMIT", RequiredEdition: EditionPro},
			sentinel: ErrEditionRequired,
			as:       func(e error) bool { var t *EditionRequiredError; return errors.As(e, &t) },
		},
		{
			name:     "not connected",
			err:      &NotConnectedError{Op: "Publish"},
			sentinel: ErrNotConnected,
			as:       func(e error) bool { var t *NotConnectedError; return errors.As(e, &t) },
		},
		{
			// not_available is the Direct backend's "pos-recovery is unavailable"
			// capability signal, not a retryable failure.
			name:     "replay not available",
			err:      &ReplayError{Code: CodeNotAvailable, Channel: "acme.trades"},
			sentinel: ErrNotAvailable,
			as:       func(e error) bool { var t *ReplayError; return errors.As(e, &t) },
		},
		{
			name:     "reconnect not available",
			err:      &ReconnectError{Code: CodeNotAvailable},
			sentinel: ErrNotAvailable,
			as:       func(e error) bool { var t *ReconnectError; return errors.As(e, &t) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !errors.Is(tc.err, tc.sentinel) {
				t.Errorf("errors.Is(%v, %v) = false, want true", tc.err, tc.sentinel)
			}
			if !tc.as(tc.err) {
				t.Errorf("errors.As failed to recover the concrete type from %v", tc.err)
			}
			// The bridge must survive wrapping — callers rarely receive an SDK
			// error un-nested.
			wrapped := fmt.Errorf("calling the sdk: %w", tc.err)
			if !errors.Is(wrapped, tc.sentinel) {
				t.Errorf("errors.Is failed through a wrap for %v", tc.err)
			}
			if !tc.as(wrapped) {
				t.Errorf("errors.As failed through a wrap for %v", tc.err)
			}
		})
	}
}

// The not_available bridge is conditional on the code, so it must not match a
// replay or reconnect error that failed for some other reason. A bridge that
// matched unconditionally would tell a caller "this backend cannot recover"
// when the real answer was "that replay was rate-limited".
func TestNotAvailableBridgeIsCodeConditional(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		&ReplayError{Code: "replay_rate_limited", Channel: "acme.trades"},
		&ReplayError{Code: "offset_out_of_range", Channel: "acme.trades"},
		&ReconnectError{Code: "replay_failed"},
	} {
		if errors.Is(err, ErrNotAvailable) {
			t.Errorf("%v must not match ErrNotAvailable — only the not_available code does", err)
		}
	}
}

// Sentinels with no typed struct are reached by being returned or wrapped
// directly. They are exported API, so callers must be able to match them
// through a wrap the same way.
func TestStandaloneSentinelsMatchThroughWrapping(t *testing.T) {
	t.Parallel()

	for _, sentinel := range []error{
		ErrClosed,
		ErrAlreadyConnected,
		ErrConsumerTooSlow,
		ErrUnsupportedByTransport,
		ErrHistoryInProgress,
		ErrSubscribeQueueFull,
		ErrPayloadTooLarge,
		ErrPayloadNotObject,
		ErrTokenSourceFailed,
		ErrInsecureTransport,
		ErrPublishRequiresJWT,
	} {
		wrapped := fmt.Errorf("sdk operation: %w", sentinel)
		if !errors.Is(wrapped, sentinel) {
			t.Errorf("%v does not match through a wrap", sentinel)
		}
	}
}

// Every sentinel must be distinct: two sentinels that compared equal would make
// errors.Is ambiguous and silently route a caller down the wrong branch.
func TestSentinelsAreDistinct(t *testing.T) {
	t.Parallel()

	all := map[string]error{
		"ErrNotConnected":           ErrNotConnected,
		"ErrClosed":                 ErrClosed,
		"ErrEditionRequired":        ErrEditionRequired,
		"ErrNotAvailable":           ErrNotAvailable,
		"ErrAlreadyConnected":       ErrAlreadyConnected,
		"ErrConsumerTooSlow":        ErrConsumerTooSlow,
		"ErrUnsupportedByTransport": ErrUnsupportedByTransport,
		"ErrHistoryInProgress":      ErrHistoryInProgress,
		"ErrSubscribeQueueFull":     ErrSubscribeQueueFull,
		"ErrPayloadTooLarge":        ErrPayloadTooLarge,
		"ErrPayloadNotObject":       ErrPayloadNotObject,
		"ErrTokenSourceFailed":      ErrTokenSourceFailed,
		"ErrInsecureTransport":      ErrInsecureTransport,
		"ErrPublishRequiresJWT":     ErrPublishRequiresJWT,
		// ErrInvalidChannel is an addition beyond the 14 the error surface
		// originally enumerated. Channel validation is caller-facing — a caller
		// checking user input needs errors.Is rather than matching on message
		// text — so it is a sentinel like the rest, and is counted here so this
		// enumeration stays the complete set it claims to be.
		"ErrInvalidChannel": ErrInvalidChannel,
	}
	if len(all) != 15 {
		t.Fatalf("expected 15 sentinels, listed %d", len(all))
	}
	for nameA, a := range all {
		for nameB, b := range all {
			if nameA != nameB && errors.Is(a, b) {
				t.Errorf("%s and %s are not distinct", nameA, nameB)
			}
		}
	}
}

// TestErrorsUnwrapToCause pins the structs that carry an underlying cause. A
// caller inspecting why a decode or a credential fetch failed needs the cause,
// and errors.Is/As must reach through to it.
func TestErrorsUnwrapToCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("the underlying failure")

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"PayloadDecodeError", &PayloadDecodeError{Channel: "acme.trades", Cause: cause}},
		{"TokenSourceError", &TokenSourceError{Attempt: 2, Cause: cause}},
		{"ProtocolError", &ProtocolError{Type: "gap", Cause: cause}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !errors.Is(tc.err, cause) {
				t.Errorf("%s does not unwrap to its cause", tc.name)
			}
		})
	}
}

// Every error message must name the SDK and describe the failure. These strings
// reach logs and support tickets, so an empty or bare-code message costs a
// round-trip to diagnose.
func TestErrorMessages(t *testing.T) {
	t.Parallel()

	retry := 3 * time.Second

	for _, tc := range []struct {
		name     string
		err      error
		contains []string
	}{
		{"NotConnectedError names the op", &NotConnectedError{Op: "Publish"}, []string{"Publish", "not connected"}},
		{"EditionRequiredError names the edition", &EditionRequiredError{Code: "EDITION_LIMIT", RequiredEdition: EditionEnterprise}, []string{"enterprise", "EDITION_LIMIT"}},
		{"CloseError names code and direction", &CloseError{Code: 1008, Direction: directionRemote, Reason: "slow"}, []string{"1008", "remote", "slow"}},
		{"HandshakeError names the status", &HandshakeError{Status: 429, Code: "TENANT_LIMIT_EXCEEDED", RetryAfter: &retry}, []string{"429", "TENANT_LIMIT_EXCEEDED"}},
		{"RateLimitError names the delay", &RateLimitError{RetryAfter: &retry}, []string{"3s"}},
		{"RecoveryInterruptedError names kind and channel", &RecoveryInterruptedError{Kind: "replay", Channel: "acme.trades"}, []string{"replay", "acme.trades"}},
		{"PayloadDecodeError names the channel", &PayloadDecodeError{Channel: "acme.trades", Cause: errors.New("bad json")}, []string{"acme.trades", "bad json"}},
		{"InternalError names the op", &InternalError{Op: "decode", Value: "boom"}, []string{"decode", "boom"}},
		{"TokenSourceError names the attempt", &TokenSourceError{Attempt: 3, Cause: errors.New("timeout")}, []string{"3", "timeout"}},
		{"AuthError names the code", &AuthError{Code: "invalid_token"}, []string{"invalid_token"}},
		{"PublishError names the code", &PublishError{Code: "message_too_large"}, []string{"message_too_large"}},
		{"HistoryError names the code", &HistoryError{Code: "history_disabled"}, []string{"history_disabled"}},
		{"ReplayError names code and channel", &ReplayError{Code: "replay_failed", Channel: "acme.trades"}, []string{"replay_failed", "acme.trades"}},
		{"ReconnectError names the code", &ReconnectError{Code: CodeNotAvailable}, []string{CodeNotAvailable}},
		{"ProtocolError names the type", &ProtocolError{Type: "gap", Message: "missing last_pos"}, []string{"gap", "missing last_pos"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := tc.err.Error()
			if !strings.HasPrefix(msg, "sukko: ") {
				t.Errorf("message %q must be prefixed so it is attributable in a caller's logs", msg)
			}
			for _, want := range tc.contains {
				if !strings.Contains(msg, want) {
					t.Errorf("message %q does not mention %q", msg, want)
				}
			}
		})
	}
}

// A RateLimitError with no Retry-After must still produce a sensible message.
// The gateway does not currently send the header, so this is the common case,
// not the edge case.
func TestRateLimitErrorWithoutRetryAfter(t *testing.T) {
	t.Parallel()

	msg := (&RateLimitError{}).Error()
	if !strings.Contains(msg, "rate limited") {
		t.Errorf("message %q should still describe the failure without a Retry-After", msg)
	}
}
