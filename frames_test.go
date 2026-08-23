package sukko

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Named builders for the server frames tests script.
//
// The alternative is JSON string literals inline at every call site, which
// three things argue against: a contract field renamed in one of forty literals
// is invisible, a reader cannot tell an intentionally-malformed frame from a
// typo, and the interesting faults — a gap with no cursor, an auth_ack that
// never expires — look like ordinary frames until someone reads them closely.
// A named builder puts the intent in the name.
//
// These emit strings rather than marshaling the SDK's own wire structs
// deliberately: a fixture that shares the encoder with the code under test
// cannot catch an encoder that is wrong in both places.

// ─── delivery ───

// frameMessage is an ordinary live message.
func frameMessage(channel string, seq int, pos, data string) string {
	return fmt.Sprintf(`{"type":"message","channel":%q,"seq":%d,"ts":%d,"pos":%q,"data":%s}`,
		channel, seq, fixedTS, pos, data)
}

// frameMessageWithoutPos is a live message carrying no cursor.
//
// This is the Direct backend's shape: pos is omitted entirely, so a client that
// treats an absent cursor as an empty one, or that expects every message to
// anchor recovery, breaks here rather than in production.
func frameMessageWithoutPos(channel string, seq int, data string) string {
	return fmt.Sprintf(`{"type":"message","channel":%q,"seq":%d,"ts":%d,"data":%s}`,
		channel, seq, fixedTS, data)
}

// frameHistoryMessage is a message delivered as part of a history response.
func frameHistoryMessage(channel string, seq int, pos, data string) string {
	return fmt.Sprintf(`{"type":"message","channel":%q,"seq":%d,"ts":%d,"pos":%q,"history":true,"data":%s}`,
		channel, seq, fixedTS, pos, data)
}

// ─── subscription ───

// frameSubscriptionAck grants exactly the channels given.
func frameSubscriptionAck(channels ...string) string {
	return fmt.Sprintf(`{"type":"subscription_ack","subscribed":[%s],"count":%d}`,
		quoteJoin(channels), len(channels))
}

// framePartialSubscriptionAck grants a subset of what was requested.
//
// The gateway drops unauthorized channels silently, so the ack is the only
// place a denial is visible. A client that assumes it received what it asked
// for waits forever on a channel that will never deliver.
func framePartialSubscriptionAck(granted ...string) string {
	return frameSubscriptionAck(granted...)
}

// frameForcedUnsubscribe is a server-initiated unsubscribe, as follows a
// credential downgrade. It carries no count, which is what distinguishes it
// from an ack for a client's own request.
func frameForcedUnsubscribe(channels ...string) string {
	return fmt.Sprintf(`{"type":"unsubscription_ack","unsubscribed":[%s],"forced":true}`,
		quoteJoin(channels))
}

// ─── auth ───

// frameAuthAck accepts a credential, expiring at the given unix time.
func frameAuthAck(exp int64) string {
	return fmt.Sprintf(`{"type":"auth_ack","exp":%d}`, exp)
}

// frameAuthAckNoExpiry accepts a credential that never expires.
//
// exp == 0 is the case that makes a caller-supplied expiry worth having: with
// no server expiry to schedule against, a client that relies on the server
// alone arms no refresh at all and discovers the problem reactively.
func frameAuthAckNoExpiry() string { return frameAuthAck(0) }

// frameAuthError rejects a credential.
func frameAuthError(code string) string {
	return fmt.Sprintf(`{"type":"auth_error","code":%q,"message":"auth rejected"}`, code)
}

// ─── recovery ───

// frameGap is an advisory gap notification carrying a cursor to replay from.
func frameGap(channel, lastPos string, fromSeq, toSeq int) string {
	return fmt.Sprintf(`{"type":"gap","channel":%q,"last_pos":%q,"from_seq":%d,"to_seq":%d}`,
		channel, lastPos, fromSeq, toSeq)
}

// frameGapWithEmptyPos is a gap whose cursor is present but empty.
//
// The contract marks last_pos required, and the server sends it unconditionally
// — so on a backend that keeps no cursors it arrives as "". A client must treat
// that as "nothing to anchor to" rather than replaying from position zero.
func frameGapWithEmptyPos(channel string) string {
	return frameGap(channel, "", 0, 0)
}

// frameReplayComplete terminates a replay window.
func frameReplayComplete(channel string, count int) string {
	return fmt.Sprintf(`{"type":"replay_complete","channel":%q,"count":%d,"truncated":false}`,
		channel, count)
}

// frameHistoryComplete terminates a history response.
func frameHistoryComplete(channel string, count int, source HistorySource) string {
	return fmt.Sprintf(`{"type":"history_complete","channel":%q,"count":%d,"source":%q,"truncated":false}`,
		channel, count, source)
}

// frameReconnectAck accepts a reconnect and reports how much was replayed.
func frameReconnectAck(replayed int) string {
	return fmt.Sprintf(`{"type":"reconnect_ack","status":"completed","messages_replayed":%d}`, replayed)
}

// frameReconnectAckNothingReplayed is what a cursorless backend actually
// answers a reconnect with.
//
// It matters because it is *success*: the client asked to resume and was told
// the resume completed, having replayed nothing. A client waiting for an error
// to tell it recovery was unavailable waits forever and loses the gap silently.
func frameReconnectAckNothingReplayed() string { return frameReconnectAck(0) }

// frameReconnectError rejects a reconnect.
func frameReconnectError(code string) string {
	return fmt.Sprintf(`{"type":"reconnect_error","code":%q,"message":"reconnect rejected"}`, code)
}

// frameReconnectNotAvailable is the rarer explicit "no pos-recovery here"
// signal, as opposed to the reconnect_ack a cursorless backend usually sends.
func frameReconnectNotAvailable() string { return frameReconnectError(CodeNotAvailable) }

// ─── errors ───

// frameError is the contract's generic error frame. A channel makes it a replay
// error; without one it is a connection-level error.
func frameError(code, channel string) string {
	if channel == "" {
		return fmt.Sprintf(`{"type":"error","code":%q,"message":"error"}`, code)
	}
	return fmt.Sprintf(`{"type":"error","code":%q,"channel":%q,"message":"error"}`, code, channel)
}

// framePong answers a heartbeat.
func framePong() string { return `{"type":"pong"}` }

// frameUnknownType is a well-formed frame of a type this SDK version does not
// know — what a newer server sends an older client. It must be survivable:
// a client that errors on it cannot be deployed ahead of a server upgrade.
func frameUnknownType() string {
	return `{"type":"quantum_entanglement_ack","payload":{"spooky":true}}`
}

// frameMalformed is not valid JSON at all.
func frameMalformed() string { return `{"type":"message",,,}` }

// ─── Retry-After forms ───
//
// The header has two legal encodings and a meaningful absence, and the SDK's
// backoff override behaves differently across all three. Naming them keeps a
// test from asserting one form and implying it covered the header.

// retryAfterSeconds is the delta-seconds form.
func retryAfterSeconds(d time.Duration) string {
	return strconv.Itoa(int(d.Seconds()))
}

// retryAfterHTTPDate is the absolute-date form, expressed relative to a base so
// a test controls the resulting delay.
func retryAfterHTTPDate(base time.Time, d time.Duration) string {
	return base.Add(d).UTC().Format(http1123)
}

// http1123 is the date format RFC 7231 requires for Retry-After.
const http1123 = "Mon, 02 Jan 2006 15:04:05 GMT"

// fixedTS is the timestamp every built frame carries. A constant keeps golden
// comparisons stable; no assertion depends on its value.
const fixedTS = 1767225600000

// quoteJoin renders a string slice as JSON array elements.
func quoteJoin(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = fmt.Sprintf("%q", item)
	}
	return strings.Join(quoted, ",")
}

// ─── self-tests ───
//
// The builders are fixtures the whole suite depends on, so their shape is
// asserted rather than assumed. A builder that emitted a field the contract
// does not have would make every test using it agree with a wire format the
// server never speaks.

func TestFrameBuildersProduceExpectedShape(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		frame       string
		wantType    string
		mustContain []string
		mustNotHave []string
	}{
		{
			name:        "live message carries a cursor",
			frame:       frameMessage("acme.trades", 1, "kafka:7:42", `{"price":100}`),
			wantType:    "message",
			mustContain: []string{`"channel":"acme.trades"`, `"pos":"kafka:7:42"`, `"seq":1`},
		},
		{
			name:        "cursorless message omits pos entirely",
			frame:       frameMessageWithoutPos("acme.trades", 1, `{"price":100}`),
			wantType:    "message",
			mustNotHave: []string{`"pos"`},
		},
		{
			name:        "history message is flagged, not a distinct type",
			frame:       frameHistoryMessage("acme.trades", 1, "kafka:7:1", `{}`),
			wantType:    "message",
			mustContain: []string{`"history":true`},
		},
		{
			name:        "partial ack grants only what it lists",
			frame:       framePartialSubscriptionAck("acme.trades"),
			wantType:    "subscription_ack",
			mustContain: []string{`"subscribed":["acme.trades"]`},
			mustNotHave: []string{"acme.private"},
		},
		{
			name:        "forced unsubscribe is flagged and carries no count",
			frame:       frameForcedUnsubscribe("acme.trades"),
			wantType:    "unsubscription_ack",
			mustContain: []string{`"forced":true`},
			mustNotHave: []string{`"count"`},
		},
		{
			name:        "no-expiry auth ack",
			frame:       frameAuthAckNoExpiry(),
			wantType:    "auth_ack",
			mustContain: []string{`"exp":0`},
		},
		{
			name:        "gap with an empty cursor is still a gap",
			frame:       frameGapWithEmptyPos("acme.trades"),
			wantType:    "gap",
			mustContain: []string{`"last_pos":""`},
		},
		{
			name:        "cursorless reconnect reports success, not an error",
			frame:       frameReconnectAckNothingReplayed(),
			wantType:    "reconnect_ack",
			mustContain: []string{`"status":"completed"`, `"messages_replayed":0`},
		},
		{
			name:        "explicit not-available reconnect",
			frame:       frameReconnectNotAvailable(),
			wantType:    "reconnect_error",
			mustContain: []string{`"code":"not_available"`},
		},
		{
			name:        "replay error carries a channel",
			frame:       frameError("replay_rate_limited", "acme.trades"),
			wantType:    "error",
			mustContain: []string{`"channel":"acme.trades"`},
		},
		{
			name:        "connection error carries no channel",
			frame:       frameError("invalid_json", ""),
			wantType:    "error",
			mustNotHave: []string{`"channel"`},
		},
		{
			name:        "history complete names its source",
			frame:       frameHistoryComplete("acme.trades", 5, HistorySourceMixed),
			wantType:    "history_complete",
			mustContain: []string{`"source":"mixed"`, `"count":5`},
		},
		{
			// The terminator that the drop-a-terminator fault drops. It has to
			// exist as a fixture before its absence can be scripted.
			name:        "replay complete terminates a window",
			frame:       frameReplayComplete("acme.trades", 3),
			wantType:    "replay_complete",
			mustContain: []string{`"channel":"acme.trades"`, `"count":3`, `"truncated":false`},
		},
		{
			name:        "auth error carries a code",
			frame:       frameAuthError("invalid_token"),
			wantType:    "auth_error",
			mustContain: []string{`"code":"invalid_token"`},
		},
		{
			name:     "pong answers a heartbeat",
			frame:    framePong(),
			wantType: "pong",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := messageType([]byte(tc.frame)); got != tc.wantType {
				t.Errorf("type = %q, want %q", got, tc.wantType)
			}
			for _, want := range tc.mustContain {
				if !strings.Contains(tc.frame, want) {
					t.Errorf("frame %s does not contain %s", tc.frame, want)
				}
			}
			for _, unwanted := range tc.mustNotHave {
				if strings.Contains(tc.frame, unwanted) {
					t.Errorf("frame %s must not contain %s", tc.frame, unwanted)
				}
			}
		})
	}
}

// The deliberately-broken fixtures must actually be broken, or the tests using
// them assert nothing. A malformed frame that happened to parse would make a
// decoder-hardening test pass against a decoder that does no hardening.
func TestMalformedFixturesAreActuallyMalformed(t *testing.T) {
	t.Parallel()

	if got := messageType([]byte(frameMalformed())); got != "" {
		t.Errorf("the malformed fixture parsed as type %q; it must not parse", got)
	}
	if got := messageType([]byte(frameUnknownType())); got == "" {
		t.Error("the unknown-type fixture must be well-formed JSON — it is a valid frame of an unknown type")
	}
}

func TestRetryAfterForms(t *testing.T) {
	t.Parallel()

	if got := retryAfterSeconds(3 * time.Second); got != "3" {
		t.Errorf("delta-seconds form = %q, want %q", got, "3")
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := retryAfterHTTPDate(base, 30*time.Second); got != "Thu, 01 Jan 2026 00:00:30 GMT" {
		t.Errorf("HTTP-date form = %q", got)
	}

	// Parsing back must recover the delay, or a test asserting the override
	// would be asserting against a header the SDK cannot read.
	parsed, err := time.Parse(http1123, retryAfterHTTPDate(base, 30*time.Second))
	if err != nil {
		t.Fatalf("the generated HTTP-date does not parse: %v", err)
	}
	if got := parsed.Sub(base); got != 30*time.Second {
		t.Errorf("round-tripped delay = %v, want 30s", got)
	}
}
