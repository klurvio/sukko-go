package sukko

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// TestGoldenFixturesDecode decodes every fixture in testdata/fixtures/valid and
// checks it produced the struct its filename names.
//
// The filename is the assertion. A fixture named message.json that decodes to a
// wireGap is a dispatch bug, and naming the file after the expected type is what
// makes that visible without a per-fixture expectation table to keep in sync.
func TestGoldenFixturesDecode(t *testing.T) {
	for name, data := range loadFixtures(t, "valid") {
		wireType := strings.TrimSuffix(name, ".json")

		t.Run(wireType, func(t *testing.T) {
			decoded, unknown, err := decodeFrame(data)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			if unknown != "" {
				t.Fatalf("type %q was not recognised, but it is in the registry", unknown)
			}

			reencoded := mustMarshal(t, decoded)

			// Every wire struct carries its own Type field; reading it back
			// confirms the decoder chose the struct the frame asked for.
			var check envelope
			if e := json.Unmarshal(reencoded, &check); e != nil {
				t.Fatalf("re-marshal of decoded struct failed: %v", e)
			}
			if check.Type != wireType {
				t.Fatalf("fixture %s decoded to a struct reporting type %q", name, check.Type)
			}

			// Full round-trip: the re-encoded frame must carry everything the
			// fixture did. Comparing as trees rather than bytes ignores key
			// order, which JSON does not define — but any field that decodes
			// and then fails to re-emit shows up here, and that is the bug
			// worth catching. A contract field with no struct field, or a
			// struct field whose json tag is misspelled, both round-trip
			// cleanly through a type check and are invisible without this.
			if diff := jsonDiff(t, data, reencoded); diff != "" {
				t.Errorf("fixture %s did not survive decode→encode:\n%s", name, diff)
			}
		})
	}
}

// TestEveryDecodableTypeHasAFixture keeps the golden set honest.
//
// Without it, adding a registry entry and forgetting its fixture leaves the new
// type untested while the suite stays green — the failure mode the fixtures
// exist to prevent.
func TestEveryDecodableTypeHasAFixture(t *testing.T) {
	fixtures := loadFixtures(t, "valid")

	for wireType := range decodeRegistry {
		if _, ok := fixtures[wireType+".json"]; !ok {
			t.Errorf("type %q is decodable but has no fixture at testdata/fixtures/valid/%s.json", wireType, wireType)
		}
	}
	for name := range fixtures {
		wireType := strings.TrimSuffix(name, ".json")
		if _, ok := decodeRegistry[wireType]; !ok {
			t.Errorf("fixture %s has no matching registry entry — the type was removed or misspelled", name)
		}
	}
}

// TestMalformedFramesAreReported pins which kind of failure each malformation
// produces.
//
// The distinction matters to the client loop: a bare error means the frame was
// unreadable and the connection is suspect, while a ProtocolError means the
// server sent something structurally wrong for a type both sides claim to know
// — contract drift, which is reportable but not fatal.
func TestMalformedFramesAreReported(t *testing.T) {
	tests := map[string]struct {
		wantProtocolError bool
	}{
		"not_json.txt":         {wantProtocolError: false},
		"no_type.txt":          {wantProtocolError: false},
		"empty_type.txt":       {wantProtocolError: false},
		"type_not_string.txt":  {wantProtocolError: false},
		"seq_not_number.txt":   {wantProtocolError: true},
		"truncated.txt":        {wantProtocolError: false},
		"scalar_for_array.txt": {wantProtocolError: true},
	}

	fixtures := loadFixtures(t, "invalid")
	if len(fixtures) != len(tests) {
		t.Errorf("fixture count %d does not match table size %d — a fixture was added without an expectation", len(fixtures), len(tests))
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			data, ok := fixtures[name]
			if !ok {
				t.Fatalf("fixture %s is missing", name)
			}

			decoded, unknown, err := decodeFrame(data)
			if err == nil {
				t.Fatalf("malformed frame decoded successfully to %T (unknown=%q)", decoded, unknown)
			}

			var pe *ProtocolError
			if got := errors.As(err, &pe); got != tc.wantProtocolError {
				t.Errorf("ProtocolError = %v, want %v (err: %v)", got, tc.wantProtocolError, err)
			}
		})
	}
}

// TestUnknownTypeIsNotAnError covers the forward-compatibility rule: a server
// may be newer than its clients, so an unrecognised type is reported as unknown
// rather than failing. A client that errored here could never be deployed ahead
// of a server upgrade.
func TestUnknownTypeIsNotAnError(t *testing.T) {
	tests := []struct {
		name  string
		frame string
		want  string
	}{
		{"future type", `{"type":"quantum_ack","channel":"acme.x","seq":1}`, "quantum_ack"},
		{"future type, no other fields", `{"type":"whatever"}`, "whatever"},
		{"type that only differs in case", `{"type":"Message","channel":"acme.x"}`, "Message"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded, unknown, err := decodeFrame([]byte(tc.frame))
			if err != nil {
				t.Fatalf("unknown type produced an error: %v", err)
			}
			if decoded != nil {
				t.Errorf("unknown type produced a decoded struct %T", decoded)
			}
			if unknown != tc.want {
				t.Errorf("unknown = %q, want %q", unknown, tc.want)
			}
		})
	}
}

// TestUnknownFieldsAreIgnored is the other half of forward compatibility: a
// server that adds a field to an existing type must not break clients that
// predate it.
func TestUnknownFieldsAreIgnored(t *testing.T) {
	frame := `{"type":"message","channel":"acme.orders.eu","seq":9,"ts":5,"data":{},"priority":"high","region":"eu-west"}`

	decoded, unknown, err := decodeFrame([]byte(frame))
	if err != nil || unknown != "" {
		t.Fatalf("decode failed: err=%v unknown=%q", err, unknown)
	}

	msg, ok := decoded.(*wireMessage)
	if !ok {
		t.Fatalf("decoded to %T, want *wireMessage", decoded)
	}
	if msg.Seq != 9 || msg.Channel != "acme.orders.eu" {
		t.Errorf("known fields were disturbed by the unknown ones: %+v", msg)
	}
}

// TestOptionalFieldSemantics is the heart of the field-modeling rule: for each
// field where absent and zero mean different things, prove the decoder keeps
// them apart.
//
// Every row here is a bug that would otherwise be invisible — a plausible zero
// standing in for "the server said nothing", which reads as a real value at
// every layer above.
func TestOptionalFieldSemantics(t *testing.T) {
	t.Run("unsubscription_ack count distinguishes absent from zero", func(t *testing.T) {
		// Absent: a forced unsubscribe carries no count.
		absent := decodeAs[*wireUnsubscriptionAck](t, `{"type":"unsubscription_ack","unsubscribed":["acme.x"],"forced":true}`)
		if absent.Count != nil {
			t.Errorf("absent count decoded to %d, want nil — a missing count must not read as zero remaining", *absent.Count)
		}
		if !absent.Forced {
			t.Error("forced = false, want true")
		}

		// Present zero: the caller now has no subscriptions. Same Go zero
		// value, entirely different meaning.
		zero := decodeAs[*wireUnsubscriptionAck](t, `{"type":"unsubscription_ack","unsubscribed":["acme.x"],"count":0}`)
		if zero.Count == nil {
			t.Fatal("present count 0 decoded to nil — zero subscriptions is a fact, not an absence")
		}
		if *zero.Count != 0 {
			t.Errorf("count = %d, want 0", *zero.Count)
		}
		if zero.Forced {
			t.Error("forced = true, want false for an absent forced flag")
		}
	})

	t.Run("subscription_ack count is required, not optional", func(t *testing.T) {
		// The contract lists count among subscription_ack's required fields and
		// the server always sends it, so this one is a plain int — unlike
		// unsubscription_ack's, which is genuinely absent on a forced removal.
		// A pointer here would add a nil branch that can never occur.
		ack := decodeAs[*wireSubscriptionAck](t, `{"type":"subscription_ack","subscribed":["acme.x","acme.y"],"count":2}`)
		if ack.Count != 2 {
			t.Errorf("count = %d, want 2", ack.Count)
		}
		if len(ack.Subscribed) != 2 {
			t.Errorf("subscribed = %v, want 2 entries", ack.Subscribed)
		}
	})

	t.Run("subscription_ack reports partial grants by omission", func(t *testing.T) {
		// Two channels requested, one granted. There is no denial list — the
		// diff against the request is the only signal a channel was refused.
		ack := decodeAs[*wireSubscriptionAck](t, `{"type":"subscription_ack","subscribed":["acme.x"],"count":1}`)
		if len(ack.Subscribed) != 1 || ack.Subscribed[0] != "acme.x" {
			t.Errorf("subscribed = %v, want exactly [acme.x]", ack.Subscribed)
		}
	})

	t.Run("gap tolerates an empty last_pos", func(t *testing.T) {
		// A backend that keeps no cursors sends the field empty. Rejecting the
		// frame would discard a real data-loss signal.
		gap := decodeAs[*wireGap](t, `{"type":"gap","channel":"acme.x","last_pos":""}`)
		if gap.LastPos != "" {
			t.Errorf("last_pos = %q, want empty", gap.LastPos)
		}
		if gap.Channel != "acme.x" {
			t.Errorf("channel = %q, want acme.x", gap.Channel)
		}
	})

	t.Run("history_complete truncated defaults to false", func(t *testing.T) {
		hc := decodeAs[*wireHistoryComplete](t, `{"type":"history_complete","channel":"acme.x","count":7,"source":"kafka"}`)
		if hc.Truncated {
			t.Error("truncated = true for a frame that did not mention it")
		}
		if hc.Source != "kafka" {
			t.Errorf("source = %q, want kafka", hc.Source)
		}
	})

	t.Run("message history flag defaults to false", func(t *testing.T) {
		msg := decodeAs[*wireMessage](t, `{"type":"message","channel":"acme.x","seq":1,"ts":2,"data":{}}`)
		if msg.History {
			t.Error("history = true for a live frame that did not mention it")
		}
		if msg.Pos != "" {
			t.Errorf("pos = %q, want empty for an absent cursor", msg.Pos)
		}
	})

	t.Run("auth_ack exp is read from the nested payload", func(t *testing.T) {
		// A real expiry first. Asserting only the zero case would pass even if
		// the field were dropped entirely — which is exactly how the flat
		// version of this struct survived its own test.
		ack := decodeAs[*wireAuthAck](t, `{"type":"auth_ack","data":{"exp":1706003600}}`)
		if ack.Data.Exp != 1706003600 {
			t.Errorf("exp = %d, want 1706003600 — a dropped expiry reads as non-expiring and disarms refresh", ack.Data.Exp)
		}
	})

	t.Run("auth_ack exp zero means non-expiring", func(t *testing.T) {
		// Not a missing value: a credential with no expiry is a real case the
		// refresh scheduler must recognise rather than treat as expired-at-epoch.
		ack := decodeAs[*wireAuthAck](t, `{"type":"auth_ack","data":{"exp":0}}`)
		if ack.Data.Exp != 0 {
			t.Errorf("exp = %d, want 0", ack.Data.Exp)
		}
	})

	t.Run("auth_error code survives decoding", func(t *testing.T) {
		// Code drives the retry decision; collapsing every refusal into an
		// empty string would make rate_limited indistinguishable from
		// tenant_mismatch.
		authErr := decodeAs[*wireAuthError](t, `{"type":"auth_error","data":{"code":"rate_limited","message":"slow down"}}`)
		if authErr.Data.Code != "rate_limited" {
			t.Errorf("code = %q, want rate_limited", authErr.Data.Code)
		}
		if authErr.Data.Message != "slow down" {
			t.Errorf("message = %q, want %q", authErr.Data.Message, "slow down")
		}
	})

	t.Run("reconnect_ack with zero replayed is success", func(t *testing.T) {
		// A cursorless backend answers this way. Treating it as a failure would
		// leave a caller waiting for an error that never comes.
		ack := decodeAs[*wireReconnectAck](t, `{"type":"reconnect_ack","status":"completed","messages_replayed":0}`)
		if ack.Status != "completed" {
			t.Errorf("status = %q, want completed", ack.Status)
		}
		if ack.MessagesReplayed != 0 {
			t.Errorf("messages_replayed = %d, want 0", ack.MessagesReplayed)
		}
	})

	t.Run("large seq and ts survive as int64", func(t *testing.T) {
		// Millisecond timestamps exceed int32 and float64's exact-integer range
		// is the trap: decoding through float64 would silently round.
		msg := decodeAs[*wireMessage](t, `{"type":"message","channel":"acme.x","seq":9007199254740993,"ts":1755820800123,"data":{}}`)
		if msg.Seq != 9007199254740993 {
			t.Errorf("seq = %d, want 9007199254740993 — precision was lost", msg.Seq)
		}
		if msg.TS != 1755820800123 {
			t.Errorf("ts = %d, want 1755820800123", msg.TS)
		}
	})

	t.Run("data is preserved verbatim", func(t *testing.T) {
		// The SDK does not interpret payloads, so it must not normalize them
		// either — key order, number formatting, and precision all survive.
		const payload = `{"amount":1.10,"b":2,"a":1}`
		msg := decodeAs[*wireMessage](t, `{"type":"message","channel":"acme.x","seq":1,"ts":2,"data":`+payload+`}`)
		if string(msg.Data) != payload {
			t.Errorf("data = %s, want %s verbatim", msg.Data, payload)
		}
	})
}

// TestErrorFrameDispatch pins the rule for the contract's one overloaded frame.
//
// The generic error type carries both connection-level failures and per-channel
// replay rejections, and presence of a channel is the only disambiguator on
// offer. Keying on it is stateless: the alternative — deciding by whether a
// replay is in flight — gives the same bytes two meanings depending on timing.
func TestErrorFrameDispatch(t *testing.T) {
	tests := []struct {
		name        string
		frame       *wireError
		wantReplay  bool
		wantChannel string
	}{
		{
			// invalid_json is a contract-defined code for this frame. Asserting
			// on a code the contract does not list would test the dispatch rule
			// against traffic the server never sends.
			name:       "no channel is connection-level",
			frame:      &wireError{Type: typeError, Code: "invalid_json", Message: "Message is not valid JSON"},
			wantReplay: false,
		},
		{
			name:        "channel present is a replay rejection",
			frame:       &wireError{Type: typeError, Code: "replay_rate_limited", Channel: "acme.orders.eu"},
			wantReplay:  true,
			wantChannel: "acme.orders.eu",
		},
		{
			name:        "not_available with a channel is still a replay rejection",
			frame:       &wireError{Type: typeError, Code: CodeNotAvailable, Channel: "acme.orders.eu"},
			wantReplay:  true,
			wantChannel: "acme.orders.eu",
		},
		{
			name:       "not_available without a channel is connection-level",
			frame:      &wireError{Type: typeError, Code: CodeNotAvailable},
			wantReplay: false,
		},
		{
			// invalid_request is the other code the contract places in both
			// roles, so it exercises the same rule from the opposite side:
			// identical code, opposite classification, decided only by channel.
			name:        "invalid_request with a channel is a replay rejection",
			frame:       &wireError{Type: typeError, Code: "invalid_request", Channel: "acme.orders.eu"},
			wantReplay:  true,
			wantChannel: "acme.orders.eu",
		},
		{
			name:       "invalid_request without a channel is connection-level",
			frame:      &wireError{Type: typeError, Code: "invalid_request", Message: "unparseable frame"},
			wantReplay: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyErrorFrame(tc.frame)

			var re *ReplayError
			if got := errors.As(err, &re); got != tc.wantReplay {
				t.Fatalf("ReplayError = %v, want %v (got %T: %v)", got, tc.wantReplay, err, err)
			}

			if tc.wantReplay {
				if re.Channel != tc.wantChannel {
					t.Errorf("channel = %q, want %q", re.Channel, tc.wantChannel)
				}
				if re.Code != tc.frame.Code {
					t.Errorf("code = %q, want %q", re.Code, tc.frame.Code)
				}
				return
			}

			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("got %T, want *ProtocolError", err)
			}
			if pe.Code != tc.frame.Code {
				t.Errorf("code = %q, want %q", pe.Code, tc.frame.Code)
			}
		})
	}
}

// TestReplayErrorBridgesOnlyForNotAvailable guards the sentinel bridge against
// over-matching. A rate-limited replay is retryable and a not-available one is
// not; collapsing them behind one sentinel would make callers retry forever
// against a backend that will never serve the request.
func TestReplayErrorBridgesOnlyForNotAvailable(t *testing.T) {
	notAvailable := classifyErrorFrame(&wireError{Type: typeError, Code: CodeNotAvailable, Channel: "acme.x"})
	if !errors.Is(notAvailable, ErrNotAvailable) {
		t.Error("not_available replay error does not match ErrNotAvailable")
	}

	rateLimited := classifyErrorFrame(&wireError{Type: typeError, Code: "replay_rate_limited", Channel: "acme.x"})
	if errors.Is(rateLimited, ErrNotAvailable) {
		t.Error("rate-limited replay error matches ErrNotAvailable — a retryable condition reads as permanent")
	}
}

// TestSendFramesEncodeToContractShape covers the emit direction. Each row is a
// frame the SDK produces; the assertion is the exact JSON the server will read,
// including which optional fields are omitted.
func TestSendFramesEncodeToContractShape(t *testing.T) {
	tests := []struct {
		name  string
		frame any
		want  string
	}{
		{
			name:  "subscribe",
			frame: wireSubscribe{Type: typeSubscribe, Data: subscribePayload{Channels: []string{"acme.orders.eu"}}},
			want:  `{"type":"subscribe","data":{"channels":["acme.orders.eu"]}}`,
		},
		{
			name:  "unsubscribe",
			frame: wireUnsubscribe{Type: typeUnsubscribe, Data: unsubscribePayload{Channels: []string{"acme.orders.eu"}}},
			want:  `{"type":"unsubscribe","data":{"channels":["acme.orders.eu"]}}`,
		},
		{
			name: "publish",
			// The caller's payload nests under a second `data` — the contract's
			// doubling, not a modelling accident.
			frame: wirePublish{Type: typePublish, Data: publishPayload{Channel: "acme.orders.eu", Data: json.RawMessage(`{"id":1}`)}},
			want:  `{"type":"publish","data":{"channel":"acme.orders.eu","data":{"id":1}}}`,
		},
		{
			name: "reconnect with cursors",
			// Keys are full tenant-prefixed channels. A bare suffix silently
			// replays nothing, which is the worst outcome for a recovery request.
			frame: wireReconnect{Type: typeReconnect, Data: reconnectPayload{ClientID: "c-1", LastPos: map[string]string{"acme.orders.eu": "0-42"}}},
			want:  `{"type":"reconnect","data":{"client_id":"c-1","last_pos":{"acme.orders.eu":"0-42"}}}`,
		},
		{
			name: "reconnect with no cursors still sends last_pos",
			// The contract marks last_pos required. A client with nothing to
			// resume from says so with an empty object; dropping the field
			// would leave the server to infer an absence instead of reading a
			// statement.
			frame: wireReconnect{Type: typeReconnect, Data: reconnectPayload{ClientID: "c-1", LastPos: map[string]string{}}},
			want:  `{"type":"reconnect","data":{"client_id":"c-1","last_pos":{}}}`,
		},
		{
			name:  "heartbeat carries no data",
			frame: wireHeartbeat{Type: typeHeartbeat},
			want:  `{"type":"heartbeat"}`,
		},
		{
			name:  "auth",
			frame: wireAuth{Type: typeAuth, Data: authPayload{Token: "t-abc"}},
			want:  `{"type":"auth","data":{"token":"t-abc"}}`,
		},
		{
			name:  "history",
			frame: wireHistory{Type: typeHistory, Data: historyPayload{Channel: "acme.orders.eu", Limit: 100}},
			want:  `{"type":"history","data":{"channel":"acme.orders.eu","limit":100}}`,
		},
		{
			name:  "replay",
			frame: wireReplay{Type: typeReplay, Data: replayPayload{Channel: "acme.orders.eu", FromPos: "0-42"}},
			want:  `{"type":"replay","data":{"channel":"acme.orders.eu","from_pos":"0-42"}}`,
		},
	}

	// Coverage is measured in types, not rows — some types need more than one
	// row to pin their optional fields. The covered set is read back out of the
	// encoded frames rather than declared alongside them, which also proves each
	// frame announces the type it is supposed to be.
	covered := make(map[string]bool, len(sendRegistry))

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := mustMarshal(t, tc.frame)
			if got := string(encoded); got != tc.want {
				t.Errorf("encoded to\n  %s\nwant\n  %s", got, tc.want)
			}

			var check envelope
			if err := json.Unmarshal(encoded, &check); err != nil {
				t.Fatalf("re-read of encoded frame failed: %v", err)
			}
			if _, ok := sendRegistry[check.Type]; !ok {
				t.Fatalf("frame declares type %q, which is not a client-originated type", check.Type)
			}
			covered[check.Type] = true
		})
	}

	for wireType := range sendRegistry {
		if !covered[wireType] {
			t.Errorf("send type %q has no encoding test", wireType)
		}
	}
}

// ─── helpers ───

// loadFixtures reads a fixture directory into a name→bytes map. Reading the
// directory rather than listing filenames in the test is what lets the
// completeness checks above compare the two sets in both directions.
func loadFixtures(t *testing.T, kind string) map[string][]byte {
	t.Helper()

	dir := filepath.Join("testdata", "fixtures", kind)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read fixture dir %s: %v", dir, err)
	}

	out := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read fixture %s: %v", entry.Name(), err)
		}
		out[entry.Name()] = data
	}
	if len(out) == 0 {
		t.Fatalf("no fixtures found in %s", dir)
	}
	return out
}

// decodeAs decodes a frame and asserts its concrete type, so the tests above
// read as statements about field semantics rather than about type assertions.
func decodeAs[T any](t *testing.T, frame string) T {
	t.Helper()

	decoded, unknown, err := decodeFrame([]byte(frame))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if unknown != "" {
		t.Fatalf("type %q was not recognised", unknown)
	}

	typed, ok := decoded.(T)
	if !ok {
		var want T
		t.Fatalf("decoded to %T, want %T", decoded, want)
	}
	return typed
}

// jsonDiff compares two frames as decoded trees and describes what differs,
// returning "" when they carry the same content. Byte comparison would fail on
// key order, which JSON does not define and Go's encoder does not preserve.
func jsonDiff(t *testing.T, want, got []byte) string {
	t.Helper()

	var wantTree, gotTree map[string]any
	if err := json.Unmarshal(want, &wantTree); err != nil {
		t.Fatalf("parse expected frame: %v", err)
	}
	if err := json.Unmarshal(got, &gotTree); err != nil {
		t.Fatalf("parse re-encoded frame: %v", err)
	}

	var problems []string
	for key, wantVal := range wantTree {
		gotVal, present := gotTree[key]
		switch {
		case !present:
			problems = append(problems, fmt.Sprintf("  field %q was dropped (contract value: %v)", key, wantVal))
		case !reflect.DeepEqual(wantVal, gotVal):
			problems = append(problems, fmt.Sprintf("  field %q changed: %v → %v", key, wantVal, gotVal))
		}
	}
	for key, gotVal := range gotTree {
		if _, present := wantTree[key]; !present {
			problems = append(problems, fmt.Sprintf("  field %q was invented: %v", key, gotVal))
		}
	}

	sort.Strings(problems)
	return strings.Join(problems, "\n")
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()

	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	return data
}
