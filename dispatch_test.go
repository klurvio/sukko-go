package sukko

import (
	"errors"
	"fmt"
	"testing"
)

// TestEveryWireTypeHasADisposition is the completeness guard for the mapping,
// the decode-side twin of Phase 2's registry-coverage test: every server type
// the SDK decodes must resolve to exactly one disposition in FR-001a's table, so
// a new decodable type cannot be added without deciding how it reaches the
// caller.
func TestEveryWireTypeHasADisposition(t *testing.T) {
	for wireType := range decodeRegistry {
		if _, ok := dispositionTable[wireType]; !ok {
			t.Errorf("decodable type %q has no disposition — FR-001a's table must place it", wireType)
		}
	}
	for wireType := range dispositionTable {
		if _, ok := decodeRegistry[wireType]; !ok {
			t.Errorf("disposition table lists %q, which is not a decodable type", wireType)
		}
	}
}

// TestSurfaceEventMapsGoldenFixtures decodes every "surface" golden fixture and
// asserts it maps to the concrete Event type FR-001a's table prescribes.
//
// It runs off the same contract-derived fixtures as Phase 2, so the mapping is
// checked against the server's own frame shapes, not against hand-built inputs.
func TestSurfaceEventMapsGoldenFixtures(t *testing.T) {
	// Expected concrete Event type per wire fixture, for the dispSurface rows.
	want := map[string]string{
		"message":            "*sukko.Message",
		"replay_message":     "*sukko.Message",
		"gap":                "*sukko.Gap",
		"unsubscription_ack": "*sukko.Unsubscribed",
		"publish_ack":        "*sukko.PublishAccepted",
		"publish_error":      "*sukko.PublishError",
		"reconnect_ack":      "*sukko.Resumed",
		"reconnect_error":    "*sukko.ReconnectError",
		"auth_error":         "*sukko.AuthError",
		"history_complete":   "*sukko.HistoryComplete",
		"replay_complete":    "*sukko.ReplayComplete",
		"history_error":      "*sukko.HistoryError",
		"subscribe_error":    "*sukko.ProtocolError",
		"unsubscribe_error":  "*sukko.ProtocolError",
		"error":              "*sukko.ProtocolError", // fixture error.json has no channel
	}

	// Bind the expectation set to the disposition table in both directions: a
	// new dispSurface type added without a mapping expectation here, or a want
	// entry that is not actually a surface type, fails rather than going
	// untested. Without this the loop below would simply skip anything missing
	// from want.
	for wireType, disp := range dispositionTable {
		if disp == dispSurface {
			if _, ok := want[wireType]; !ok {
				t.Errorf("dispSurface type %q has no mapping expectation — add it to want", wireType)
			}
		}
	}
	for wireType := range want {
		if dispositionTable[wireType] != dispSurface {
			t.Errorf("want lists %q, which is not a dispSurface type", wireType)
		}
	}

	fixtures := loadFixtures(t, "valid")
	for wireType, wantType := range want {
		t.Run(wireType, func(t *testing.T) {
			if dispositionTable[wireType] != dispSurface {
				t.Fatalf("%s is expected to be a surface type but its disposition is %v", wireType, dispositionTable[wireType])
			}
			data, ok := fixtures[wireType+".json"]
			if !ok {
				t.Fatalf("no fixture for %s", wireType)
			}
			decoded, unknown, err := decodeFrame(data)
			if err != nil || unknown != "" {
				t.Fatalf("decode failed: err=%v unknown=%q", err, unknown)
			}

			ev, err := surfaceEvent(decoded)
			if err != nil {
				t.Fatalf("surfaceEvent returned an error: %v", err)
			}
			if got := fmt.Sprintf("%T", ev); got != wantType {
				t.Errorf("%s surfaced %s, want %s", wireType, got, wantType)
			}
		})
	}
}

// TestMessageSourceFromFrame pins the frame-derived Source: live by default,
// history when the frame's history flag is set, replay for a replay_message.
// The replay-window override (a live message becoming SourceReplay inside the
// reconnect window) is client state and lands in Phase 5.
func TestMessageSourceFromFrame(t *testing.T) {
	tests := []struct {
		name  string
		frame string
		want  MessageSource
	}{
		{"live", `{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`, SourceLive},
		{"history", `{"type":"message","channel":"acme.x","seq":1,"ts":1,"history":true,"data":{}}`, SourceHistory},
		{"replay", `{"type":"replay_message","channel":"acme.x","seq":1,"ts":1,"data":{}}`, SourceReplay},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded, _, err := decodeFrame([]byte(tc.frame))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			ev, err := surfaceEvent(decoded)
			if err != nil {
				t.Fatalf("surfaceEvent: %v", err)
			}
			msg, ok := ev.(*Message)
			if !ok {
				t.Fatalf("surfaced %T, want *Message", ev)
			}
			if msg.Source != tc.want {
				t.Errorf("Source = %s, want %s", msg.Source, tc.want)
			}
		})
	}
}

// TestPresenceRejectsEmptyChannel is the T024a §II check. It validates only
// fields where empty is itself invalid — an empty channel is unusable however it
// arose — because after decode a missing string and an empty string are
// indistinguishable. gap's empty last_pos is deliberately NOT rejected: FR-006
// surfaces it and pairs a PossibleGap, so it is a valid frame, not a malformed one.
func TestPresenceRejectsEmptyChannel(t *testing.T) {
	rejected := []struct {
		name  string
		frame string
	}{
		{"message without channel", `{"type":"message","seq":1,"ts":1,"data":{}}`},
		{"gap without channel", `{"type":"gap","last_pos":"0-1","from_seq":1,"to_seq":2,"ts":1}`},
		{"publish_ack without channel", `{"type":"publish_ack","status":"accepted"}`},
		{"history_complete without channel", `{"type":"history_complete","count":1,"source":"kafka"}`},
		{"replay_complete without channel", `{"type":"replay_complete","messages_replayed":1}`},
		{"history_error without channel", `{"type":"history_error","code":"not_available","message":"no history"}`},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			decoded, _, err := decodeFrame([]byte(tc.frame))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			ev, err := surfaceEvent(decoded)
			if err == nil {
				t.Fatalf("empty channel was accepted, surfaced %T", ev)
			}
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Errorf("got %T, want *ProtocolError", err)
			}
		})
	}

	// The counter-case: gap WITH an empty last_pos is valid and surfaces a *Gap.
	t.Run("gap with empty last_pos is valid", func(t *testing.T) {
		decoded, _, err := decodeFrame([]byte(`{"type":"gap","channel":"acme.x","last_pos":"","from_seq":1,"to_seq":2,"ts":1}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		ev, err := surfaceEvent(decoded)
		if err != nil {
			t.Fatalf("empty last_pos was rejected but is valid: %v", err)
		}
		gap, ok := ev.(*Gap)
		if !ok || gap.LastPos != "" {
			t.Errorf("want *Gap with empty LastPos, got %T %+v", ev, ev)
		}
	})
}

// TestUnknownHistorySourceIsPreserved pins the forward-compatibility choice: an
// unknown source is a well-formed frame carrying a value the SDK predates, so it
// is surfaced verbatim (like an unknown error code), not rejected — rejecting a
// history_complete would also drop the single-flight terminator.
func TestUnknownHistorySourceIsPreserved(t *testing.T) {
	decoded, _, err := decodeFrame([]byte(`{"type":"history_complete","channel":"acme.x","count":1,"source":"valkey"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev, err := surfaceEvent(decoded)
	if err != nil {
		t.Fatalf("an unknown source must not be rejected: %v", err)
	}
	hc, ok := ev.(*HistoryComplete)
	if !ok {
		t.Fatalf("surfaced %T, want *HistoryComplete", ev)
	}
	if hc.Source != HistorySource("valkey") {
		t.Errorf("Source = %q, want the raw \"valkey\" preserved", hc.Source)
	}
}

// TestClientDerivedAndSilentDispositions pins the three rows that surfaceEvent
// does not handle: the two needing SDK state and the one that surfaces nothing.
func TestClientDerivedAndSilentDispositions(t *testing.T) {
	if dispositionTable[typeSubscriptionAck] != dispClientDerived {
		t.Error("subscription_ack must be client-derived (the grant diff needs the requested set)")
	}
	if dispositionTable[typeAuthAck] != dispClientDerived {
		t.Error("auth_ack must be client-derived (Authenticated.Mode is SDK-owned state)")
	}
	if dispositionTable[typePong] != dispInternalSilent {
		t.Error("pong must be internally silent — the one wire type with no caller-visible form")
	}
}

// Mid — the stable message identity — must surface on the public Message across
// every provenance (live, history, replay), so a caller can deduplicate a record
// it already saw on a reconnect-replay overlap.
func TestMessageMidSurfaced(t *testing.T) {
	tests := []struct {
		name  string
		frame string
		want  string
	}{
		{"live", `{"type":"message","channel":"acme.x","seq":1,"ts":1,"mid":"cafe-2-9","data":{}}`, "cafe-2-9"},
		{"history", `{"type":"message","channel":"acme.x","seq":1,"ts":1,"history":true,"mid":"cafe-2-9","data":{}}`, "cafe-2-9"},
		{"replay", `{"type":"replay_message","channel":"acme.x","seq":1,"ts":1,"mid":"cafe-2-9","data":{}}`, "cafe-2-9"},
		{"absent (old server)", `{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded, _, err := decodeFrame([]byte(tc.frame))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			ev, err := surfaceEvent(decoded)
			if err != nil {
				t.Fatalf("surfaceEvent: %v", err)
			}
			msg, ok := ev.(*Message)
			if !ok {
				t.Fatalf("surfaced %T, want *Message", ev)
			}
			if msg.Mid != tc.want {
				t.Errorf("Mid = %q, want %q", msg.Mid, tc.want)
			}
		})
	}
}

// A WS publish_ack carries the stable message identity the gateway assigned; it
// must reach the caller on PublishAccepted, symmetric with PublishResult.Mid on
// the REST path, so a publisher can correlate its publish with the delivered
// copy. Absent (multi-topic fan-out / old gateway) → empty.
func TestPublishAcceptedMidSurfaced(t *testing.T) {
	tests := []struct {
		name  string
		frame string
		want  string
	}{
		{"present", `{"type":"publish_ack","status":"accepted","channel":"acme.x","mid":"beef-1-7"}`, "beef-1-7"},
		{"absent", `{"type":"publish_ack","status":"accepted","channel":"acme.x"}`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded, _, err := decodeFrame([]byte(tc.frame))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			ev, err := surfaceEvent(decoded)
			if err != nil {
				t.Fatalf("surfaceEvent: %v", err)
			}
			ack, ok := ev.(*PublishAccepted)
			if !ok {
				t.Fatalf("surfaced %T, want *PublishAccepted", ev)
			}
			if ack.Mid != tc.want {
				t.Errorf("Mid = %q, want %q", ack.Mid, tc.want)
			}
		})
	}
}
