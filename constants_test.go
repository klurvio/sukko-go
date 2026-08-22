package sukko

import "testing"

// TestEnumStringForms pins the wire/string form of every exported enum.
//
// These strings are public API frozen from v0.1.0 and several are contract
// values (PushPlatform mirrors the gateway's closed `enum: [web, android, ios]`;
// HistorySource mirrors `history_complete.source`). ConnectionState's six values
// are asserted verbatim per NFR-005(oo) — including StateClosed == "closed",
// the resting state a clean stop terminates in.
func TestEnumStringForms(t *testing.T) {
	t.Parallel()

	t.Run("ConnectionState", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			state ConnectionState
			want  string
		}{
			{StateDisconnected, "disconnected"},
			{StateConnecting, "connecting"},
			{StateConnected, "connected"},
			{StateReconnecting, "reconnecting"},
			{StateError, "error"},
			{StateClosed, "closed"},
		} {
			if got := tc.state.String(); got != tc.want {
				t.Errorf("ConnectionState.String() = %q, want %q", got, tc.want)
			}
		}
	})

	t.Run("MessageSource", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			source MessageSource
			want   string
		}{
			{SourceLive, "live"},
			{SourceHistory, "history"},
			{SourceReplay, "replay"},
		} {
			if got := tc.source.String(); got != tc.want {
				t.Errorf("MessageSource.String() = %q, want %q", got, tc.want)
			}
		}
	})

	t.Run("HistorySource", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			source HistorySource
			want   string
		}{
			{HistorySourceCache, "cache"},
			{HistorySourceKafka, "kafka"},
			{HistorySourceMixed, "mixed"},
		} {
			if got := tc.source.String(); got != tc.want {
				t.Errorf("HistorySource.String() = %q, want %q", got, tc.want)
			}
		}
	})

	t.Run("Edition", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			edition Edition
			want    string
		}{
			{EditionPro, "pro"},
			{EditionEnterprise, "enterprise"},
		} {
			if got := tc.edition.String(); got != tc.want {
				t.Errorf("Edition.String() = %q, want %q", got, tc.want)
			}
		}
	})

	t.Run("AuthMode", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			mode AuthMode
			want string
		}{
			{AuthRefresh, "refresh"},
			{AuthEscalation, "escalation"},
		} {
			if got := tc.mode.String(); got != tc.want {
				t.Errorf("AuthMode.String() = %q, want %q", got, tc.want)
			}
		}
	})

	// PushPlatform values are contract values — the gateway's push subscribe
	// endpoint declares a closed enum, so these strings must match it exactly.
	t.Run("PushPlatform", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			platform PushPlatform
			want     string
		}{
			{PlatformWeb, "web"},
			{PlatformAndroid, "android"},
			{PlatformIOS, "ios"},
		} {
			if got := tc.platform.String(); got != tc.want {
				t.Errorf("PushPlatform.String() = %q, want %q", got, tc.want)
			}
		}
	})

	t.Run("TransportKind", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			kind TransportKind
			want string
		}{
			{TransportWebSocket, "websocket"},
			{TransportSSE, "sse"},
		} {
			if got := tc.kind.String(); got != tc.want {
				t.Errorf("TransportKind.String() = %q, want %q", got, tc.want)
			}
		}
	})
}

// TestEnumZeroValues pins each enum's zero value.
//
// A Go zero value is what a caller gets from `var s ConnectionState` or an
// unset struct field, so it is API whether or not it is chosen deliberately.
// StateDisconnected is the zero ConnectionState because it is the state a
// client legitimately occupies before Connect. The remaining enums have no
// meaningful zero, so theirs is an explicit invalid sentinel that stringifies
// recognisably rather than masquerading as a valid value.
func TestEnumZeroValues(t *testing.T) {
	t.Parallel()

	if got := ConnectionState(0); got != StateDisconnected {
		t.Errorf("zero ConnectionState = %v, want StateDisconnected", got)
	}

	for _, tc := range []struct {
		name string
		got  string
	}{
		{"MessageSource", MessageSource(0).String()},
		{"HistorySource", HistorySource(0).String()},
		{"Edition", Edition(0).String()},
		{"AuthMode", AuthMode(0).String()},
		{"PushPlatform", PushPlatform(0).String()},
		{"TransportKind", TransportKind(0).String()},
	} {
		if tc.got != "unknown" {
			t.Errorf("zero %s.String() = %q, want %q", tc.name, tc.got, "unknown")
		}
	}
}

// TestEnumStringOutOfRange ensures an out-of-range value degrades to a
// recognisable form instead of panicking or indexing past a lookup table —
// String() is reached from log lines and error messages on failure paths.
func TestEnumStringOutOfRange(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		got  string
	}{
		{"ConnectionState", ConnectionState(99).String()},
		{"MessageSource", MessageSource(99).String()},
		{"HistorySource", HistorySource(99).String()},
		{"Edition", Edition(99).String()},
		{"AuthMode", AuthMode(99).String()},
		{"PushPlatform", PushPlatform(99).String()},
		{"TransportKind", TransportKind(99).String()},
	} {
		if tc.got != "unknown" {
			t.Errorf("out-of-range %s.String() = %q, want %q", tc.name, tc.got, "unknown")
		}
	}
}
