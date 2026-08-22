package sukko

import (
	"encoding/json"
	"testing"
)

// TestEnumWireValues pins each enum constant to its exact underlying string.
//
// These are string-backed types, and for several the underlying value IS the
// wire value: PushPlatform mirrors the gateway's closed push-subscribe enum and
// HistorySource mirrors `history_complete.source`. Pinning the underlying string
// (not just String()) is what keeps encoding correct — see TestEnumJSONRoundTrip.
// ConnectionState's six values are asserted verbatim per NFR-005(oo), including
// StateClosed == "closed".
func TestEnumWireValues(t *testing.T) {
	t.Parallel()

	t.Run("ConnectionState", func(t *testing.T) {
		t.Parallel()
		for want, got := range map[string]ConnectionState{
			"disconnected": StateDisconnected,
			"connecting":   StateConnecting,
			"connected":    StateConnected,
			"reconnecting": StateReconnecting,
			"error":        StateError,
			"closed":       StateClosed,
		} {
			if string(got) != want {
				t.Errorf("ConnectionState = %q, want %q", string(got), want)
			}
			if got.String() != want {
				t.Errorf("ConnectionState.String() = %q, want %q", got.String(), want)
			}
		}
	})

	t.Run("MessageSource", func(t *testing.T) {
		t.Parallel()
		for want, got := range map[string]MessageSource{
			"live":    SourceLive,
			"history": SourceHistory,
			"replay":  SourceReplay,
		} {
			if string(got) != want || got.String() != want {
				t.Errorf("MessageSource = %q/%q, want %q", string(got), got.String(), want)
			}
		}
	})

	t.Run("HistorySource", func(t *testing.T) {
		t.Parallel()
		for want, got := range map[string]HistorySource{
			"cache": HistorySourceCache,
			"kafka": HistorySourceKafka,
			"mixed": HistorySourceMixed,
		} {
			if string(got) != want || got.String() != want {
				t.Errorf("HistorySource = %q/%q, want %q", string(got), got.String(), want)
			}
		}
	})

	t.Run("Edition", func(t *testing.T) {
		t.Parallel()
		for want, got := range map[string]Edition{
			"pro":        EditionPro,
			"enterprise": EditionEnterprise,
		} {
			if string(got) != want || got.String() != want {
				t.Errorf("Edition = %q/%q, want %q", string(got), got.String(), want)
			}
		}
	})

	t.Run("AuthMode", func(t *testing.T) {
		t.Parallel()
		for want, got := range map[string]AuthMode{
			"refresh":    AuthRefresh,
			"escalation": AuthEscalation,
		} {
			if string(got) != want || got.String() != want {
				t.Errorf("AuthMode = %q/%q, want %q", string(got), got.String(), want)
			}
		}
	})

	t.Run("PushPlatform", func(t *testing.T) {
		t.Parallel()
		for want, got := range map[string]PushPlatform{
			"web":     PlatformWeb,
			"android": PlatformAndroid,
			"ios":     PlatformIOS,
		} {
			if string(got) != want || got.String() != want {
				t.Errorf("PushPlatform = %q/%q, want %q", string(got), got.String(), want)
			}
		}
	})

	t.Run("TransportKind", func(t *testing.T) {
		t.Parallel()
		for want, got := range map[string]TransportKind{
			"websocket": TransportWebSocket,
			"sse":       TransportSSE,
		} {
			if string(got) != want || got.String() != want {
				t.Errorf("TransportKind = %q/%q, want %q", string(got), got.String(), want)
			}
		}
	})
}

// TestEnumJSONRoundTrip is the reason these types are string-backed rather than
// int-backed: the contract-valued enums must encode as their wire strings with
// no conversion step. An int-backed PushPlatform would marshal as a number and
// the gateway would reject the push-subscribe request.
func TestEnumJSONRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("marshals to the wire string", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name string
			give any
			want string
		}{
			{"PushPlatform", PlatformAndroid, `"android"`},
			{"HistorySource", HistorySourceMixed, `"mixed"`},
			{"MessageSource", SourceReplay, `"replay"`},
			{"ConnectionState", StateClosed, `"closed"`},
		} {
			got, err := json.Marshal(tc.give)
			if err != nil {
				t.Fatalf("%s: marshal: %v", tc.name, err)
			}
			if string(got) != tc.want {
				t.Errorf("%s marshalled to %s, want %s", tc.name, got, tc.want)
			}
		}
	})

	t.Run("unmarshals from the wire string", func(t *testing.T) {
		t.Parallel()
		var platform PushPlatform
		if err := json.Unmarshal([]byte(`"ios"`), &platform); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if platform != PlatformIOS {
			t.Errorf("unmarshalled to %q, want %q", platform, PlatformIOS)
		}

		var source HistorySource
		if err := json.Unmarshal([]byte(`"kafka"`), &source); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if source != HistorySourceKafka {
			t.Errorf("unmarshalled to %q, want %q", source, HistorySourceKafka)
		}
	})
}

// TestEnumZeroValue pins the zero value of each enum.
//
// A Go zero value is what a caller gets from `var s ConnectionState` or an unset
// struct field, so it is API whether or not it was chosen deliberately. For a
// string-backed type the zero is "", which is not a valid value for any of these
// enums — deliberately, so an unset field cannot masquerade as a valid one.
// ConnectionState is the one enum whose zero is meaningful.
func TestEnumZeroValue(t *testing.T) {
	t.Parallel()

	if StateDisconnected != "disconnected" {
		t.Errorf("StateDisconnected = %q, want %q", StateDisconnected, "disconnected")
	}
	if got := ConnectionState(""); got == StateDisconnected {
		t.Error("the empty ConnectionState must not equal StateDisconnected")
	}

	for name, got := range map[string]string{
		"MessageSource":   string(MessageSource("")),
		"HistorySource":   string(HistorySource("")),
		"Edition":         string(Edition("")),
		"AuthMode":        string(AuthMode("")),
		"PushPlatform":    string(PushPlatform("")),
		"TransportKind":   string(TransportKind("")),
		"ConnectionState": string(ConnectionState("")),
	} {
		if got != "" {
			t.Errorf("zero %s = %q, want the empty string", name, got)
		}
	}
}

// TestEnumStringIsTotal ensures String() never panics and never invents a value
// for an out-of-set string — it is reached from log lines and error messages on
// failure paths, including when a server sends something the SDK does not know.
func TestEnumStringIsTotal(t *testing.T) {
	t.Parallel()

	if got := PushPlatform("wearos").String(); got != "wearos" {
		t.Errorf("unknown PushPlatform.String() = %q, want the value verbatim", got)
	}
	if got := ConnectionState("").String(); got != "" {
		t.Errorf("empty ConnectionState.String() = %q, want the empty string", got)
	}
}
