package sukko

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

// newTestConfig applies options over the defaults, the way NewClient will.
func newTestConfig(opts ...Option) *config {
	c := defaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// withCredential is the minimum every valid configuration needs, so a test
// exercising some other rule does not trip the credential check first.
func withCredential() Option { return WithToken("jwt") }

// TestDefaultsMatchTheirConstants asserts the zero-option configuration by
// value. The defaults are documented API; a change here should be a deliberate
// edit to a constant, not a side effect of touching the config struct.
func TestDefaultsMatchTheirConstants(t *testing.T) {
	t.Parallel()

	c := defaultConfig()

	for _, tc := range []struct {
		name      string
		got, want any
	}{
		{"queueSize", c.queueSize, DefaultQueueSize},
		{"historyLimit", c.historyLimit, DefaultHistoryLimit},
		{"maxPublishSize", c.maxPublishSize, DefaultMaxPublishSize},
		{"maxBackpressureReconnects", c.maxBackpressureReconnects, DefaultMaxBackpressureReconnects},
		{"dialTimeout", c.dialTimeout, DefaultDialTimeout},
		{"heartbeatInterval", c.heartbeatInterval, DefaultHeartbeatInterval},
		{"pongTimeout", c.pongTimeout, DefaultPongTimeout},
		{"refreshMinInterval", c.refreshMinInterval, DefaultRefreshMinInterval},
		{"refreshLead", c.refreshLead, DefaultRefreshLead},
		{"replayFloor", c.replayFloor, DefaultReplayFloor},
		{"recoveryDeadline", c.recoveryDeadline, DefaultRecoveryDeadline},
		{"sseIdleTimeout", c.sseIdleTimeout, DefaultSSEIdleTimeout},
		{"tokenSourceTimeout", c.tokenSourceTimeout, DefaultTokenSourceTimeout},
		{"backoff", c.backoff, DefaultBackoff},
		{"transport", c.transport, TransportWebSocket},
		{"reconnect", c.reconnect, true},
	} {
		if tc.got != tc.want {
			t.Errorf("default %s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}

	// A default HTTP client with a Timeout would cap a long-lived stream, so
	// the SDK ships one without.
	if c.httpClient == nil || c.httpClient.Timeout != 0 {
		t.Errorf("default httpClient must be non-nil with no Timeout, got %+v", c.httpClient)
	}
}

// TestZeroConfigIsValid is the SC-001 path: the quickstart sets no capacity or
// timing knob, so the defaults must satisfy the SDK's own validation. A default
// its constructor rejects would make the documented example fail.
func TestZeroConfigIsValid(t *testing.T) {
	t.Parallel()

	c := newTestConfig(withCredential())
	if err := c.validate("wss://gateway.example.com/ws"); err != nil {
		t.Fatalf("the zero-option configuration is invalid: %v", err)
	}
}

// Every option must actually reach the field it names — a copy-paste error in a
// setter is invisible until the knob silently does nothing.
func TestOptionsSetTheirFields(t *testing.T) {
	t.Parallel()

	custom := &http.Client{Timeout: time.Second}
	logger := slog.New(discardHandler{})
	clock := newFakeClock()
	rand := newFakeRand(0.25)
	src := func(context.Context) (Token, error) { return Token{Value: "t"}, nil }

	c := newTestConfig(
		WithToken("jwt"), WithAPIKey("key"), WithTokenSource(src),
		WithQueryParamAuth(), WithTokenSourceTimeout(11*time.Second),
		WithInsecureTransport(), WithTransport(TransportSSE),
		WithHTTPClient(custom), WithDialTimeout(12*time.Second),
		WithQueueSize(999), WithMaxPublishSize(4096),
		WithReconnect(false), WithMaxBackpressureReconnects(7),
		WithHistoryLimit(50), WithReplayFloor(13*time.Second),
		WithRecoveryDeadline(14*time.Second), WithClientID("client-1"),
		WithHeartbeatInterval(15*time.Second), WithPongTimeout(5*time.Second),
		WithSSEIdleTimeout(16*time.Second),
		WithRefreshMinInterval(17*time.Second), WithRefreshLead(18*time.Second),
		WithBackoff(BackoffConfig{Initial: time.Second, Max: time.Minute, Multiplier: 3, Jitter: 0.5}),
		WithLogger(logger), WithClock(clock), WithRand(rand),
	)

	for _, tc := range []struct {
		name      string
		got, want any
	}{
		{"token", c.token, "jwt"},
		{"apiKey", c.apiKey, "key"},
		{"queryParamAuth", c.queryParamAuth, true},
		{"tokenSourceTimeout", c.tokenSourceTimeout, 11 * time.Second},
		{"insecureTransport", c.insecureTransport, true},
		{"transport", c.transport, TransportSSE},
		{"dialTimeout", c.dialTimeout, 12 * time.Second},
		{"queueSize", c.queueSize, 999},
		{"maxPublishSize", c.maxPublishSize, 4096},
		{"reconnect", c.reconnect, false},
		{"maxBackpressureReconnects", c.maxBackpressureReconnects, 7},
		{"historyLimit", c.historyLimit, 50},
		{"replayFloor", c.replayFloor, 13 * time.Second},
		{"recoveryDeadline", c.recoveryDeadline, 14 * time.Second},
		{"clientID", c.clientID, "client-1"},
		{"heartbeatInterval", c.heartbeatInterval, 15 * time.Second},
		{"pongTimeout", c.pongTimeout, 5 * time.Second},
		{"sseIdleTimeout", c.sseIdleTimeout, 16 * time.Second},
		{"refreshMinInterval", c.refreshMinInterval, 17 * time.Second},
		{"refreshLead", c.refreshLead, 18 * time.Second},
		{"backoff.Multiplier", c.backoff.Multiplier, float64(3)},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}

	if c.tokenSource == nil {
		t.Error("WithTokenSource did not set the source")
	}
	if c.httpClient != custom {
		t.Error("WithHTTPClient did not set the client")
	}
	if c.clock != Clock(clock) {
		t.Error("WithClock did not set the clock")
	}
	if c.rand != Rand(rand) {
		t.Error("WithRand did not set the source")
	}
	if c.logger != logger {
		t.Error("WithLogger did not set the logger")
	}
}

// Transport security is scheme-based only. Both sides of the gate are asserted:
// a guard that rejected everything would pass a one-sided test.
func TestTransportSecurityGate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		url     string
		opts    []Option
		wantErr bool
	}{
		{"wss is accepted", "wss://gateway.example.com/ws", nil, false},
		{"https is accepted", "https://gateway.example.com/ws", nil, false},
		{"ws is rejected", "ws://gateway.example.com/ws", nil, true},
		{"http is rejected", "http://gateway.example.com/ws", nil, true},
		{"ws with the opt-in is accepted", "ws://gateway.example.com/ws", []Option{WithInsecureTransport()}, false},
		{"http with the opt-in is accepted", "http://gateway.example.com/ws", []Option{WithInsecureTransport()}, false},

		// The decision is the scheme and nothing else. localhost over plaintext
		// is still plaintext, and a remote host over TLS is still fine — a
		// hostname heuristic would get both of these wrong.
		{"localhost is not special", "ws://localhost:8080/ws", nil, true},
		{"127.0.0.1 is not special", "ws://127.0.0.1:8080/ws", nil, true},
		{"private range is not special", "ws://10.0.0.1/ws", nil, true},
		{"tls localhost is fine", "wss://localhost:8080/ws", nil, false},

		{"unknown scheme is rejected", "ftp://gateway.example.com", nil, true},
		{"empty url is rejected", "", nil, true},
		{"url without host is rejected", "wss:///ws", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := append([]Option{withCredential()}, tc.opts...)
			err := newTestConfig(opts...).validate(tc.url)
			if tc.wantErr && err == nil {
				t.Errorf("validate(%q) succeeded, want an error", tc.url)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validate(%q) errored: %v", tc.url, err)
			}
		})
	}
}

// Query-param auth over plaintext is refused unconditionally, and the insecure
// opt-in does not unlock it. Redaction removes a credential from this SDK's
// output; it cannot remove one from the wire.
func TestQueryParamAuthRequiresTLS(t *testing.T) {
	t.Parallel()

	err := newTestConfig(withCredential(), WithQueryParamAuth(), WithInsecureTransport()).
		validate("ws://gateway.example.com/ws")
	if err == nil {
		t.Fatal("query-param auth over plaintext was accepted with the insecure opt-in")
	}
	if !errors.Is(err, ErrInsecureTransport) {
		t.Errorf("error %v does not match ErrInsecureTransport", err)
	}

	// Over TLS it is permitted.
	if err := newTestConfig(withCredential(), WithQueryParamAuth()).
		validate("wss://gateway.example.com/ws"); err != nil {
		t.Errorf("query-param auth over TLS was rejected: %v", err)
	}
}

// A client must declare either a credential or that the deployment has auth
// disabled — never both, and never neither.
func TestCredentialDeclarationIsRequired(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{"token alone", []Option{WithToken("jwt")}, false},
		{"api key alone", []Option{WithAPIKey("key")}, false},
		{"token source alone", []Option{WithTokenSource(func(context.Context) (Token, error) { return Token{}, nil })}, false},
		{"token and api key compose", []Option{WithToken("jwt"), WithAPIKey("key")}, false},
		{"explicit no-auth", []Option{WithNoAuth()}, false},

		{"nothing at all", nil, true},
		{"no-auth with a credential is two meanings", []Option{WithNoAuth(), WithToken("jwt")}, true},
		{"no-auth with query-param auth has no credential to place", []Option{WithNoAuth(), WithQueryParamAuth()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := newTestConfig(tc.opts...).validate("wss://gateway.example.com/ws")
			if tc.wantErr && err == nil {
				t.Error("validate succeeded, want an error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validate errored: %v", err)
			}
		})
	}
}

// The QueueSize floor is a three-row boundary: below fails, at succeeds, above
// succeeds. Two rows would pass against a validator that rejected every value.
func TestQueueSizeFloorBoundary(t *testing.T) {
	t.Parallel()

	floor := DefaultHistoryLimit + MaxReplayMessages + AdvisoryHeadroom

	for _, tc := range []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"one below the floor", floor - 1, true},
		{"exactly at the floor", floor, false},
		{"one above the floor", floor + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := newTestConfig(withCredential(), WithQueueSize(tc.size)).
				validate("wss://gateway.example.com/ws")
			if tc.wantErr && err == nil {
				t.Errorf("QueueSize %d was accepted, want rejection", tc.size)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("QueueSize %d was rejected: %v", tc.size, err)
			}
		})
	}

	// The message must name the arithmetic, or a caller cannot tell what to
	// raise.
	err := newTestConfig(withCredential(), WithQueueSize(floor-1)).
		validate("wss://gateway.example.com/ws")
	if err == nil || !strings.Contains(err.Error(), "AdvisoryHeadroom") {
		t.Errorf("the floor error should explain the sum, got: %v", err)
	}
}

// The floor tracks HistoryLimit rather than being a fixed number, so lowering
// the history limit lowers the floor with it.
func TestQueueSizeFloorTracksHistoryLimit(t *testing.T) {
	t.Parallel()

	small := 10
	floor := small + MaxReplayMessages + AdvisoryHeadroom

	if err := newTestConfig(withCredential(), WithHistoryLimit(small), WithQueueSize(floor)).
		validate("wss://gateway.example.com/ws"); err != nil {
		t.Errorf("a queue at the recomputed floor was rejected: %v", err)
	}
	if err := newTestConfig(withCredential(), WithHistoryLimit(small), WithQueueSize(floor-1)).
		validate("wss://gateway.example.com/ws"); err == nil {
		t.Error("a queue below the recomputed floor was accepted")
	}
}

// Every bounded option is asserted on both sides, so a validator that rejected
// or accepted everything would fail here.
func TestBoundsValidation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		opt     Option
		wantErr bool
	}{
		{"history limit at 1", WithHistoryLimit(1), false},
		{"history limit at the ceiling", WithHistoryLimit(MaxHistoryLimit), false},
		{"history limit of zero", WithHistoryLimit(0), true},
		// The ceiling is the server's own bound, not the SDK's default — a
		// deployment that raised its limit must remain usable.
		{"history limit above the ceiling", WithHistoryLimit(MaxHistoryLimit + 1), true},

		{"publish size positive", WithMaxPublishSize(1), false},
		{"publish size of zero", WithMaxPublishSize(0), true},
		{"publish size negative", WithMaxPublishSize(-1), true},

		{"backpressure limit of one", WithMaxBackpressureReconnects(1), false},
		{"backpressure limit of zero", WithMaxBackpressureReconnects(0), true},

		{"positive dial timeout", WithDialTimeout(time.Second), false},
		{"zero dial timeout", WithDialTimeout(0), true},
		{"negative dial timeout", WithDialTimeout(-time.Second), true},

		{"known transport", WithTransport(TransportSSE), false},
		{"unknown transport", WithTransport(TransportKind("carrier-pigeon")), true},

		{"nil http client", WithHTTPClient(nil), true},
		{"nil logger", WithLogger(nil), true},
		{"nil clock", WithClock(nil), true},
		{"nil rand", WithRand(nil), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// A large queue keeps the floor from masking the rule under test.
			err := newTestConfig(withCredential(), WithQueueSize(4096), tc.opt).
				validate("wss://gateway.example.com/ws")
			if tc.wantErr && err == nil {
				t.Error("validate succeeded, want an error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validate errored: %v", err)
			}
		})
	}
}

// The pong timeout must stay under the heartbeat interval, or a timeout could
// not be attributed to a particular heartbeat.
func TestPongTimeoutMustBeUnderHeartbeatInterval(t *testing.T) {
	t.Parallel()

	base := []Option{withCredential(), WithQueueSize(4096)}

	if err := newTestConfig(append(base, WithHeartbeatInterval(30*time.Second), WithPongTimeout(10*time.Second))...).
		validate("wss://gateway.example.com/ws"); err != nil {
		t.Errorf("a pong timeout under the interval was rejected: %v", err)
	}
	for _, pong := range []time.Duration{30 * time.Second, 31 * time.Second} {
		if err := newTestConfig(append(base, WithHeartbeatInterval(30*time.Second), WithPongTimeout(pong))...).
			validate("wss://gateway.example.com/ws"); err == nil {
			t.Errorf("a pong timeout of %v was accepted against a 30s interval", pong)
		}
	}
}

func TestBackoffValidation(t *testing.T) {
	t.Parallel()

	valid := BackoffConfig{Initial: 500 * time.Millisecond, Max: 30 * time.Second, Multiplier: 2, Jitter: 0.2}

	for _, tc := range []struct {
		name    string
		mutate  func(BackoffConfig) BackoffConfig
		wantErr bool
	}{
		{"the shipped default", func(b BackoffConfig) BackoffConfig { return b }, false},
		{"no jitter", func(b BackoffConfig) BackoffConfig { b.Jitter = 0; return b }, false},
		{"full jitter", func(b BackoffConfig) BackoffConfig { b.Jitter = 1; return b }, false},
		{"initial equal to max", func(b BackoffConfig) BackoffConfig { b.Max = b.Initial; return b }, false},

		{"zero initial", func(b BackoffConfig) BackoffConfig { b.Initial = 0; return b }, true},
		{"max below initial", func(b BackoffConfig) BackoffConfig { b.Max = b.Initial - 1; return b }, true},
		// At or below 1 the delay never grows, so a persistent failure retries
		// forever at a fixed rate.
		{"multiplier of one", func(b BackoffConfig) BackoffConfig { b.Multiplier = 1; return b }, true},
		{"multiplier below one", func(b BackoffConfig) BackoffConfig { b.Multiplier = 0.5; return b }, true},
		{"negative jitter", func(b BackoffConfig) BackoffConfig { b.Jitter = -0.1; return b }, true},
		{"jitter above one", func(b BackoffConfig) BackoffConfig { b.Jitter = 1.1; return b }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := newTestConfig(withCredential(), WithQueueSize(4096), WithBackoff(tc.mutate(valid))).
				validate("wss://gateway.example.com/ws")
			if tc.wantErr && err == nil {
				t.Error("validate succeeded, want an error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validate errored: %v", err)
			}
		})
	}
}

// Options are applied in order, so a later one of the same kind wins. Callers
// build option slices programmatically and rely on being able to override.
func TestLaterOptionWins(t *testing.T) {
	t.Parallel()

	c := newTestConfig(WithQueueSize(500), WithQueueSize(600))
	if c.queueSize != 600 {
		t.Errorf("queueSize = %d, want the later value 600", c.queueSize)
	}
}

func TestNormalizeURL(t *testing.T) {
	t.Parallel()

	for give, want := range map[string]string{
		"https://gateway.example.com/ws": "wss://gateway.example.com/ws",
		"http://gateway.example.com/ws":  "ws://gateway.example.com/ws",
		"wss://gateway.example.com/ws":   "wss://gateway.example.com/ws",
		"ws://gateway.example.com/ws":    "ws://gateway.example.com/ws",
	} {
		if got := normalizeURL(give); got != want {
			t.Errorf("normalizeURL(%q) = %q, want %q", give, got, want)
		}
	}
}

// The default logger must discard: the SDK stays silent until a caller asks for
// output. This asserts the handler directly; the full lifecycle version belongs
// with the client tests.
func TestDefaultLoggerDiscards(t *testing.T) {
	t.Parallel()

	if defaultConfig().logger.Enabled(context.Background(), slog.LevelError) {
		t.Error("the default logger is enabled; the SDK must be quiet until given a logger")
	}
}
