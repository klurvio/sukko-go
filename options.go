package sukko

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Configuration is functional options over a validated internal config.
//
// Options are used rather than an exported Config struct because a struct
// cannot distinguish "unset" from "the zero value", which breaks defaults for
// every numeric knob: a caller who leaves QueueSize alone and a caller who sets
// it to 0 are indistinguishable, and one of them wants 512 while the other has
// made a mistake worth reporting.
//
// Validation happens once, in NewClient, and fails the construction. A client
// that starts with a bad configuration and discovers it at the first reconnect
// has moved a startup error into production.

// Token is a credential and, optionally, when it expires.
//
// A zero Expiry means "unknown": the SDK falls back to the expiry the server
// reports at auth time, which is the default behaviour and costs a caller
// nothing. A non-zero Expiry is caller-authoritative and lets the refresh
// scheduler arm without waiting for a round-trip — which matters most when the
// server reports no expiry at all, where there would otherwise be nothing to
// schedule against. When both are known, the earlier instant wins.
type Token struct {
	// Value is the credential itself.
	Value string
	// Expiry is when it stops being valid, or the zero time if unknown.
	Expiry time.Time
}

// TokenSource supplies fresh credentials. The SDK calls it from its own timers,
// so it receives a context derived from the client's lifetime and bounded by
// WithTokenSourceTimeout — never a caller's request context, because there is
// no caller present when a refresh falls due.
type TokenSource func(ctx context.Context) (Token, error)

// Option configures a Client. Options are applied in order, so a later option
// of the same kind wins.
type Option func(*config)

// config is the validated internal configuration. It is unexported: every field
// has an option, and exposing the struct would let a caller bypass validation.
type config struct {
	// credentials
	token              string
	apiKey             string
	tokenSource        TokenSource
	noAuth             bool
	queryParamAuth     bool
	tokenSourceTimeout time.Duration

	// transport
	insecureTransport bool
	transport         TransportKind
	httpClient        *http.Client
	dialTimeout       time.Duration

	// delivery
	queueSize      int
	maxPublishSize int

	// recovery
	reconnect                 bool
	backoff                   BackoffConfig
	maxBackpressureReconnects int
	historyLimit              int
	replayFloor               time.Duration
	recoveryDeadline          time.Duration
	clientID                  string

	// liveness
	heartbeatInterval time.Duration
	pongTimeout       time.Duration
	sseIdleTimeout    time.Duration

	// auth timing
	refreshMinInterval time.Duration
	refreshLead        time.Duration

	// observability and seams
	logger *slog.Logger
	clock  Clock
	rand   Rand
}

// defaultConfig returns the configuration a client gets with no options. Every
// value here is an exported constant, so the defaults a caller reads in the
// documentation are the ones the code uses.
func defaultConfig() *config {
	return &config{
		tokenSourceTimeout:        DefaultTokenSourceTimeout,
		transport:                 TransportWebSocket,
		httpClient:                &http.Client{}, // no Timeout: it would cap a long-lived stream
		dialTimeout:               DefaultDialTimeout,
		queueSize:                 DefaultQueueSize,
		maxPublishSize:            DefaultMaxPublishSize,
		reconnect:                 true,
		backoff:                   DefaultBackoff,
		maxBackpressureReconnects: DefaultMaxBackpressureReconnects,
		historyLimit:              DefaultHistoryLimit,
		replayFloor:               DefaultReplayFloor,
		recoveryDeadline:          DefaultRecoveryDeadline,
		heartbeatInterval:         DefaultHeartbeatInterval,
		pongTimeout:               DefaultPongTimeout,
		sseIdleTimeout:            DefaultSSEIdleTimeout,
		refreshMinInterval:        DefaultRefreshMinInterval,
		refreshLead:               DefaultRefreshLead,
		// A discard handler, not a nil logger: the SDK is quiet until a caller
		// asks for output, and nil-checking at every call site would be noise.
		logger: slog.New(discardHandler{}),
		clock:  realClock{},
		rand:   realRand{},
	}
}

// ─── credentials ───

// WithToken sets a JWT credential. It composes with WithAPIKey: a client may
// hold both and escalate from one to the other.
func WithToken(token string) Option { return func(c *config) { c.token = token } }

// WithAPIKey sets an API-key credential. An API key alone is receive-only —
// publishing requires a JWT, which the SDK checks locally rather than letting
// the server reject the write.
func WithAPIKey(key string) Option { return func(c *config) { c.apiKey = key } }

// WithTokenSource supplies fresh credentials on demand, for refresh and for
// reconnects. Without one, a client's credential is whatever it was given at
// construction and cannot be renewed.
func WithTokenSource(src TokenSource) Option { return func(c *config) { c.tokenSource = src } }

// WithNoAuth declares that the target deployment has authentication disabled.
//
// It exists because "does this server require a credential?" is a handshake-time
// property the constructor cannot discover, and silently allowing a
// credential-less client would turn a configuration mistake into a runtime
// failure. Declaring it explicitly keeps the constructor's fail-fast rule intact
// without blocking a legitimate deployment.
func WithNoAuth() Option { return func(c *config) { c.noAuth = true } }

// WithQueryParamAuth sends credentials as URL query parameters instead of
// headers. It is enable-only, and rejected outright over a non-TLS URL: a token
// in a URL is logged by every intermediary, and redaction can remove a
// credential from this SDK's own output but not from the wire.
func WithQueryParamAuth() Option { return func(c *config) { c.queryParamAuth = true } }

// WithTokenSourceTimeout bounds a single TokenSource call. The callback is
// caller code on a path the SDK depends on, so it is bounded rather than
// trusted to return.
func WithTokenSourceTimeout(d time.Duration) Option {
	return func(c *config) { c.tokenSourceTimeout = d }
}

// ─── transport ───

// WithInsecureTransport permits a ws:// or http:// URL.
//
// The check it relaxes is scheme-based only. Hostname, "localhost", private IP
// ranges and environment names are deliberately not inputs: each is a heuristic
// that is wrong somewhere, and a security decision that is right by accident is
// not a security decision.
func WithInsecureTransport() Option { return func(c *config) { c.insecureTransport = true } }

// WithTransport selects the transport. The choice is explicit — the SDK never
// infers it from the URL scheme.
func WithTransport(kind TransportKind) Option { return func(c *config) { c.transport = kind } }

// WithHTTPClient supplies the HTTP client used for every network path: the
// WebSocket dial, REST publish, SSE, and push.
//
// It is one seam rather than several because the WebSocket library dials
// through an *http.Client too. A WS-only TLS option would leave a caller with a
// private CA configured for REST and not for the socket — working in tests,
// failing on the transport that matters.
func WithHTTPClient(client *http.Client) Option { return func(c *config) { c.httpClient = client } }

// WithDialTimeout bounds a reconnect dial. It does not bound Connect, which is
// bounded by the caller's own context.
func WithDialTimeout(d time.Duration) Option { return func(c *config) { c.dialTimeout = d } }

// ─── delivery ───

// WithQueueSize sets the delivery channel's capacity in events. It must leave
// room for a full history and replay window plus the advisory reserve, or a
// recovery burst could fill the channel and starve the notices that explain it.
func WithQueueSize(n int) Option { return func(c *config) { c.queueSize = n } }

// WithMaxPublishSize bounds a published payload in bytes, and derives the
// transport read limit so a legal inbound message is never rejected locally. It
// must match the server's own limit.
func WithMaxPublishSize(n int) Option { return func(c *config) { c.maxPublishSize = n } }

// ─── recovery ───

// WithReconnect enables or disables automatic reconnection. Disabling it turns
// every reconnect-class outcome into a clean stop: the client emits its final
// event and closes the delivery channel, rather than resting in a state a
// caller cannot observe.
func WithReconnect(enabled bool) Option { return func(c *config) { c.reconnect = enabled } }

// WithBackoff sets the reconnect backoff parameters.
func WithBackoff(b BackoffConfig) Option { return func(c *config) { c.backoff = b } }

// WithMaxBackpressureReconnects sets how many consecutive back-pressure-induced
// reconnects are tolerated before the client terminates with ErrConsumerTooSlow.
// The run resets on any healthy epoch, so a consumer that recovers between
// bursts is not terminated.
func WithMaxBackpressureReconnects(n int) Option {
	return func(c *config) { c.maxBackpressureReconnects = n }
}

// WithHistoryLimit sets how many records a History call requests.
func WithHistoryLimit(n int) Option { return func(c *config) { c.historyLimit = n } }

// WithReplayFloor sets the per-channel minimum interval between replay
// requests, matching the server's own rate limit so the SDK does not provoke it.
func WithReplayFloor(d time.Duration) Option { return func(c *config) { c.replayFloor = d } }

// WithRecoveryDeadline sets how long the client waits for a recovery terminator
// before reporting the recovery as interrupted.
func WithRecoveryDeadline(d time.Duration) Option { return func(c *config) { c.recoveryDeadline = d } }

// WithClientID sets the identity used to resume a session across reconnects.
//
// Without one the SDK generates a per-process identifier, which is correct for
// a long-lived service but forfeits resumption across a restart — a caller that
// wants that must persist an ID and supply it.
func WithClientID(id string) Option { return func(c *config) { c.clientID = id } }

// ─── liveness ───

// WithHeartbeatInterval sets how often the client sends an application-level
// heartbeat.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *config) { c.heartbeatInterval = d }
}

// WithPongTimeout sets how long the client waits for a heartbeat reply before
// closing the connection locally.
func WithPongTimeout(d time.Duration) Option { return func(c *config) { c.pongTimeout = d } }

// WithSSEIdleTimeout sets how long an SSE stream may deliver no bytes at all —
// neither a record nor a keepalive comment — before the client treats it as dead.
func WithSSEIdleTimeout(d time.Duration) Option { return func(c *config) { c.sseIdleTimeout = d } }

// ─── auth timing ───

// WithRefreshMinInterval sets the floor between credential refreshes, which is
// what prevents an auth error from driving a refresh loop.
func WithRefreshMinInterval(d time.Duration) Option {
	return func(c *config) { c.refreshMinInterval = d }
}

// WithRefreshLead sets how far ahead of expiry a proactive refresh is armed.
func WithRefreshLead(d time.Duration) Option { return func(c *config) { c.refreshLead = d } }

// ─── observability and seams ───

// WithLogger supplies the logger the SDK writes to. Without one it writes
// nowhere: a library that logs into its caller's output uninvited is a library
// the caller cannot quiet.
func WithLogger(logger *slog.Logger) Option { return func(c *config) { c.logger = logger } }

// WithClock replaces the time source. It exists so tests advance time instead of
// sleeping; production code has no reason to set it.
func WithClock(clock Clock) Option { return func(c *config) { c.clock = clock } }

// WithRand replaces the randomness source used for backoff jitter, so a seeded
// implementation yields an assertable delay sequence.
func WithRand(rand Rand) Option { return func(c *config) { c.rand = rand } }

// discardHandler drops every record. It is the default so the SDK is silent
// until a caller supplies a logger, without nil-checking at each call site.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }

// validate checks the configuration and reports the first problem, so
// construction fails rather than deferring the error to a connection attempt.
//
// Rules that span options live here rather than in the setters: an option
// applied before its counterpart cannot see it, so cross-field rules are only
// decidable once every option has run.
func (c *config) validate(rawURL string) error {
	if err := c.validateURL(rawURL); err != nil {
		return err
	}
	if err := c.validateCredentials(); err != nil {
		return err
	}
	return c.validateBounds()
}

// validateURL enforces the transport-security rules. The scheme is the only
// input — see WithInsecureTransport.
func (c *config) validateURL(rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("sukko: url is empty")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("sukko: parsing url: %w", err)
	}

	secure := parsed.Scheme == "wss" || parsed.Scheme == "https"
	switch parsed.Scheme {
	case "wss", "https", "ws", "http":
	default:
		return fmt.Errorf("sukko: unsupported url scheme %q (want wss, https, ws or http)", parsed.Scheme)
	}
	if parsed.Host == "" {
		return fmt.Errorf("sukko: url %q has no host", rawURL)
	}

	// Query-param auth over plaintext is refused outright, and the insecure
	// opt-in does not unlock it: the opt-in covers "I accept an unencrypted
	// channel", not "I accept publishing my credential to every intermediary".
	if c.queryParamAuth && !secure {
		return fmt.Errorf(
			"%w: query-parameter auth over %s would put the credential on the wire in clear text; "+
				"WithInsecureTransport does not permit this combination",
			ErrInsecureTransport, parsed.Scheme)
	}
	if !secure && !c.insecureTransport {
		return fmt.Errorf("%w: url %q is not TLS; pass WithInsecureTransport to allow it",
			ErrInsecureTransport, rawURL)
	}
	return nil
}

// validateCredentials enforces the "declare your intent" rule: exactly one of
// "here is a credential" or "this deployment has auth disabled".
func (c *config) validateCredentials() error {
	hasCredential := c.token != "" || c.apiKey != "" || c.tokenSource != nil

	if c.noAuth && hasCredential {
		return fmt.Errorf("sukko: WithNoAuth cannot be combined with a credential; " +
			"a client that both has a credential and declares auth disabled has two meanings")
	}
	if c.noAuth && c.queryParamAuth {
		return fmt.Errorf("sukko: WithNoAuth cannot be combined with WithQueryParamAuth; " +
			"there is no credential to place in the query string")
	}
	if !c.noAuth && !hasCredential {
		return fmt.Errorf("sukko: no credential configured; " +
			"pass WithToken, WithAPIKey or WithTokenSource, or WithNoAuth if the deployment has auth disabled")
	}
	return nil
}

// validateBounds enforces the per-option ranges and the relationships between
// them.
func (c *config) validateBounds() error {
	floor := c.historyLimit + MaxReplayMessages + AdvisoryHeadroom
	switch {
	case c.queueSize < floor:
		// The floor is a sum, not a maximum: a caller's history request and an
		// automatic replay can be in flight at once, and the advisory reserve
		// has to survive both.
		return fmt.Errorf(
			"sukko: QueueSize %d is below the floor of %d (HistoryLimit %d + MaxReplayMessages %d + AdvisoryHeadroom %d); "+
				"a recovery burst could fill the channel and starve the notices that explain it",
			c.queueSize, floor, c.historyLimit, MaxReplayMessages, AdvisoryHeadroom)

	case c.historyLimit < 1 || c.historyLimit > MaxHistoryLimit:
		return fmt.Errorf("sukko: HistoryLimit %d out of range [1, %d]", c.historyLimit, MaxHistoryLimit)

	case c.maxPublishSize <= 0:
		return fmt.Errorf("sukko: MaxPublishSize must be positive, got %d", c.maxPublishSize)

	case c.maxBackpressureReconnects < 1:
		return fmt.Errorf("sukko: MaxBackpressureReconnects must be at least 1, got %d",
			c.maxBackpressureReconnects)

	case c.pongTimeout >= c.heartbeatInterval:
		// Otherwise a timeout could not be attributed to one heartbeat.
		return fmt.Errorf("sukko: PongTimeout (%v) must be less than HeartbeatInterval (%v)",
			c.pongTimeout, c.heartbeatInterval)

	case c.transport != TransportWebSocket && c.transport != TransportSSE:
		return fmt.Errorf("sukko: unknown transport %q", c.transport)

	case c.httpClient == nil:
		return fmt.Errorf("sukko: WithHTTPClient requires a non-nil client")

	case c.logger == nil:
		return fmt.Errorf("sukko: WithLogger requires a non-nil logger")

	case c.clock == nil:
		return fmt.Errorf("sukko: WithClock requires a non-nil clock")

	case c.rand == nil:
		return fmt.Errorf("sukko: WithRand requires a non-nil source")
	}

	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"DialTimeout", c.dialTimeout},
		{"HeartbeatInterval", c.heartbeatInterval},
		{"PongTimeout", c.pongTimeout},
		{"RefreshMinInterval", c.refreshMinInterval},
		{"RefreshLead", c.refreshLead},
		{"ReplayFloor", c.replayFloor},
		{"RecoveryDeadline", c.recoveryDeadline},
		{"SSEIdleTimeout", c.sseIdleTimeout},
		{"TokenSourceTimeout", c.tokenSourceTimeout},
	} {
		if d.value <= 0 {
			return fmt.Errorf("sukko: %s must be positive, got %v", d.name, d.value)
		}
	}

	return c.validateBackoff()
}

// validateBackoff checks the backoff parameters, whose relationships decide
// whether a delay sequence converges.
func (c *config) validateBackoff() error {
	b := c.backoff
	switch {
	case b.Initial <= 0:
		return fmt.Errorf("sukko: Backoff.Initial must be positive, got %v", b.Initial)
	case b.Max < b.Initial:
		return fmt.Errorf("sukko: Backoff.Max (%v) must be at least Backoff.Initial (%v)", b.Max, b.Initial)
	case b.Multiplier <= 1:
		// At or below 1 the delay never grows, so a persistent failure retries
		// at a fixed rate forever.
		return fmt.Errorf("sukko: Backoff.Multiplier must exceed 1, got %v", b.Multiplier)
	case b.Jitter < 0 || b.Jitter > 1:
		return fmt.Errorf("sukko: Backoff.Jitter must be within [0, 1], got %v", b.Jitter)
	}
	return nil
}

// normalizeURL returns the URL with its scheme mapped to the WebSocket
// equivalent, so a caller may pass either form.
func normalizeURL(rawURL string) string {
	switch {
	case strings.HasPrefix(rawURL, "https://"):
		return "wss://" + strings.TrimPrefix(rawURL, "https://")
	case strings.HasPrefix(rawURL, "http://"):
		return "ws://" + strings.TrimPrefix(rawURL, "http://")
	default:
		return rawURL
	}
}
