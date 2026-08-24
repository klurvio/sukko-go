package sukko

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"sync"
)

// Client is a connection to the Sukko platform. It is single-use: after Close
// (or a terminal failure) every method returns ErrClosed, including Connect —
// build a new client to reconnect from scratch.
//
// The public API lives here; the supervisor goroutine that owns the connection
// lifecycle is in supervisor.go, as methods on this same struct.
type Client struct {
	cfg   *config
	clock Clock

	transport Transport
	delivery  *delivery
	counters  *counters
	// creds holds the live credential the dial presents. UpdateToken/Escalate
	// mutate it; the transport reads it per dial so rotation is picked up.
	creds *credentialStore
	// redactor masks credentials in returned errors (a dial *url.Error embeds the
	// query-param token) and in log records.
	redactor *redactor

	// rootCtx is the client-lifetime context: canceling it tears the client
	// down exactly as Close does. rootCancel is that cancel.
	rootCtx    context.Context
	rootCancel context.CancelFunc

	// firstDial carries the first dial's outcome from the supervisor to Connect.
	// Buffered so the supervisor never blocks on it even if Connect has already
	// returned on its own context deadline.
	firstDial chan error
	// doneCh is closed by terminalSequence when teardown is complete.
	doneCh chan struct{}
	// terminalOnce guards the whole terminal sequence: it runs once whether the
	// supervisor's exit or a never-connected Close reaches it.
	terminalOnce sync.Once

	// mu guards the fields below.
	mu    sync.Mutex
	state ConnectionState
	// err is the Err() value: nil on every clean stop (Close, lifetime cancel,
	// or the WithReconnect(false) downgrade), the failure otherwise.
	err error
	// terminalCause is the *Terminal.Err value. It diverges from err on the
	// WithReconnect(false) downgrade, where the contract says *Terminal carries
	// the close cause for diagnosis while Err() stays nil.
	terminalCause error
	conn          Conn
	started       bool // the supervisor was launched (Connect called)
	closed        bool // Close was called
}

// NewClient builds a client for a URL. It validates the configuration and fails
// fast; it does not dial — call Connect for that. The context is the client
// lifetime: canceling it tears the client down like Close.
func NewClient(ctx context.Context, url string, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if err := cfg.validate(url); err != nil {
		return nil, err
	}

	rootCtx, rootCancel := context.WithCancel(ctx)
	counters := &counters{}
	creds := newCredentialStore(cfg.token, cfg.apiKey)

	// Redact credentials at both §IX boundaries: returned errors and log records.
	// A network dial failure returns Go's *url.Error, which embeds the full dial
	// URL — including a query-param token — so a client using WithQueryParamAuth
	// would otherwise leak its token through any transport error. Value masking
	// covers the credential the client holds now; the redactor's pattern rules
	// cover a rotated token (in a token=/Authorization/X-API-Key position) too.
	redactor := newRedactor(cfg.token, cfg.apiKey)
	cfg.logger = slog.New(redactor.wrapHandler(cfg.logger.Handler()))

	// Under WithNoAuth the dial carries no credential; otherwise it reads the
	// live store per connect.
	var credentials func() (string, string)
	if !cfg.noAuth {
		credentials = creds.snapshot
	}

	c := &Client{
		cfg:        cfg,
		clock:      cfg.clock,
		transport:  newWSTransport(url, cfg, credentials),
		delivery:   newDelivery(cfg.queueSize, cfg.clock, counters),
		counters:   counters,
		creds:      creds,
		redactor:   redactor,
		rootCtx:    rootCtx,
		rootCancel: rootCancel,
		firstDial:  make(chan error, 1),
		doneCh:     make(chan struct{}),
		state:      StateDisconnected,
	}

	// Canceling the client-lifetime context tears the client down like Close.
	// When a supervisor is running it already watches rootCtx (via the epoch
	// context); when Close ran it already tore down. This handles the remaining
	// case — the lifetime canceled before Connect — so Messages() still closes
	// and a ranging caller is not stranded.
	context.AfterFunc(rootCtx, c.onLifetimeCancel)

	return c, nil
}

// onLifetimeCancel tears down a client whose lifetime context was canceled
// before a supervisor ever ran. It defers to a running supervisor or a
// concurrent Close: the started/closed check under mu is the same handshake
// Connect and Close use, so exactly one path performs the teardown.
func (c *Client) onLifetimeCancel() {
	c.mu.Lock()
	if c.started || c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()

	c.transition(triggerCloseCalled)
	c.terminalSequence()
}

// Connect starts the connection. The context bounds the first dial and handshake
// only — it is not retained, so a Connect(ctx) whose deadline passes after a
// successful handshake does not tear down the live connection. Connect returns
// the first dial's outcome: nil on success, a typed error otherwise.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	switch {
	case c.closed || c.state == StateClosed || c.state == StateError:
		c.mu.Unlock()
		return ErrClosed
	case c.started:
		c.mu.Unlock()
		return ErrAlreadyConnected
	case c.rootCtx.Err() != nil:
		// The client lifetime was already canceled.
		c.mu.Unlock()
		return ErrClosed
	}
	c.started = true
	c.mu.Unlock()

	go c.run(ctx)

	select {
	case err := <-c.firstDial:
		return err
	case <-c.doneCh:
		// The supervisor exited. If Close (or a lifetime cancel) raced the first
		// dial, BOTH firstDial and doneCh are ready and a bare select would pick
		// at random — returning Err() (nil on a clean stop) would report a false
		// success on an already-closed client. The first dial's outcome is always
		// sent before doneCh closes, so prefer it; fall back to Err() only when no
		// outcome was reported (a panic before the firstDial send).
		select {
		case err := <-c.firstDial:
			return err
		default:
			return c.Err()
		}
	case <-ctx.Done():
		return fmt.Errorf("sukko: connect: %w", ctx.Err())
	}
}

// Close tears the client down and waits, bounded by ctx, for the teardown to
// finish. It is idempotent: a second Close returns nil. Canceling the
// client-lifetime context has the same effect.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.waitOrCtx(ctx) // still honor the caller's wait on an in-flight close
		return nil
	}
	c.closed = true
	started := c.started
	c.mu.Unlock()

	c.rootCancel()

	// If no supervisor ever ran, nobody else will perform the terminal sequence,
	// so Close does it. started cannot flip to true after closed is set, so
	// there is no orphaned supervisor.
	if !started {
		c.transition(triggerCloseCalled)
		c.terminalSequence()
	}

	select {
	case <-c.doneCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("sukko: close: %w", ctx.Err())
	}
}

// waitOrCtx returns a channel that closes when teardown finishes or ctx ends —
// used by the idempotent second-Close path.
func (c *Client) waitOrCtx(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		select {
		case <-c.doneCh:
		case <-ctx.Done():
		}
		close(done)
	}()
	return done
}

// State returns the current connection state.
func (c *Client) State() ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Err returns the terminal cause after Messages() has closed: nil after a clean
// stop — Close, lifetime cancellation, OR the WithReconnect(false) downgrade of a
// reconnect-class outcome (where *Terminal.Err still carries the cause for
// diagnosis) — and the failure otherwise. The channel close is the happens-before
// edge that makes it race-free to read after close.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Messages returns the in-band event stream. It is closed once, by the
// supervisor, after the final *Terminal.
func (c *Client) Messages() <-chan Event { return c.delivery.messages() }

// Iter yields events until the caller's context ends or Messages() closes. It
// receives and yields in the same step, so an abandoned range strands no event.
func (c *Client) Iter(ctx context.Context) iter.Seq[Event] {
	return func(yield func(Event) bool) {
		msgs := c.delivery.messages()
		for {
			select {
			case ev, ok := <-msgs:
				if !ok || !yield(ev) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

// UpdateToken stores a JWT for the next connect and the next auth refresh. It is
// store-only — it never sends an auth frame, in any state (that is RefreshToken
// and Escalate) — and storing a JWT flips an API-key-only client's credential
// class to publish-capable. An empty token is rejected.
func (c *Client) UpdateToken(token string) error {
	if token == "" {
		return ErrEmptyToken
	}
	c.creds.setToken(token)
	return nil
}

// Stats returns an eventually-consistent snapshot of the client's counters.
func (c *Client) Stats() Stats { return c.counters.snapshot() }

// Capabilities reports what the client's transport can do.
func (c *Client) Capabilities() Capabilities { return c.transport.Capabilities() }
