package sukko

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"slices"
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
	// authInbox carries auth_ack/auth_error answers from the decode loop to the
	// auth-owner (coalesced; drained on each poke).
	authInbox *authInbox

	// rootCtx is the client-lifetime context: canceling it tears the client
	// down exactly as Close does. rootCancel is that cancel.
	rootCtx    context.Context
	rootCancel context.CancelFunc

	// firstDial carries the first dial's outcome from the supervisor to Connect.
	// Buffered so the supervisor never blocks on it even if Connect has already
	// returned on its own context deadline.
	firstDial chan error
	// authRefreshCmd carries a caller RefreshToken request to the auth-owner.
	// Buffered(1) and sent non-blocking, so concurrent refresh requests coalesce
	// (the owner also single-flights) and RefreshToken never blocks.
	authRefreshCmd chan struct{}
	// authPoke tells the auth-owner an auth_ack/auth_error arrived, completing the
	// in-flight refresh. Buffered(1), sent non-blocking by the decode loop.
	authPoke chan struct{}
	// authEpochReset tells the auth-owner an epoch ended before its in-flight
	// refresh was answered, so it must abandon that flight — the answer will never
	// come on the new connection, and the reconnect re-auths via the dial
	// credential. Buffered(1), sent non-blocking by the supervisor per epoch.
	authEpochReset chan struct{}
	// authEpochUp tells the auth-owner a new epoch is connected, so it retries a
	// refresh that was wanted while disconnected (e.g. the proactive timer fired
	// during backoff and could not send). Buffered(1), sent non-blocking by the
	// supervisor after each handshake.
	authEpochUp chan struct{}
	// authDialCred carries a supervisor pre-dial ensure-token request to the
	// auth-owner (B1): a TokenSource client must fetch a fresh credential before
	// every dial, and the owner is the sole TokenSource caller. Unbuffered — a
	// request-reply rendezvous, not a fire-and-forget signal — and the supervisor
	// only ever has one outstanding, so the send never queues.
	authDialCred chan authDialReq
	// authEscalateCmd signals the auth-owner that an Escalate is wanted; the JWT
	// itself rides escalateBox (latest-wins). Buffered(1), sent non-blocking.
	authEscalateCmd chan struct{}
	// escalateBox holds the latest caller-supplied escalation JWT awaiting send.
	escalateBox escalateBox
	// subs holds the subscription sets (granted/desired), read by the caller-facing
	// accessors and written by the subscribe serializer (sole writer).
	subs *subState
	// subReqCh is the bounded subscribe-serializer request queue (SubscribeQueueDepth):
	// Subscribe/Unsubscribe non-blocking-send here and return; full ⇒ ErrSubscribeQueueFull.
	subReqCh chan subReq
	// subPoke tells the serializer to release the flight identified by the sent
	// generation (an ack/error the decode loop already reconciled). Buffered(1); the
	// serializer releases only if the gen still matches the current flight.
	subPoke chan uint64
	// subFlight is the one outstanding subscribe/unsubscribe: the serializer sets it
	// before the send and clears it on release; the decode loop reads it to
	// reconcile an ack in receive order.
	subFlight subFlight
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
	// currentEpoch is the live epoch, so the auth-owner can record a
	// connected-refresh TokenSource-exhaustion terminal into its first-cause slot
	// (the owner cannot call terminalSequence itself). nil between epochs.
	currentEpoch *epoch
	// flightMode is the mode (refresh vs escalation) of the auth frame currently in
	// flight. The auth-owner writes it when it sends; the decode goroutine reads it
	// to label *Authenticated (the wire carries no mode). Single-flight keeps it
	// stable between send and the answering ack.
	flightMode AuthMode
	started    bool // the supervisor was launched (Connect called)
	closed     bool // Close was called
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
		cfg:             cfg,
		clock:           cfg.clock,
		transport:       newWSTransport(url, cfg, credentials),
		delivery:        newDelivery(cfg.queueSize, cfg.clock, counters),
		counters:        counters,
		creds:           creds,
		redactor:        redactor,
		authInbox:       &authInbox{},
		rootCtx:         rootCtx,
		rootCancel:      rootCancel,
		firstDial:       make(chan error, 1),
		authRefreshCmd:  make(chan struct{}, 1),
		authPoke:        make(chan struct{}, 1),
		authEpochReset:  make(chan struct{}, 1),
		authEpochUp:     make(chan struct{}, 1),
		authDialCred:    make(chan authDialReq),
		authEscalateCmd: make(chan struct{}, 1),
		subs:            newSubState(),
		subReqCh:        make(chan subReq, SubscribeQueueDepth),
		subPoke:         make(chan uint64, 1),
		doneCh:          make(chan struct{}),
		flightMode:      AuthRefresh, // the initial/handshake auth is a refresh; escalation sets it explicitly
		state:           StateDisconnected,
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
		// The supervisor exited. If a first-dial outcome was reported before the
		// exit, prefer it (a bare select could pick doneCh and return Err() — nil on
		// a clean stop — reporting a false success on an already-closed client). Fall
		// back to Err() when no outcome was sent: a first-dial success that Close
		// raced (the success path reports only after the →connected transition, which
		// this exit preempted) or a panic before any report — Err() carries the clean
		// stop's nil or the failure's cause.
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

// RefreshToken triggers an immediate credential refresh: the SDK sends an `auth`
// frame with the current JWT on the live connection. It is an imperative "send
// now" method — it REQUIRES a live socket and returns *NotConnectedError in any
// other state (ADR-0011), because queuing a send for the reconnect would be
// redundant: the reconnect's dial re-authenticates via the per-dial credential
// read. The offline path is UpdateToken (store) + reconnect. On a live socket it
// is fire-and-forget (FR-001b) and single-flight — concurrent calls coalesce into
// one in-flight refresh. It returns ErrClosed once the client is closed.
func (c *Client) RefreshToken(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sukko: refresh token: %w", err)
	}
	c.mu.Lock()
	state := c.state
	closed := c.closed
	c.mu.Unlock()
	switch {
	case closed || state == StateClosed || state == StateError:
		return ErrClosed
	case state != StateConnected:
		return &NotConnectedError{Op: "RefreshToken"}
	}
	select {
	case c.authRefreshCmd <- struct{}{}:
	default: // a refresh is already queued; coalesce
	}
	return nil
}

// Escalate widens an API-key connection to full JWT access (or replaces the JWT
// in escalation mode) by sending an `auth` frame carrying the caller-supplied JWT
// on the live socket. Like RefreshToken it is an imperative "send now" method: it
// REQUIRES a live socket and returns *NotConnectedError in any other state — it
// does not silently store the credential (that is UpdateToken). The offline path
// is UpdateToken(jwt) + reconnect. An empty JWT is rejected. On a successful ack
// the JWT is committed to the credential store (flipping the client's class to
// "has JWT"); a rejected escalation leaves the store unchanged. It is
// fire-and-forget (FR-001b) and single-flight (latest JWT wins). An escalation
// accepted on a live socket that then drops before its ack is re-sent on the
// reconnect rather than lost.
func (c *Client) Escalate(ctx context.Context, jwt string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sukko: escalate: %w", err)
	}
	if jwt == "" {
		return ErrEmptyToken
	}
	c.mu.Lock()
	state := c.state
	closed := c.closed
	c.mu.Unlock()
	switch {
	case closed || state == StateClosed || state == StateError:
		return ErrClosed
	case state != StateConnected:
		return &NotConnectedError{Op: "Escalate"}
	}
	// Capture the override generation at call time: a later commit uses it to
	// resolve UpdateToken-vs-Escalate as latest-caller-wins despite the ack's
	// round-trip latency.
	c.escalateBox.put(jwt, c.creds.generation())
	select {
	case c.authEscalateCmd <- struct{}{}:
	default: // a signal is already queued; the box holds the latest JWT
	}
	return nil
}

// setFlightMode records the mode of the auth frame the owner is sending, for the
// decode goroutine to read when it labels *Authenticated.
func (c *Client) setFlightMode(m AuthMode) {
	c.mu.Lock()
	c.flightMode = m
	c.mu.Unlock()
}

// currentFlightMode returns the in-flight auth's mode.
func (c *Client) currentFlightMode() AuthMode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flightMode
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

// Subscribe requests delivery of the given channels. Subscription is STATE, not an
// action: the request is enqueued on the serializer and Subscribe returns as soon
// as it is accepted — it is fire-and-forget (FR-001b), never awaits an ack, and is
// accepted in any non-closed state (a request made while disconnected updates the
// desired set and is flushed on the next connect). A full serializer queue yields
// ErrSubscribeQueueFull; a closed client returns ErrClosed. The grant outcome
// arrives in-band as *SubscriptionResult.
func (c *Client) Subscribe(ctx context.Context, channels []string) error {
	return c.enqueueSubReq(ctx, "subscribe", subReq{kind: reqSubscribe, channels: slices.Clone(channels)})
}

// Unsubscribe abandons the given channels. Like Subscribe it is fire-and-forget
// and accepted in any non-closed state. It prunes the channels from both the
// granted and the pending sets; a wire `unsubscribe` is sent only for channels
// currently granted (a never-granted channel is pruned locally with no wire
// traffic).
func (c *Client) Unsubscribe(ctx context.Context, channels []string) error {
	return c.enqueueSubReq(ctx, "unsubscribe", subReq{kind: reqUnsubscribe, channels: slices.Clone(channels)})
}

// enqueueSubReq is the shared enqueue path for Subscribe/Unsubscribe.
func (c *Client) enqueueSubReq(ctx context.Context, op string, req subReq) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sukko: %s: %w", op, err)
	}
	if len(req.channels) == 0 {
		return nil // nothing to do
	}
	c.mu.Lock()
	closed := c.closed || c.state == StateClosed || c.state == StateError
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	select {
	case c.subReqCh <- req:
		return nil
	default:
		return ErrSubscribeQueueFull
	}
}

// Subscriptions returns the granted set — the channels the client is actually
// receiving — as full tenant-prefixed strings.
func (c *Client) Subscriptions() []string { return c.subs.grantedSnapshot() }

// PendingSubscriptions returns the requested-but-not-yet-granted set (FR-001a):
// channels a Subscribe asked for that are not (yet) granted — covering both a
// Subscribe issued while disconnected and the post-ack denial delta.
func (c *Client) PendingSubscriptions() []string { return c.subs.pendingSnapshot() }

// Stats returns an eventually-consistent snapshot of the client's counters.
func (c *Client) Stats() Stats { return c.counters.snapshot() }

// Capabilities reports what the client's transport can do.
func (c *Client) Capabilities() Capabilities { return c.transport.Capabilities() }
