package sukko

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

// The supervisor is the client-lifetime goroutine that owns the connection: it
// dials, runs the decode loop for one epoch, and — on every exit path — performs
// the terminal sequence (set the terminal cause, close the transport, deliver
// the final *Terminal into its reserved slot, close Messages()). Close does NOT
// perform that sequence; it cancels the root and waits, and the supervisor's own
// exit does the teardown. That inversion is what makes "Messages() is closed by
// exactly one goroutine" literal and keeps a terminal failure (which runs with
// no Close in flight) on the same path as a clean stop.
//
// This slice runs a single epoch — reconnect (the redial loop around runEpoch)
// lands in the next slice.

// closeCodeNormalClosure is the RFC 6455 normal-closure code the SDK sends on a
// clean local close. coder/websocket usually kills the socket first when the
// read context is canceled, so this frame often never reaches the wire; the
// pinned close ordering accepts that.
const closeCodeNormalClosure = 1000

// run is the supervisor goroutine. connectCtx bounds only the first dial.
//
//nolint:contextcheck // by design the epoch and every downstream context derive from the stored client-lifetime context (rootCtx), NOT from connectCtx — connectCtx bounds only the first dial and is deliberately not inherited by the running connection (the "Connect ctx does not tear down a live connection" rule).
func (c *Client) run(connectCtx context.Context) {
	epochCtx, epochCancel := context.WithCancel(c.rootCtx)

	// One combined exit defer (§VII: recover is effectively first, and the
	// terminal sequence runs after the cause is set). On a panic the recover
	// sets the cause BEFORE terminalSequence delivers *Terminal, so the caller
	// never gets *Terminal{Err: nil} for a crash.
	defer func() {
		if r := recover(); r != nil {
			// A recovered panic becomes a typed *InternalError, so the cause on
			// Err() and the final *Terminal is errors.As-matchable and carries the
			// stack. (Full in-band *InternalError emission and epoch-failure land
			// with the recover helper in a later slice; here a panic is terminal.)
			c.setErrIfNil(&InternalError{
				Op:    "supervisor",
				Value: fmt.Sprint(r),
				Stack: string(debug.Stack()),
			})
			c.cfg.logger.Error("sukko: supervisor panic", "value", fmt.Sprint(r))
			c.transition(epochCtx, triggerTerminalFailure)
		}
		c.terminalSequence()
	}()
	// epochCtx is dead before the exit defer runs (LIFO), so any teardown-path
	// send discards instead of parking on a hung consumer (never blocks close).
	defer epochCancel()

	c.transition(epochCtx, triggerConnectCalled) // disconnected → connecting

	conn, err := c.dial(connectCtx)
	if err != nil {
		c.firstDial <- err
		// Discriminate on cause, not outcome: a dial aborted because Close or the
		// lifetime context canceled the root is a clean stop, not a failure —
		// the same rule runEpoch applies to a mid-epoch read error. Only a real
		// dial/handshake failure is terminal.
		if c.rootCtx.Err() != nil {
			c.transition(epochCtx, triggerCloseCalled)
			return
		}
		c.classifyDialFailure(epochCtx, err)
		return
	}
	c.setConn(conn)
	c.firstDial <- nil
	c.transition(epochCtx, triggerHandshakeOK) // connecting → connected

	c.runEpoch(epochCtx, conn)
}

// dial performs the first dial, bounded by the connect deadline AND the client
// lifetime: canceling the NewClient context aborts an in-flight dial.
func (c *Client) dial(connectCtx context.Context) (Conn, error) {
	dialCtx, dialCancel := context.WithCancel(connectCtx)
	defer dialCancel()
	//nolint:contextcheck // the dial is aborted by the client-lifetime context (rootCtx) as well as the connect deadline — that is the point.
	stop := context.AfterFunc(c.rootCtx, dialCancel)
	defer stop()

	conn, err := c.transport.Open(dialCtx)
	if err != nil {
		return nil, fmt.Errorf("sukko: dial: %w", err)
	}
	return conn, nil
}

// runEpoch is the decode loop: the sole reader of the socket. When it parks on a
// delivery send under back-pressure it stops reading, which is what closes the
// TCP window. It returns when the epoch ends, having set the terminal state.
func (c *Client) runEpoch(epochCtx context.Context, conn Conn) {
	for {
		data, err := conn.Read(epochCtx)
		if err == nil {
			c.dispatch(epochCtx, data)
			continue
		}

		// The root being canceled means Close or the lifetime context ended
		// the epoch — a clean stop, no error to report.
		if c.rootCtx.Err() != nil {
			c.transition(epochCtx, triggerCloseCalled)
			return
		}

		var ce *CloseError
		if errors.As(err, &ce) {
			c.classifyClose(epochCtx, ce)
			return
		}
		// An unexpected read error with the root still live.
		c.setErr(fmt.Errorf("sukko: read: %w", err))
		c.transition(epochCtx, triggerTerminalFailure)
		return
	}
}

// dispatch routes one decoded frame. Data and surfaced advisories go to the
// delivery channel in receive order; unknown types surface as *UnknownEvent
// (forward compatibility); control frames whose disposition is not "surface"
// carry their effect in a later phase and surface nothing here.
func (c *Client) dispatch(epochCtx context.Context, data []byte) {
	decoded, unknown, err := decodeFrame(data)
	if err != nil {
		// Unreadable frame. Surfaced as a protocol error so drift is visible;
		// the epoch-tear-on-non-JSON refinement (FR-002 liveness) is a later task.
		c.forward(epochCtx, &ProtocolError{Message: "unreadable frame", Cause: err})
		return
	}
	if unknown != "" {
		c.counters.unknownEvents.Add(1)
		c.forward(epochCtx, &UnknownEvent{Type: unknown, Raw: bytes.Clone(data)})
		return
	}

	ev, serr := surfaceEvent(decoded)
	switch {
	case serr == nil:
		c.forward(epochCtx, ev)
	case errors.Is(serr, errNotSurfaceFrame):
		// A control-derived (subscription_ack, auth_ack) or internal-silent
		// (pong) frame. Its side effects — grant diff, auth mode, pong deadline
		// — belong to later phases; none reach a client that cannot yet
		// subscribe or authenticate.
	default:
		// A presence/protocol failure: surfaceEvent returned a *ProtocolError,
		// which is itself an Event.
		if pe, ok := serr.(Event); ok {
			c.forward(epochCtx, pe)
		}
	}
}

// forward sends an event to the delivery channel, counting the data plane.
func (c *Client) forward(epochCtx context.Context, ev Event) {
	if _, ok := ev.(*Message); ok {
		c.counters.messagesReceived.Add(1)
	}
	c.delivery.send(c.rootCtx, epochCtx, ev)
}

// classifyDialFailure sets the terminal state for a failed first dial.
func (c *Client) classifyDialFailure(epochCtx context.Context, err error) {
	c.setErr(err)

	var he *HandshakeError
	if errors.As(err, &he) {
		out := lookupHandshakePolicy(he.Status, he.Code, c.cfg.reconnect)
		if out.class == classTerminal {
			c.transition(epochCtx, triggerTerminalFailure)
			return
		}
		// classReconnect (and the reconnect-disabled downgrade) redial in the
		// next slice; a single-epoch client ends here for now.
		c.transition(epochCtx, triggerTerminalFailure)
		return
	}
	// A network/TLS dial error or a canceled dial context.
	c.transition(epochCtx, triggerTerminalFailure)
}

// classifyClose sets the terminal state for a closed epoch.
func (c *Client) classifyClose(epochCtx context.Context, ce *CloseError) {
	out, ok := lookupClosePolicy(closeKey{code: ce.Code, direction: ce.Direction, reconnectEnabled: c.cfg.reconnect})
	if !ok || out.class == classTerminal {
		c.setErr(ce)
		c.transition(epochCtx, triggerTerminalFailure)
		return
	}
	if out.class == classCleanStop {
		// Slice-3 obligation: this branch is reachable only via the
		// WithReconnect(false) downgrade, where the contract (events.go: Terminal
		// docs) says *Terminal.Err carries the close cause while Err() stays nil.
		// That needs a SECOND cause field — terminalSequence and Err() share one
		// today — and must land when this classifier is rebuilt for reconnect,
		// before the two collapse permanently.
		c.transition(epochCtx, triggerCloseCalled)
		return
	}
	// classReconnect redials in the next slice.
	c.setErr(ce)
	c.transition(epochCtx, triggerTerminalFailure)
}

// transition advances the state machine via a trigger and emits one
// *StateChange per real transition. The target comes from the stateTransitions
// table, never a hardcoded state, so a terminal failure lands in StateError and
// a clean stop in StateClosed rather than the two collapsing. A trigger with no
// defined transition from the current state is a no-op and emits nothing.
func (c *Client) transition(epochCtx context.Context, tr trigger) {
	c.mu.Lock()
	from := c.state
	to, ok := lookupStateTransition(from, tr)
	if !ok || from == to {
		c.mu.Unlock()
		return
	}
	c.state = to
	c.mu.Unlock()

	c.delivery.send(c.rootCtx, epochCtx, &StateChange{From: from, To: to})
}

// terminalSequence is the client's teardown, performed exactly once by whichever
// path reaches it — the supervisor's exit, or Close when no supervisor ran. It
// runs after every delivery sender has exited (the decode loop is done), so the
// non-blocking *Terminal send into its reserved slot cannot be starved.
func (c *Client) terminalSequence() {
	c.terminalOnce.Do(func() {
		c.mu.Lock()
		conn := c.conn
		cause := c.err
		c.mu.Unlock()

		if conn != nil {
			_ = conn.Close(closeCodeNormalClosure, "") // best-effort; may already be closed
		}
		c.delivery.sendTerminal(&Terminal{Err: cause})
		c.delivery.close()
		close(c.doneCh)
	})
}

// setConn stashes the live connection so terminalSequence can close it.
func (c *Client) setConn(conn Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

func (c *Client) setErr(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
}

func (c *Client) setErrIfNil(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
}
