package sukko

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// The supervisor is the client-lifetime goroutine that owns the connection: it
// dials, runs the decode loop for one epoch, classifies how the epoch ended,
// backs off, and re-dials — and, on every exit path, performs the terminal
// sequence (set the terminal cause, close the transport, deliver the final
// *Terminal into its reserved slot, close Messages()). Close does NOT perform
// that sequence; it cancels the root and waits, and the supervisor's own exit
// does the teardown. That inversion is what makes "Messages() is closed by
// exactly one goroutine" literal and keeps a terminal failure (which runs with
// no Close in flight) on the same path as a clean stop.
//
// Termination classification is driven entirely from the policy tables in
// policy.go — closePolicy, handshakePolicy, and (in a later slice)
// internalCausePolicy — so a class the tables enumerate is the class the loop
// enacts, with no second copy of the rules here.

// closeCodeNormalClosure is the RFC 6455 normal-closure code the SDK sends on a
// clean local close. coder/websocket usually kills the socket first when the
// read context is canceled, so this frame often never reaches the wire; the
// pinned close ordering accepts that.
const closeCodeNormalClosure = 1000

// terminationOutcome is the classified result of an epoch ending or a dial
// failing. It is what the classifiers return and what applyOutcome enacts, so
// the "what class is this" decision (the policy tables) and the "what do we do
// about it" decision (the loop) are separated.
type terminationOutcome struct {
	class terminationClass
	// cause is the failure carried on Err()/*Terminal.Err for a terminal stop,
	// on *Terminal.Err alone for the WithReconnect(false) downgrade, and unused
	// for a reconnect-class outcome.
	cause error
	// surface is the in-band event emitted before terminating or reconnecting —
	// the *CloseError on a coded close (when the policy's surfacesCloseError is
	// set) or the *HandshakeError on a failed upgrade. nil when the ending has no
	// server-originated outcome to report (a network dial error).
	surface Event
	// trigger drives the state machine.
	trigger trigger
	// retryAfter, when non-nil, overrides the NEXT reconnect attempt's backoff
	// delay — the handshake-429 tenant-limit case where the server's own hint
	// beats the client's exponential guess. A present zero means "dial
	// immediately".
	retryAfter *time.Duration
}

// run is the supervisor goroutine. connectCtx bounds only the first dial.
//
//nolint:contextcheck // the auth-owner and every epoch derive from the stored client-lifetime context (rootCtx), NOT connectCtx — connectCtx bounds only the first dial and is deliberately not inherited by the supervisor-lifetime goroutines.
func (c *Client) run(connectCtx context.Context) {
	// The auth-owner is a supervisor-lifetime goroutine, launched once here and
	// living across epochs. It MUST be torn down before terminalSequence closes
	// the transport and Messages(): a SUPERVISOR-side terminal failure never
	// cancels rootCtx, so an owner left running would write a dying socket and
	// send *TokenSourceError on a closed channel and panic. Stopping it here (the
	// exit defer's ownerCancel → ownerWg.Wait) prevents that.
	//
	// Single-terminator rule, refined: the owner still never calls terminalSequence
	// — the exit defer below is the sole terminator. But an owner PANIC is the one
	// path where the owner cancels rootCtx (recoverAuthOwner records the cause
	// first, then cancels), which merely TRIGGERS this same exit defer exactly as
	// Close does; stopTrigger then discriminates the recorded cause into StateError.
	ownerCtx, ownerCancel := context.WithCancel(c.rootCtx)
	var ownerWg sync.WaitGroup
	ownerWg.Go(func() { c.runAuthOwner(ownerCtx) })
	// The subscribe serializer is the second supervisor-lifetime owner goroutine,
	// launched and torn down on the same ownerCtx/ownerWg as the auth-owner.
	ownerWg.Go(func() { c.runSubscribeSerializer(ownerCtx) })
	// The recovery owner is the third such owner: it drives the gap→replay FSM and is
	// the sole `replay` sender. Torn down on the same ownerCtx/ownerWg.
	ownerWg.Go(func() { c.runRecoveryOwner(ownerCtx) })

	// One combined exit defer (§VII: recover is effectively first, and the
	// terminal sequence runs after the cause is set). On a panic the recover
	// sets the cause BEFORE terminalSequence delivers *Terminal, so the caller
	// never gets *Terminal{Err: nil} for a crash. (The full in-band
	// *InternalError emission + epoch-failure reconnect land with the shared
	// recover helper in a later slice; here a panic is terminal.)
	defer func() {
		if r := recover(); r != nil {
			ie := &InternalError{
				Op:    "supervisor",
				Value: fmt.Sprint(r),
				Stack: string(debug.Stack()),
			}
			c.setErrIfNil(ie)
			c.setTerminalCauseIfNil(ie)
			c.cfg.logger.Error("sukko: supervisor panic", "value", fmt.Sprint(r))
			c.transition(triggerTerminalFailure)
		}
		// Cancel and wait for the auth-owner before closing Messages() — cancel
		// then Wait, never the reverse (its select escapes on ownerCtx.Done()).
		ownerCancel()
		ownerWg.Wait()
		c.terminalSequence()
	}()

	c.transition(triggerConnectCalled) // disconnected → connecting

	// attempt is the backoff exponent for the NEXT reconnect dial: 0 for the
	// first reconnect after an epoch ends or a first dial fails, incremented per
	// consecutive failed reconnect, and reset only on a successful re-handshake
	// (never on a failed dial), so a flapping connection keeps backing off.
	attempt := 0
	firstDialReported := false
	// hadEpoch is true once a prior epoch actually ran — the gate for sending
	// reconnect{last_pos}. It is NOT the same as firstDialReported: a failed first
	// dial reports Connect but establishes no epoch, so the first SUCCESSFUL connect
	// still has nothing to resume and must send no reconnect frame.
	hadEpoch := false
	var override *time.Duration

	for {
		if firstDialReported {
			// Reconnect: wait out the backoff (or the server's Retry-After
			// override), then dial. Root cancellation during the wait is a clean
			// stop.
			// A pending Retry-After override replaces this attempt's delay and
			// draws no randomness; otherwise the pinned formula. override is
			// reassigned on every path below (a fresh failure sets it, a handshake
			// clears it) before it is next read, so it needs no explicit reset here.
			var delay time.Duration
			if override != nil {
				delay = *override
			} else {
				delay = computeBackoffDelay(c.cfg.backoff, attempt, c.cfg.rand)
			}
			if !c.backoffWait(delay) {
				c.transition(c.stopTrigger())
				return
			}
			attempt++
		}

		wasFirst := !firstDialReported
		conn, dialErr := c.acquireConn(connectCtx, wasFirst)

		if dialErr != nil {
			if wasFirst {
				// Connect returns the first dial's failure (FR-001); reconnect (when
				// enabled) still proceeds in the background below.
				c.firstDial <- dialErr
				firstDialReported = true
			}
			// Discriminate on cause, not outcome: a dial aborted because Close or
			// the lifetime context canceled the root is a clean stop, not a
			// failure.
			if c.rootCtx.Err() != nil {
				c.transition(c.stopTrigger())
				return
			}
			out := c.classifyDial(connectCtx, dialErr, wasFirst)
			if c.applyOutcome(out) {
				return
			}
			override = out.retryAfter
			continue
		}

		// Reconnect-replay: on a RE-connect (a prior epoch ran — hadEpoch, NOT merely
		// "the first dial was answered") send reconnect{client_id, last_pos} to reopen
		// the server's replay window, and open the SDK's replay window so the server's
		// replayed records are tagged SourceReplay. This is sent on the LOCAL conn
		// BEFORE setConn publishes it to currentConn() — the serializer's and
		// auth-owner's send gate — so reconnect provably LEADS the resume subscribe and
		// any auth frame on the wire (FR-006); setConn-first would let a resume that was
		// pending from a prior epoch race ahead of it. The window is ASSIGNED every
		// epoch-up (open iff a reconnect was sent), never toggled, so a mid-window
		// death cannot leak an open window into an epoch that sends no reconnect. It is
		// set before runEpoch (the decode loop, same goroutine) reads it.
		sentReconnect := false
		if hadEpoch {
			sentReconnect = c.sendReconnect(conn)
		}
		c.replayWin.set(sentReconnect)

		// Connected: reset the backoff and run the epoch.
		c.setConn(conn)
		// Close (or a lifetime cancel) can land exactly as the dial completes.
		// Discriminate on cause: take the clean-stop exit rather than emit a
		// spurious →connected the caller would see after Close. terminalSequence
		// closes the stashed conn. On this path a first dial reports nothing to
		// Connect; it unblocks via doneCh (the supervisor exits), returning the
		// clean-stop nil — never a spurious success.
		if c.rootCtx.Err() != nil {
			c.transition(c.stopTrigger())
			return
		}
		attempt = 0
		override = nil
		c.transition(triggerHandshakeOK) // connecting/reconnecting → connected
		if wasFirst {
			// Report the first dial's SUCCESS only after the state is Connected, so a
			// caller whose Connect returns nil can immediately RefreshToken/Escalate
			// without racing the →connected transition (FR-001).
			c.firstDial <- nil
			firstDialReported = true
		}
		// The connection is live: let the auth-owner retry a refresh it wanted
		// while disconnected (a proactive timer that fired during backoff).
		c.upAuthOwner()
		// Re-subscribe the desired set on the new epoch: the reset cleared granted,
		// so the resume covers the whole desired set (FR-001a). Sent after upAuthOwner
		// so auth (if any) leads, though the serializer and auth-owner are independent.
		c.resumeSubscribeSerializer()

		out, rootStopped := c.runEpoch(conn)
		hadEpoch = true // an epoch ran; the next successful connect is a reconnect
		// The epoch ended. Clear the live-conn reference so the auth-owner's
		// conn-nil guard actually holds between epochs — otherwise it would fetch a
		// TokenSource and send on a dead socket while reconnecting, burning
		// connected-path strikes during a backoff FR-005 says is non-terminal.
		c.setConn(nil)
		// Tell the auth-owner to abandon any refresh outstanding on this now-dead
		// connection (its answer will never arrive).
		c.resetAuthOwner()
		// SLICE-4 HAZARD (recovery *PossibleGap snapshot): T131 must snapshot the set
		// granted in the epoch that just ended, but resetSubscribeSerializer() below
		// asynchronously clears the granted set. When that snapshot lands, read
		// grantedSnapshot() HERE — before this reset enqueue — or the snapshot races
		// the wipe and emits an empty *PossibleGap, losing the loss signal.
		//
		// Tell the serializer the epoch ended: drop the outstanding flight, disarm its
		// timeout, and clear the granted set. Enqueued here — BEFORE the next dial — so
		// the reset provably precedes any send on the reconnected epoch (the
		// drain-before-send discipline in the serializer relies on this ordering).
		c.resetSubscribeSerializer()
		// Admit the recovery epoch reset through the recovery owner's inbox — the same
		// stream as this epoch's decode-loop events (all on this goroutine) — so it is
		// ordered after every gap/replay_complete/grant of the epoch that just died and
		// before any event of the next one. Channels mid-recovery move to awaiting-grant
		// (re-driven on the new epoch's resume grant), and the per-channel replay floor
		// resets with the epoch (T128).
		c.resetRecoveryOwner()
		// Discriminate on cause, not outcome (as the dial path does): if Close or a
		// lifetime cancel raced the epoch's end — even if the heartbeat or a panic
		// won the first-cause slot first, making rootStopped false — the caller's
		// stop wins. Otherwise a Close could surface a post-Close reconnect event,
		// or deliver a non-nil *Terminal.Err for a clean stop under WithReconnect(false).
		if rootStopped || c.rootCtx.Err() != nil {
			c.transition(c.stopTrigger())
			return
		}
		if c.applyOutcome(out) {
			return
		}
		// A reconnect-class epoch termination: attempt is already 0 (reset at the
		// handshake), so the first reconnect after a healthy epoch waits Initial.
	}
}

// acquireConn performs a dial: the first one bounded by the caller's connect
// deadline, every reconnect bounded by the injectable "dial" timer.
//
//nolint:contextcheck // a reconnect's pre-dial ensure-token waits on the client-lifetime context (rootCtx), not connectCtx — connectCtx bounds only the FIRST dial, exactly as dial()/reconnectDial() already split. The first-dial branch does wait on connectCtx.
func (c *Client) acquireConn(connectCtx context.Context, first bool) (Conn, error) {
	// A TokenSource client fetches a fresh credential before EVERY dial (B1): the
	// owner is the sole TokenSource caller, so the supervisor requests it over a
	// reply chan and the fetched token reaches the handshake via the store the
	// transport reads per dial. A fetch failure becomes the dial error, which
	// classifyDial's non-HandshakeError fallthrough makes reconnect-class —
	// non-terminal forever (FR-005 line 63), never a doomed handshake. Static
	// clients keep the untouched store-read path.
	if c.cfg.tokenSource != nil {
		waitCtx := c.rootCtx
		if first {
			waitCtx = connectCtx // the first dial is also bounded by Connect(ctx)
		}
		if err := c.ensureDialToken(waitCtx); err != nil {
			return nil, c.redactor.redactError(err)
		}
	}

	var conn Conn
	var err error
	if first {
		conn, err = c.dial(connectCtx)
	} else {
		conn, err = c.reconnectDial()
	}
	// Redact at the single source: a network dial failure returns Go's *url.Error
	// with the dial URL (query-param token included), and this error flows to both
	// Connect (via firstDial) and Err() (via setErr). redactError is nil-safe and
	// leaves a non-credential error (e.g. *HandshakeError) untouched so errors.As
	// still matches.
	return conn, c.redactor.redactError(err)
}

// ensureDialToken asks the auth-owner to fetch a fresh credential and store it
// before a dial (B1, TokenSource clients only). It rendezvouses with the owner
// and waits for the reply, escaping on waitCtx (the connect deadline for the
// first dial) and rootCtx (a Close mid-fetch, which the caller discriminates as a
// clean stop). A fetch failure is returned so the dial path classifies it
// reconnect-class; a context escape is wrapped per §III and keys classifyDial's
// dial-abort branch (which tests connectCtx.Err(), not the error value).
func (c *Client) ensureDialToken(waitCtx context.Context) error {
	reply := make(chan error, 1)
	select {
	case c.authDialCred <- authDialReq{reply: reply}:
	case <-waitCtx.Done():
		return fmt.Errorf("sukko: ensure dial token: %w", waitCtx.Err())
	case <-c.rootCtx.Done():
		return fmt.Errorf("sukko: ensure dial token: %w", c.rootCtx.Err())
	}
	select {
	case err := <-reply:
		return err
	case <-waitCtx.Done():
		return fmt.Errorf("sukko: ensure dial token: %w", waitCtx.Err())
	case <-c.rootCtx.Done():
		return fmt.Errorf("sukko: ensure dial token: %w", c.rootCtx.Err())
	}
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

// reconnectDial performs a reconnect dial bounded by the injectable "dial" timer
// and the client lifetime. The timer goes through the Clock seam so a test drives
// it deterministically (BlockUntilTimer("dial") → Advance(DialTimeout)); a watcher
// goroutine cancels the dial when the timer fires and exits when the dial
// completes, so no goroutine outlives the call (goleak stays clean).
func (c *Client) reconnectDial() (Conn, error) {
	var conn Conn
	err := c.callWithTimeout(c.rootCtx, c.cfg.dialTimeout, purposeDial, func(ctx context.Context) error {
		var e error
		conn, e = c.transport.Open(ctx)
		if e != nil {
			return fmt.Errorf("sukko: reconnect dial: %w", e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// callWithTimeout runs fn under a context canceled by the injectable timer of the
// given purpose (or by the parent). It is the shared shape for the two external
// calls the SDK must bound through the clock seam rather than context.WithTimeout
// (§VII forbids real time in tests): the reconnect dial and the TokenSource fetch.
// A watcher goroutine cancels on the timer firing and exits when fn completes, so
// no goroutine outlives the call (goleak stays clean).
func (c *Client) callWithTimeout(parent context.Context, timeout time.Duration, purpose timerPurpose, fn func(context.Context) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	timer := c.clock.NewTimer(timeout, purpose)
	var wg sync.WaitGroup
	wg.Go(func() {
		select {
		case <-timer.C():
			cancel() // timed out
		case <-ctx.Done():
			timer.Stop()
		}
	})

	err := fn(ctx)
	cancel()  // wake the watcher on a completed call before it can fire the timer
	wg.Wait() // the watcher has exited before we return
	return err
}

// backoffWait sleeps for delay on the injectable "backoff" timer, returning false
// if the client lifetime ends first. A non-positive delay dials immediately (the
// Retry-After: 0 case) without arming a timer.
func (c *Client) backoffWait(delay time.Duration) bool {
	if delay <= 0 {
		return c.rootCtx.Err() == nil
	}
	timer := c.clock.NewTimer(delay, purposeBackoff)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-c.rootCtx.Done():
		return false
	}
}

// runEpoch runs one connection epoch: it spawns the heartbeat goroutine and is
// itself the decode loop — the sole reader of the socket. It returns the
// classified termination (first-cause-wins across the decode loop and the
// heartbeat goroutine) and whether the root was canceled (a clean stop, no
// outcome). Both goroutines are torn down before it returns.
func (c *Client) runEpoch(conn Conn) (out terminationOutcome, rootStopped bool) {
	e := newEpoch(c.rootCtx)
	e.conn = conn        // bind the socket for the recovery owner's epoch-gated replay sends
	c.setCurrentEpoch(e) // let the auth-owner reach this epoch's first-cause slot
	// Registered FIRST so it runs LAST (LIFO): after the decode-loop recover
	// (below) has had its chance to record a panic, cancel the epoch, wait for the
	// heartbeat goroutine to exit, then read the recorded termination. A decode
	// loop panic unwinds (it produces no read error), so reading the slot here —
	// not off a return value — is what carries a panic outcome out.
	defer func() {
		c.clearCurrentEpoch(e)
		e.cancel()
		e.wg.Wait()
		// Close the epoch's conn unconditionally (best-effort, idempotent via
		// closeOnce). Most endings already closed it — a remote close, a 1006 drop,
		// the 4999 heartbeat close, terminalSequence — but a decode-path PANIC does
		// not: the goroutine was not parked in a Read, so ctx cancellation (which
		// coder/websocket honors only for an in-flight Read/Write) never reaches the
		// socket, and reconnecting would leak the fd and the server tenant slot.
		_ = conn.Close(closeCodeNormalClosure, "")
		out, rootStopped = e.result()
	}()
	defer c.recoverEpoch(e, "decode")

	e.wg.Go(func() { c.runHeartbeat(e, conn) })

	for {
		data, err := conn.Read(e.ctx)
		if err == nil {
			c.dispatch(e, data)
			continue
		}

		// Priority: root cancel (a clean stop) first, then a termination already
		// recorded by the heartbeat goroutine (a 4999 timeout or its own panic),
		// then classify the read error itself.
		if c.rootCtx.Err() != nil {
			e.recordRootStop()
			return
		}
		if e.hasTerminated() {
			return
		}

		var ce *CloseError
		if errors.As(err, &ce) {
			e.record(c.classifyClose(ce))
			return
		}
		// An unexpected read error with the root still live — terminal.
		e.record(terminationOutcome{
			class:   classTerminal,
			cause:   fmt.Errorf("sukko: read: %w", err),
			trigger: triggerTerminalFailure,
		})
		return
	}
}

// applyOutcome enacts a classified outcome. It emits the outcome's surface event
// (using the root context so a between-epochs lifecycle event parks until Close
// rather than being discarded on epoch teardown), then acts on the class:
// terminal and clean-stop set the terminal state and return true (the loop
// exits); reconnect transitions to reconnecting and returns false (the loop
// re-dials).
func (c *Client) applyOutcome(out terminationOutcome) (done bool) {
	if out.surface != nil {
		c.surface(out.surface)
	}
	switch out.class {
	case classReconnect:
		c.transition(out.trigger) // → reconnecting
		return false
	case classCleanStop:
		// The WithReconnect(false) downgrade: Err() stays nil, but *Terminal.Err
		// carries the cause for diagnosis. (A genuine caller Close never reaches
		// here — it ends the epoch via root cancellation, the rootStopped path.)
		c.setTerminalCause(out.cause)
		c.transition(out.trigger)
		return true
	default: // classTerminal
		c.setErr(out.cause)
		c.setTerminalCause(out.cause)
		c.transition(out.trigger)
		return true
	}
}

// classifyDial resolves a failed dial to an outcome. first distinguishes the
// caller's Connect(ctx) deadline (a dial-abort that keeps reconnecting in the
// background) from a reconnect dial's own timeout.
func (c *Client) classifyDial(connectCtx context.Context, err error, first bool) terminationOutcome {
	var he *HandshakeError
	if errors.As(err, &he) {
		out := lookupHandshakePolicy(he.Status, he.Code, c.cfg.reconnect)
		var retryAfter *time.Duration
		if out.retryAfterOverrides && he.RetryAfter != nil {
			retryAfter = he.RetryAfter
		}
		return terminationOutcome{
			class:      out.class,
			cause:      he,
			surface:    he, // *HandshakeError is an Event, so a 429 loop is never silent
			trigger:    triggerForClass(out.class),
			retryAfter: retryAfter,
		}
	}

	// A caller-ctx-expired FIRST dial is a dial-abort: reconnect keeps trying in
	// the background (WithReconnect(false) downgrades it to a clean stop). Any
	// other network/TLS error — or a reconnect dial's own timeout — is a plain
	// reconnect-class failure.
	trig := triggerReconnectClassFailure
	if first && connectCtx.Err() != nil {
		trig = triggerDialAborted
	}
	// Route the WithReconnect(false) downgrade through the single applyReconnectFlag
	// rule rather than a fourth inline copy of it (file header: no second copy).
	out := applyReconnectFlag(closeOutcome{class: classReconnect}, c.cfg.reconnect)
	return terminationOutcome{class: out.class, cause: err, trigger: trig}
}

// classifyClose resolves a closed epoch to an outcome via the close policy.
func (c *Client) classifyClose(ce *CloseError) terminationOutcome {
	out, ok := lookupClosePolicy(closeKey{code: ce.Code, direction: ce.Direction, reconnectEnabled: c.cfg.reconnect})
	if !ok {
		// An unenumerated close code is terminal rather than silently reconnected.
		return terminationOutcome{class: classTerminal, cause: ce, surface: ce, trigger: triggerTerminalFailure}
	}

	var surface Event
	if out.surfacesCloseError {
		surface = ce
	}

	switch out.class {
	case classReconnect:
		return terminationOutcome{class: classReconnect, cause: ce, surface: surface, trigger: triggerReconnectClassFailure}
	case classCleanStop:
		if out.surfacesCloseError {
			// The WithReconnect(false) downgrade of a reconnect-class close:
			// *CloseError, then *Terminal{cause}, then close, with Err() nil.
			return terminationOutcome{class: classCleanStop, cause: ce, surface: ce, trigger: triggerReconnectClassFailure}
		}
		// A genuine local normal-closure — caller-driven, no cause to report.
		return terminationOutcome{class: classCleanStop, trigger: triggerCloseCalled}
	default: // classTerminal
		return terminationOutcome{class: classTerminal, cause: ce, surface: surface, trigger: triggerTerminalFailure}
	}
}

// stopTrigger picks the state-machine trigger for a supervisor exit that saw the
// root context canceled. A canceled root with a failure cause already recorded
// (c.err != nil) means a failure initiated the teardown — today an auth-owner
// panic, which records its *InternalError and cancels root (helpers record, the
// supervisor emits) — so the client lands in StateError. A canceled root with no
// cause is a genuine Close or lifetime cancel: StateClosed. Close never sets
// c.err, and the WithReconnect(false) downgrade sets only terminalCause via
// applyOutcome (which never reaches these root-canceled branches), so the
// discriminator is unambiguous.
func (c *Client) stopTrigger() trigger {
	c.mu.Lock()
	failed := c.err != nil
	c.mu.Unlock()
	if failed {
		return triggerTerminalFailure
	}
	return triggerCloseCalled
}

// triggerForClass maps a termination class to its state-machine trigger. The
// clean-stop case is the WithReconnect(false) downgrade of a reconnect-class
// outcome, so it carries the reconnect trigger — the state machine downgrades it
// to StateClosed under the reconnect flag, keeping the class and the state in
// agreement without a second rule here.
func triggerForClass(class terminationClass) trigger {
	if class == classTerminal {
		return triggerTerminalFailure
	}
	return triggerReconnectClassFailure
}

// dispatch routes one decoded frame. Data and surfaced advisories go to the
// delivery channel in receive order; unknown types surface as *UnknownEvent
// (forward compatibility); control frames whose disposition is not "surface"
// carry their effect in a later phase and surface nothing here.
func (c *Client) dispatch(e *epoch, data []byte) {
	decoded, unknown, err := decodeFrame(data)
	if err != nil {
		// Unreadable frame. Surfaced as a protocol error so drift is visible;
		// the epoch-tear-on-non-JSON refinement (FR-002 liveness) is a later task.
		c.forward(e.ctx, &ProtocolError{Message: "unreadable frame", Cause: err})
		return
	}
	if unknown != "" {
		c.counters.unknownEvents.Add(1)
		c.forward(e.ctx, &UnknownEvent{Type: unknown, Raw: bytes.Clone(data)})
		return
	}

	// A pong's whole effect is resetting the heartbeat's pong deadline; it
	// surfaces nothing. Signal the heartbeat goroutine (non-blocking — a prior
	// unconsumed signal already covers this pong).
	if _, ok := decoded.(*wirePong); ok {
		select {
		case e.pongCh <- struct{}{}:
		default:
		}
		return
	}

	// auth_ack and auth_error answer an in-flight auth (refresh or escalation):
	// record the answer in the inbox and poke the auth-owner (it clears its flight,
	// commits an escalation's JWT, and re-arms the schedule). auth_ack also surfaces
	// *Authenticated in receive order, labeled with the in-flight mode — read BEFORE
	// poking so a new flight the owner may start on the poke cannot relabel this ack
	// (the wire carries no mode; it is the SDK's own owned state). auth_error falls
	// through to surfaceEvent below, forwarding *AuthError.
	switch f := decoded.(type) {
	case *wireAuthAck:
		mode := c.currentFlightMode()
		c.authInbox.putAck(f.Data.Exp)
		c.pokeAuthOwner()
		c.forward(e.ctx, &Authenticated{Exp: f.Data.Exp, Mode: mode})
		return
	case *wireAuthError:
		c.authInbox.putError()
		c.pokeAuthOwner()
	case *wireSubscriptionAck:
		// Reconcile the grant against the serializer's outstanding requested set and
		// emit *SubscriptionResult HERE, in receive order (like *Authenticated). Poke
		// to release the slot ONLY if this ack matched the outstanding SUBSCRIBE — a
		// stale/duplicate ack (or one arriving after a partial-send cleared the flight)
		// must not release the NEXT flight and roll mis-attribution forward.
		// Control-plane: hand off, never enqueue.
		if g, ok := c.reconcileSubscriptionAck(e.ctx, f.Subscribed, f.Count); ok {
			c.pokeSubscribeSerializer(g)
			// Hand the granted channels to the recovery owner: on a reconnect the resume
			// subscribe re-grants the desired set, which is the trigger to re-drive any
			// replay retained across the epoch boundary (T130). A grant with no retained
			// recovery is a cheap no-op.
			c.admitGrant(e, f.Subscribed)
		}
		return
	case *wireUnsubscriptionAck:
		// A client-initiated unsubscribe (forced:false) answers the outstanding
		// unsubscribe — release the slot, but only if an unsubscribe is actually
		// outstanding (a stale/duplicate ack must not release the next flight). A
		// forced (unsolicited) ack is not a reply to a client request and must NOT
		// release the slot; it still surfaces *Unsubscribed below in receive order.
		if f.Forced {
			// A server-forced removal (e.g. an auth-refresh downgrade): prune the
			// granted set ONLY, keeping desired — the channel moves to
			// PendingSubscriptions() so a later escalation delta can re-subscribe it
			// (events.go). It does not release the slot (it answers no client request).
			c.subs.pruneGranted(f.Unsubscribed)
		} else if g, ok := c.matchUnsubscriptionAck(f.Unsubscribed); ok {
			c.pokeSubscribeSerializer(g)
		}
	case *wireSubscribeError:
		// Release the outstanding subscribe's slot (gen-matched only), then fall
		// through to surface the *ProtocolError in receive order.
		if g, ok := c.outstandingGen(reqSubscribe); ok {
			c.pokeSubscribeSerializer(g)
		}
	case *wireUnsubscribeError:
		// Symmetric with subscribe_error: release the outstanding unsubscribe's slot,
		// then fall through to surface the *ProtocolError.
		if g, ok := c.outstandingGen(reqUnsubscribe); ok {
			c.pokeSubscribeSerializer(g)
		}
	case *wireReconnectAck, *wireReconnectError:
		// The reconnect-replay round-trip is answered: close the window so subsequent
		// records are tagged live again (no more SourceReplay override). Then fall
		// through to surface *Resumed / *ReconnectError in receive order.
		c.replayWin.close()
	case *wireGap:
		// A server gap advisory. Count every one received, and hand a well-formed gap
		// to the recovery owner to drive a live gap→replay (T127/T128). An empty
		// last_pos is un-anchorable — it surfaces *Gap but drives no replay; its
		// *PossibleGap pairing lands in Slice 4. Falls through to surface *Gap in
		// receive order.
		c.counters.gaps.Add(1)
		if f.Channel != "" && f.LastPos != "" {
			c.admitGap(e, f.Channel, f.LastPos)
		}
	case *wireReplayComplete:
		// A replay terminator: hand it to the recovery owner to close out the cycle
		// (or begin a floor-gated follow-up). A replay REJECTION (a `wireError` with a
		// channel → *ReplayError) is deliberately NOT handled by the owner in Slice 2 —
		// the recovery deadline (Slice 3) unwedges a stuck REPLAYING and the un-
		// anchorable/degrade paths (Slice 4) handle the failure classes. Falls through
		// to surface *ReplayComplete in receive order.
		if f.Channel != "" {
			c.admitReplayComplete(e, f.Channel)
		}
	}

	ev, serr := surfaceEvent(decoded)
	switch {
	case serr == nil:
		// Recovery post-processing on the decode goroutine: for a message-class event,
		// resolve the final Source (reconnect-replay window override) and update the
		// pos cursor — Source FIRST, then the cursor keyed on it, so a replayed record
		// never advances the cursor (FR-006).
		if m, ok := ev.(*Message); ok {
			c.applyRecovery(m)
		}
		c.forward(e.ctx, ev)
	case errors.Is(serr, errNotSurfaceFrame):
		// A control-derived (subscription_ack, auth_ack) frame. Its side effects —
		// grant diff, auth mode — belong to later phases; none reach a client that
		// cannot yet subscribe or authenticate.
	default:
		// A presence/protocol failure: surfaceEvent returned a *ProtocolError,
		// which is itself an Event.
		if pe, ok := serr.(Event); ok {
			c.forward(e.ctx, pe)
		}
	}
}

// forward sends a data or advisory event through the epoch context, counting the
// data plane. An event forwarded during a live epoch is discarded if that epoch
// tears down mid-send (the single-event carve-out).
func (c *Client) forward(epochCtx context.Context, ev Event) {
	if _, ok := ev.(*Message); ok {
		c.counters.messagesReceived.Add(1)
	}
	c.delivery.send(c.rootCtx, epochCtx, ev)
}

// surface emits a lifecycle event (state change, close error, handshake error)
// using the root context for BOTH delivery slots. These events are emitted
// between epochs, when the epoch context is already dead, so they must park until
// Close rather than be discarded on epoch teardown.
func (c *Client) surface(ev Event) {
	c.delivery.send(c.rootCtx, c.rootCtx, ev)
}

// transition advances the state machine via a trigger and emits one *StateChange
// per real transition. The target comes from the stateTransitions table under the
// reconnect flag, so a reconnect-class failure lands in StateReconnecting with
// reconnect on and StateClosed with it off — the same downgrade the termination
// tables apply. A trigger with no defined transition from the current state is a
// no-op and emits nothing.
func (c *Client) transition(tr trigger) {
	c.mu.Lock()
	from := c.state
	to, ok := lookupStateTransitionWithReconnect(from, tr, c.cfg.reconnect)
	if !ok || from == to {
		c.mu.Unlock()
		return
	}
	c.state = to
	c.mu.Unlock()

	c.surface(&StateChange{From: from, To: to})
}

// terminalSequence is the client's teardown, performed exactly once by whichever
// path reaches it — the supervisor's exit, or Close when no supervisor ran. It
// runs after every delivery sender has exited (the decode loop is done), so the
// non-blocking *Terminal send into its reserved slot cannot be starved.
func (c *Client) terminalSequence() {
	c.terminalOnce.Do(func() {
		c.mu.Lock()
		conn := c.conn
		cause := c.terminalCause
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

// currentConn returns the live connection for the auth-owner to send `auth` on,
// or nil between epochs. A send on a just-closed conn returns an error the owner
// treats as a no-op, so a small stale window is harmless.
func (c *Client) currentConn() Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

func (c *Client) setCurrentEpoch(e *epoch) {
	c.mu.Lock()
	c.currentEpoch = e
	c.mu.Unlock()
}

// clearCurrentEpoch nils the reference only if it is still e — a fast reconnect
// may have already installed the next epoch.
func (c *Client) clearCurrentEpoch(e *epoch) {
	c.mu.Lock()
	if c.currentEpoch == e {
		c.currentEpoch = nil
	}
	c.mu.Unlock()
}

func (c *Client) currentEpochRef() *epoch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentEpoch
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

func (c *Client) setTerminalCause(err error) {
	c.mu.Lock()
	c.terminalCause = err
	c.mu.Unlock()
}

func (c *Client) setTerminalCauseIfNil(err error) {
	c.mu.Lock()
	if c.terminalCause == nil {
		c.terminalCause = err
	}
	c.mu.Unlock()
}
