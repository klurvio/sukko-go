package sukko

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// An epoch is one connection's goroutine set: the decode loop (the supervisor
// goroutine itself) plus the heartbeat goroutine. They share this struct — the
// epoch context, its cancel, a WaitGroup for the spawned goroutines, the pong
// signal channel, and a first-cause-wins termination slot. It is created fresh
// per dial and torn down wholesale on every reconnect; no goroutine survives its
// epoch (FR-009). Only timers are epoch-scoped.
type epoch struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// pongCh carries the "a pong arrived" signal from the decode loop to the
	// heartbeat goroutine. Buffered (1) and scoped to this epoch, so a stale
	// signal can never leak into the next epoch's heartbeat.
	pongCh chan struct{}

	// mu guards the termination slot. The slot is first-cause-wins: whichever
	// goroutine ends the epoch (a classified close, a heartbeat timeout, a
	// recovered panic, or a root-cancel clean stop) records first and the rest
	// no-op.
	mu         sync.Mutex
	terminated bool
	rootStop   bool
	outcome    terminationOutcome
}

// newEpoch derives a fresh epoch from the client-lifetime context.
func newEpoch(rootCtx context.Context) *epoch {
	ctx, cancel := context.WithCancel(rootCtx)
	return &epoch{ctx: ctx, cancel: cancel, pongCh: make(chan struct{}, 1)}
}

// record stores the first termination outcome to arrive and cancels the epoch to
// unblock the other goroutine. Later callers no-op (first-cause-wins). cancel is
// called outside the lock (§VII: no work held under the mutex) and is idempotent.
func (e *epoch) record(out terminationOutcome) {
	e.mu.Lock()
	if !e.terminated {
		e.terminated = true
		e.outcome = out
	}
	e.mu.Unlock()
	e.cancel()
}

// recordRootStop marks a clean stop caused by root cancellation (Close or a
// lifetime cancel), which carries no outcome.
func (e *epoch) recordRootStop() {
	e.mu.Lock()
	if !e.terminated {
		e.terminated = true
		e.rootStop = true
	}
	e.mu.Unlock()
	e.cancel()
}

// hasTerminated reports whether an outcome (or a root stop) was already recorded,
// so the decode loop can defer to a heartbeat-timeout/panic termination rather
// than re-classify the read error the cancel produced.
func (e *epoch) hasTerminated() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.terminated
}

// result returns the recorded termination.
func (e *epoch) result() (terminationOutcome, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.outcome, e.rootStop
}

// recoverEpoch is the FIRST defer at every per-epoch goroutine entry (§V: a bare
// inline recover() is forbidden). It converts a panic into a reconnect-class
// epoch failure — a panic is treated as a disconnect (§IV), so the supervisor
// tears the epoch down and reconnects through the normal backoff path rather than
// killing the caller's process (§VI). It records an *InternalError outcome (the
// supervisor emits it in-band via applyOutcome — helpers record, the supervisor
// is the single delivery sender) and logs the redacted panic.
func (c *Client) recoverEpoch(e *epoch, op string) {
	r := recover()
	if r == nil {
		return
	}
	ie := &InternalError{
		Op:    op,
		Value: fmt.Sprint(r),
		Stack: string(debug.Stack()),
	}
	c.cfg.logger.Error("sukko: recovered panic", "op", op, "value", fmt.Sprint(r))

	co, ok := lookupInternalCausePolicy(causeRecoveredPanic, c.cfg.reconnect)
	if !ok {
		// An unenumerated internal cause is terminal, never a silent reconnect
		// (§IX): the zero closeOutcome's class is classReconnect, which is exactly
		// the "zero value reads as reconnect" trap the policy tables guard against.
		e.record(terminationOutcome{class: classTerminal, cause: ie, surface: ie, trigger: triggerTerminalFailure})
		return
	}
	e.record(terminationOutcome{
		class:   co.class,
		cause:   ie,
		surface: ie,
		trigger: triggerForClass(co.class),
	})
}

// heartbeatFrame is the app-level heartbeat, marshaled once from the contract
// struct so the wire shape tracks the type rather than a literal (§I).
var heartbeatFrame = func() []byte {
	b, err := json.Marshal(wireHeartbeat{Type: typeHeartbeat})
	if err != nil {
		panic(fmt.Sprintf("sukko: marshaling the constant heartbeat frame: %v", err))
	}
	return b
}()

// runHeartbeat sends an application-level heartbeat every HeartbeatInterval and
// arms a pong deadline after each send; a pong (signaled by the decode loop on
// e.pongCh) disarms it. If the deadline fires the link is dead: it records a
// local 4999 close (reconnect-class) — classified DIRECTLY as {4999, local}, not
// read back through the transport's remote-labeled path — and closes the conn to
// unblock the decode loop. The deadline is SUSPENDED while the decode loop is
// parked on back-pressure, so the SDK never preempts the server's own 1008
// slow-client verdict.
func (c *Client) runHeartbeat(e *epoch, conn Conn) {
	defer c.recoverEpoch(e, "heartbeat")

	ticker := c.clock.NewTicker(c.cfg.heartbeatInterval, purposeHeartbeat)
	defer ticker.Stop()

	var pong Timer
	var pongC <-chan time.Time
	var armEpisodes int64 // back-pressure episode count when the deadline was armed

	arm := func() {
		armEpisodes = c.delivery.parkEpisodes()
		pong = c.clock.NewTimer(c.cfg.pongTimeout, purposePong)
		pongC = pong.C()
	}
	disarm := func() {
		if pong != nil {
			pong.Stop()
			pong = nil
			pongC = nil
		}
	}
	defer disarm()

	for {
		select {
		case <-e.ctx.Done():
			return

		case <-ticker.C():
			if err := conn.Send(e.ctx, heartbeatFrame); err != nil {
				// The read side owns wire-error classification; a failed send just
				// means the socket is going away — let the decode loop report it.
				return
			}
			// PongTimeout < HeartbeatInterval (validated), so a prior deadline has
			// always resolved by now; arm only when disarmed.
			if pong == nil {
				arm()
			}

		case <-e.pongCh:
			disarm()

		case <-pongC:
			// The pong deadline fired. A pong that landed at the same instant wins
			// over the timeout.
			select {
			case <-e.pongCh:
				disarm()
				continue
			default:
			}
			// Suspend rather than close if the decode loop was parked on
			// back-pressure at ANY point during this deadline's window (parked now,
			// or a park episode began since the deadline was armed). It is not
			// processing pongs while parked, so a missed pong is the slow consumer's
			// doing — the server's 1008 owns that verdict, and the SDK must not
			// preempt it. Point-sampling isParked() alone would miss a park that
			// opened and closed while a backlog drained.
			if c.delivery.isParked() || c.delivery.parkEpisodes() != armEpisodes {
				arm()
				continue
			}
			e.record(c.classifyClose(&CloseError{Code: CloseCodeHeartbeatTimeout, Direction: directionLocal}))
			_ = conn.Close(CloseCodeHeartbeatTimeout, "heartbeat timeout")
			return
		}
	}
}
