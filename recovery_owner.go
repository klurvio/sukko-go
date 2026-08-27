package sukko

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"
)

// The recovery owner is the third supervisor-lifetime single-owner goroutine
// launched in run() (alongside the auth-owner and the subscribe serializer). It owns the recovery
// FSM (recovery.go) and is the sole sender of `replay` frames.
//
// It exists for the same reason the serializer does: recovery is stateful and
// must not race the data plane. The DECODE loop hands off — it admits gap /
// replay_complete / grant events to the inbox and pokes; the owner drains them and
// performs the FSM's replay sends. The decode loop and the supervisor's epoch
// reset are the SAME goroutine (runEpoch is the decode loop), so admitting the
// reset through this SAME inbox makes the event stream totally ordered: every
// event is processed relative to the epoch that produced it, and a reset applies
// to exactly the epoch that just died. That total order is what lets the owner
// gate each send on the current epoch with a simple pointer compare, with no
// drain-at-send race window — a deliberate departure from the serializer's
// drain-after-read discipline, which suits inputs that are NOT epoch-ordered and
// whose state self-corrects by recomputation; the recovery FSM is neither.

// recoveryEventKind tags an inbox event.
type recoveryEventKind uint8

const (
	// recEvGap: a server `gap` advisory (channel + anchor last_pos).
	recEvGap recoveryEventKind = iota
	// recEvReplayComplete: a `replay_complete` terminator for a channel.
	recEvReplayComplete
	// recEvGrant: the channels a subscription_ack granted — the re-drive trigger.
	recEvGrant
	// recEvReplayFailure: a per-channel replay rejection (a server `error` frame
	// carrying the channel) — interrupts an in-flight replay.
	recEvReplayFailure
	// recEvReset: an epoch boundary (admitted by the supervisor at epoch-down).
	recEvReset
)

// recoveryEvent is one item handed from the decode loop / supervisor to the owner.
type recoveryEvent struct {
	kind    recoveryEventKind
	channel string // recEvGap, recEvReplayComplete
	lastPos string // recEvGap
	// channels is the granted set for recEvGrant.
	channels []string
	// epoch is the epoch that produced the event — the send-gate identity. Nil for
	// recEvReset (which carries no send and is ordered by the stream alone).
	epoch *epoch
}

// recoveryInbox is the lossless hand-off: a mutex-guarded FIFO the decode loop and
// supervisor append to and the owner drains fully on each poke. It is NOT a
// drop-on-full buffered channel — a dropped gap is silent data loss and a dropped
// replay_complete would wedge a channel in REPLAYING forever (there is no recovery
// deadline until Slice 3). The buffered-1 poke may coalesce; the queue never does.
type recoveryInbox struct {
	mu     sync.Mutex
	events []recoveryEvent
}

func newRecoveryInbox() *recoveryInbox { return &recoveryInbox{} }

func (b *recoveryInbox) put(ev recoveryEvent) {
	b.mu.Lock()
	b.events = append(b.events, ev)
	b.mu.Unlock()
}

// drain returns and clears the queued events in arrival order.
func (b *recoveryInbox) drain() []recoveryEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.events
	b.events = nil
	return out
}

// runRecoveryOwner is the single-owner recovery goroutine: it drains the inbox in
// order, drives the FSM, performs the FSM's epoch-gated replay sends, and manages
// the one replay-floor timer (armed to the earliest pending floor wake — the
// next_deadline pattern; the Slice-3 recovery deadline folds into the same
// minimum). It never reads or writes the FSM off this goroutine, so the FSM needs
// no lock.
func (c *Client) runRecoveryOwner(ownerCtx context.Context) {
	defer c.recoverRecoveryOwner()

	fsm := newRecoveryFSM(c.cfg.replayFloor, c.cfg.recoveryDeadline)

	var wakeTimer Timer
	var wakeC <-chan time.Time
	disarmWake := func() {
		if wakeTimer != nil {
			wakeTimer.Stop()
			wakeTimer, wakeC = nil, nil
		}
	}
	// rearmWake re-arms the single owner timer to the earliest pending wake — a floor
	// wake (purposeReplayFloor) or a recovery deadline (purposeRecoveryDeadline),
	// whichever is sooner — or disarms it when nothing is pending. The purpose tracks
	// the winning class so a test rendezvouses on the right timer. A wake already in
	// the past arms a zero delay (fires next tick) rather than looping — a channel
	// whose epoch died is moved off FLOOR_WAIT by the reset or due's epoch gate, and a
	// suspended deadline re-arms to a future time, so no wake persists to spin here.
	rearmWake := func() {
		disarmWake()
		wake, purpose, ok := fsm.nextWake()
		// Fold the single-flight history deadline into the same timer — it arms under
		// purposeRecoveryDeadline like a replay deadline, so the owner runs one timer for
		// both recovery flows.
		if hWake, hOk := c.historyFlight.nextDeadline(); hOk && (!ok || hWake.Before(wake)) {
			wake, purpose, ok = hWake, purposeRecoveryDeadline, true
		}
		if !ok {
			return
		}
		after := max(wake.Sub(c.clock.Now()), 0)
		wakeTimer = c.clock.NewTimer(after, purpose)
		wakeC = wakeTimer.C()
	}
	// exec performs the FSM's replay sends. Each action carries the epoch the FSM
	// chose (always the current one), and the frame goes out on THAT epoch's bound
	// conn — so a replay can only ever reach the epoch on which its channel is
	// subscribed. A failed send (the epoch is already dying) is cleaned up by the
	// stream-ordered reset that follows it; the counter tracks only frames that left.
	exec := func(actions []replayAction) {
		for _, a := range actions {
			if a.epoch != nil && a.epoch.conn != nil && c.sendReplayFrame(ownerCtx, a.epoch.conn, a.channel, a.fromPos) {
				c.counters.replays.Add(1)
			}
		}
	}
	// surfaceInterrupt emits a *RecoveryInterruptedError via ownerSurface — the
	// classed-reserve safety path (parks on the owner context, discarded only on owner
	// teardown), the same emission the auth-owner uses. It is called AFTER the FSM /
	// historyFlight decision has released its lock (§VII: never surface under the lock,
	// which parks on back-pressure).
	surfaceInterrupt := func(kind, channel string) {
		c.ownerSurface(ownerCtx, &RecoveryInterruptedError{Kind: kind, Channel: channel})
	}
	interruptReplay := func(channels []string) {
		for _, ch := range channels {
			surfaceInterrupt(RecoveryKindReplay, ch)
		}
	}

	for {
		select {
		case <-ownerCtx.Done():
			disarmWake()
			return
		case <-c.recoveryPoke:
			// Read the tick FRESH per event: a drained batch can span an epoch boundary
			// (a reset then the next epoch's grant), and each event must be gated against
			// the epoch live — and the park state observed — when it is processed.
			for _, ev := range c.recoveryInbox.drain() {
				switch ev.kind {
				case recEvGap:
					exec(fsm.handleGap(ev.channel, ev.lastPos, ev.epoch, c.recoveryTick()))
				case recEvReplayComplete:
					exec(fsm.handleReplayComplete(ev.channel, ev.epoch, c.recoveryTick()))
				case recEvGrant:
					exec(fsm.handleGrant(ev.channels, ev.epoch, c.recoveryTick()))
				case recEvReplayFailure:
					interruptReplay(fsm.handleReplayFailure(ev.channel))
				case recEvReset:
					interruptReplay(fsm.handleReset(c.recoveryTick()))
					// An epoch boundary also interrupts an in-flight history (it does not
					// re-drive) — promptly, on the reset, rather than waiting for its deadline.
					if ch := c.historyFlight.interruptIfEpochDead(c.currentEpochRef()); ch != "" {
						surfaceInterrupt(RecoveryKindHistory, ch)
					}
				}
			}
		case <-wakeC:
			wakeTimer, wakeC = nil, nil
			t := c.recoveryTick()
			out := fsm.due(t)
			exec(out.replays)
			interruptReplay(out.interrupts)
			// The history deadline shares this timer: an elapsed history with no
			// terminator (under a live, matching epoch and a draining consumer) is
			// interrupted; a consumer stall suspends it.
			if ch := c.historyFlight.due(t, c.cfg.recoveryDeadline); ch != "" {
				surfaceInterrupt(RecoveryKindHistory, ch)
			}
		}
		rearmWake()
	}
}

// recoveryTick snapshots the owner's clock/epoch context and the delivery park
// state for one FSM call, so the FSM reads no clock or delivery state of its own.
func (c *Client) recoveryTick() tick {
	return tick{
		now:      c.clock.Now(),
		current:  c.currentEpochRef(),
		episodes: c.delivery.parkEpisodes(),
		parked:   c.delivery.isParked(),
	}
}

// sendReplayFrame marshals and sends `replay{channel, from_pos}` on conn,
// reporting whether it was sent. A send error means the socket is going away; the
// decode loop classifies it.
func (c *Client) sendReplayFrame(ownerCtx context.Context, conn Conn, channel, fromPos string) bool {
	frame, err := json.Marshal(wireReplay{Type: typeReplay, Data: replayPayload{Channel: channel, FromPos: fromPos}})
	if err != nil {
		return false // unreachable: a struct of strings always marshals
	}
	return conn.Send(ownerCtx, frame) == nil
}

// admitGap hands a well-formed gap to the recovery owner (decode goroutine).
func (c *Client) admitGap(e *epoch, channel, lastPos string) {
	c.recoveryInbox.put(recoveryEvent{kind: recEvGap, channel: channel, lastPos: lastPos, epoch: e})
	c.pokeRecoveryOwner()
}

// admitReplayComplete hands a replay terminator to the recovery owner (decode
// goroutine).
func (c *Client) admitReplayComplete(e *epoch, channel string) {
	c.recoveryInbox.put(recoveryEvent{kind: recEvReplayComplete, channel: channel, epoch: e})
	c.pokeRecoveryOwner()
}

// admitReplayFailure hands a per-channel replay rejection to the recovery owner
// (decode goroutine), so it interrupts the in-flight replay. It carries no epoch —
// the stream ordering (failure before the epoch's reset) makes the FSM's phase
// check sufficient, so no send-gate identity is needed.
func (c *Client) admitReplayFailure(channel string) {
	c.recoveryInbox.put(recoveryEvent{kind: recEvReplayFailure, channel: channel})
	c.pokeRecoveryOwner()
}

// admitGrant hands the channels a subscription_ack granted to the recovery owner
// (decode goroutine), so it re-drives any retained replay for those channels on
// this epoch (T130).
func (c *Client) admitGrant(e *epoch, channels []string) {
	if len(channels) == 0 {
		return
	}
	c.recoveryInbox.put(recoveryEvent{kind: recEvGrant, channels: slices.Clone(channels), epoch: e})
	c.pokeRecoveryOwner()
}

// resetRecoveryOwner tells the recovery owner an epoch ended. It is admitted
// through the SAME inbox as the decode events — and by the same goroutine (the
// decode loop is the supervisor) — so it lands after every event of the epoch that
// just died and before any event of the next one, keeping the stream totally
// ordered. Called at epoch-down, before the next dial.
func (c *Client) resetRecoveryOwner() {
	c.recoveryInbox.put(recoveryEvent{kind: recEvReset})
	c.pokeRecoveryOwner()
}

// pokeRecoveryOwner wakes the owner to drain the inbox. Non-blocking (buffered 1):
// a prior unconsumed poke already covers this admission, since the owner drains
// the whole queue on each wake.
func (c *Client) pokeRecoveryOwner() {
	select {
	case c.recoveryPoke <- struct{}{}:
	default:
	}
}

// recoverRecoveryOwner routes a recovery-owner panic to a terminal, mirroring the
// auth-owner and serializer (ADR-0010): record the cause, then cancel root so the
// supervisor tears down with the cause set (the owner never calls terminalSequence).
func (c *Client) recoverRecoveryOwner() {
	r := recover()
	if r == nil {
		return
	}
	ie := &InternalError{Op: "recovery-owner", Value: fmt.Sprint(r), Stack: string(debug.Stack())}
	c.cfg.logger.Error("sukko: recovery-owner panic", "value", fmt.Sprint(r))
	c.setErrIfNil(ie)
	c.setTerminalCauseIfNil(ie)
	c.rootCancel()
}
