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

	fsm := newRecoveryFSM(c.cfg.replayFloor)

	var floorTimer Timer
	var floorC <-chan time.Time
	disarmFloor := func() {
		if floorTimer != nil {
			floorTimer.Stop()
			floorTimer, floorC = nil, nil
		}
	}
	// rearmFloor re-arms the single floor timer to the earliest pending floor wake,
	// or disarms it when nothing is floor-waiting. A wake already in the past arms a
	// zero delay (fires on the next tick) rather than looping — but a channel whose
	// epoch has died is moved off FLOOR_WAIT by the reset (or by due's epoch gate),
	// so a live-but-unsendable FLOOR_WAIT never persists to spin here.
	rearmFloor := func() {
		disarmFloor()
		wake, ok := fsm.nextFloorWake()
		if !ok {
			return
		}
		after := max(wake.Sub(c.clock.Now()), 0)
		floorTimer = c.clock.NewTimer(after, purposeReplayFloor)
		floorC = floorTimer.C()
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

	for {
		select {
		case <-ownerCtx.Done():
			disarmFloor()
			return
		case <-c.recoveryPoke:
			// Read the current epoch FRESH per event: a drained batch can span an
			// epoch boundary (a reset then the next epoch's grant), and each event must
			// be gated against the epoch live when it is processed.
			for _, ev := range c.recoveryInbox.drain() {
				switch ev.kind {
				case recEvGap:
					exec(fsm.handleGap(ev.channel, ev.lastPos, c.clock.Now(), c.currentEpochRef(), ev.epoch))
				case recEvReplayComplete:
					exec(fsm.handleReplayComplete(ev.channel, c.clock.Now(), c.currentEpochRef(), ev.epoch))
				case recEvGrant:
					exec(fsm.handleGrant(ev.channels, c.clock.Now(), c.currentEpochRef(), ev.epoch))
				case recEvReset:
					fsm.handleReset()
				}
			}
		case <-floorC:
			floorTimer, floorC = nil, nil
			exec(fsm.due(c.clock.Now(), c.currentEpochRef()))
		}
		rearmFloor()
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
