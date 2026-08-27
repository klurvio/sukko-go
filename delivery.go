package sukko

import (
	"context"
	"sync"
	"time"
)

// The delivery channel and its classed reserve (FR-001a, ADR-0006).
//
// Messages() hands the caller this raw bounded channel, so the SDK has no hook
// on receives and cannot keep a true occupancy count — only len(ch). Yet the
// channel's top AdvisoryHeadroom slots must be reserved by class, so an
// unbounded producer (a publish-rate event) can never starve a bounded safety
// producer, and the final *Terminal always has a slot. Admission is therefore a
// lock plus a reservation counter, with the channel send performed outside the
// lock; the parked-sender wake, which no channel primitive provides, is a
// clock-driven re-probe. The full argument is in ADR-0006.

// deliveryProbeInterval is how often a send parked on back-pressure re-checks its
// class ceiling. It bounds resume latency under sustained back-pressure only —
// paid while a consumer is already behind, invisible behind the backlog — and in
// tests fires deterministically via the fake clock.
const deliveryProbeInterval = 5 * time.Millisecond

// discardReason reports the fate of a send. A parked send is discarded when a
// context cancels, and the two contexts mean different things to the supervisor
// (a root teardown versus an epoch reconnect), so the reason distinguishes them
// even though the parked event's fate is identical.
type discardReason int

const (
	notDiscarded discardReason = iota
	discardedRoot
	discardedEpoch
)

func (r discardReason) String() string {
	switch r {
	case notDiscarded:
		return "delivered"
	case discardedRoot:
		return "discarded (root canceled)"
	case discardedEpoch:
		return "discarded (epoch canceled)"
	default:
		return "discardReason(?)"
	}
}

// delivery owns the bounded event channel and enforces the classed reserve.
type delivery struct {
	ch       chan Event
	clock    Clock
	counters *counters

	queueSize     int
	dataCeiling   int // QueueSize - AdvisoryHeadroom: data and unbounded advisories stop here
	safetyCeiling int // QueueSize - 1: the safety subset may fill all but the *Terminal slot

	mu       sync.Mutex
	reserved int // sends admitted but not yet placed in ch, so a concurrent admission counts them
	parked   int // how many sends are currently parked on back-pressure
	// episodeStart is when the current back-pressure episode began (the 0→1
	// parked transition), for Blocked accounting. An episode spans from the
	// first sender parking to the last one leaving, so a second sender admitting
	// mid-episode neither ends it nor banks time it did not spend parked.
	episodeStart time.Time
	// blockWaiters are test rendezvous channels released when a send parks. The
	// harness needs a positive observable that a send blocked, because proving a
	// negative ("the send did not complete") otherwise needs a wall-clock wait.
	blockWaiters []chan struct{}
}

// newDelivery builds a delivery over a channel of the given capacity.
func newDelivery(queueSize int, clock Clock, c *counters) *delivery {
	return &delivery{
		ch:            make(chan Event, queueSize),
		clock:         clock,
		counters:      c,
		queueSize:     queueSize,
		dataCeiling:   queueSize - AdvisoryHeadroom,
		safetyCeiling: queueSize - 1,
	}
}

// messages returns the receive end for the caller.
func (d *delivery) messages() <-chan Event { return d.ch }

// close closes the delivery channel. It is called exactly once, by the
// supervisor's terminal sequence, after every sender has exited — so no send can
// race the close.
func (d *delivery) close() { close(d.ch) }

// safetySubset are the reserve-eligible event types (FR-001a). Each member's
// per-epoch volume is bounded by SDK-owned state, not by caller or server
// volume, which is what makes 15 slots ≥2× the worst-case demand.
//
// The classification is by construction — a pure function of the Go type — so it
// cannot drift from the frame's content. It must stay in sync with FR-001a's
// reserve membership; the delivery tests pin the discriminating cases.
func isSafetyEvent(ev Event) bool {
	switch ev.(type) {
	case *CloseError, *StateChange, *RecoveryInterruptedError,
		*HistoryComplete, *ReplayComplete, *PossibleGap, *Gap:
		return true
	default:
		return false
	}
}

// send places an event on the channel, blocking under back-pressure until room
// appears under the event's class ceiling or a context cancels.
//
// *Terminal must NOT be sent through here — it has its own reserved slot and
// non-blocking path (sendTerminal); routed here it would be classed as data and
// could be discarded, defeating FR-009's guarantee.
func (d *delivery) send(rootCtx, epochCtx context.Context, ev Event) discardReason {
	ceiling := d.dataCeiling
	if isSafetyEvent(ev) {
		ceiling = d.safetyCeiling
	}

	// parkedThisSend tracks whether THIS send has parked, so it touches the
	// episode counters exactly once (on its own park and its own departure) and
	// never on behalf of another sender.
	parkedThisSend := false

	for {
		d.mu.Lock()
		// Under the lock len(ch) can only shrink (a consumer receive) and never
		// grow (all senders serialize here), so this check-then-reserve cannot
		// over-admit. reserved counts admissions not yet in the channel.
		if len(d.ch)+d.reserved < ceiling {
			d.reserved++
			if parkedThisSend {
				d.leaveParkedLocked() // this send's own park ends
			}
			d.mu.Unlock()

			// Provably non-blocking: len+reserved was < ceiling ≤ QueueSize-1 <
			// cap, so a slot is guaranteed. The send is outside the lock (§VII).
			d.ch <- ev

			d.mu.Lock()
			d.reserved--
			d.mu.Unlock()
			return notDiscarded
		}
		if !parkedThisSend {
			d.enterParkedLocked()
			parkedThisSend = true
		}
		d.mu.Unlock()

		// No channel primitive wakes a sender on drain, so re-probe on a timer.
		// The two contexts discard the parked event with distinguishable reasons.
		timer := d.clock.NewTimer(deliveryProbeInterval, purposeDeliveryProbe)
		select {
		case <-timer.C():
			// Room may have appeared; loop and re-check.
		case <-rootCtx.Done():
			timer.Stop()
			d.leaveParked()
			return discardedRoot
		case <-epochCtx.Done():
			timer.Stop()
			d.leaveParked()
			return discardedEpoch
		}
	}
}

// sendTerminal places the final *Terminal into its reserved slot with a
// non-blocking send.
//
// It is called by the supervisor after every other sender has exited, so the
// ceilings guarantee len(ch) ≤ QueueSize-1 and the slot is free — the send lands
// structurally, not by luck. The send is non-blocking regardless: Close must
// never hang, so if the invariant were ever violated by a bug the default keeps
// Close moving rather than deadlocking, and the returned bool lets the caller
// (and the test) see that it landed.
func (d *delivery) sendTerminal(t *Terminal) bool {
	select {
	case d.ch <- t:
		return true
	default:
		return false
	}
}

// trySend places a safety-class event into the reserve with a NON-BLOCKING send —
// the sendTerminal shape, but admitting under the safety ceiling rather than the
// single Terminal slot. It reports whether the event landed. It exists for drained
// data-loss signals (*PossibleGap): a parking send there could strand the caller
// holding an already-cleared snapshot, so the snapshot is cleared only when this
// returns true, and the reserve argument (safety events are a bounded subset with ≥2×
// headroom) makes a false return all but unreachable outside genuine reserve
// exhaustion. Admission is under the same lock as send, so it cannot over-admit past
// the reserved Terminal slot.
func (d *delivery) trySend(ev Event) bool {
	d.mu.Lock()
	if len(d.ch)+d.reserved < d.safetyCeiling {
		d.reserved++
		d.mu.Unlock()

		d.ch <- ev // provably non-blocking: len+reserved < safetyCeiling ≤ QueueSize-1 < cap

		d.mu.Lock()
		d.reserved--
		d.mu.Unlock()
		return true
	}
	d.mu.Unlock()
	return false
}

// isParked reports whether any send is currently parked on back-pressure. The
// heartbeat's pong-deadline path reads it to suspend the deadline while the
// decode loop is stalled, so the SDK never preempts the server's own slow-client
// (1008) verdict.
func (d *delivery) isParked() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.parked > 0
}

// parkEpisodes returns the count of back-pressure episodes begun so far (a 0→1
// parked transition). The heartbeat samples it when it arms a pong deadline and
// again when the deadline fires: a change means the decode loop was parked at
// some point during the window (so a missed pong is the slow consumer's doing,
// not a dead link), which point-sampling isParked() at fire-time alone would miss
// while a backlog drains.
func (d *delivery) parkEpisodes() int64 {
	return d.counters.backpressureBlocks.Load()
}

// enterParkedLocked records that a send has parked. The back-pressure episode
// begins on the 0→1 parked transition — a second sender parking during an open
// episode joins it rather than starting a new one — and any test waiter is
// released, since a send is now demonstrably blocked.
func (d *delivery) enterParkedLocked() {
	if d.parked == 0 {
		d.episodeStart = d.clock.Now()
		d.counters.backpressureBlocks.Add(1)
	}
	d.parked++
	for _, w := range d.blockWaiters {
		close(w)
	}
	d.blockWaiters = nil
}

// leaveParkedLocked records that a parked send has left (admitted or discarded).
// The episode ends, and its wall-clock duration is banked, only on the 1→0
// transition — so the duration reflects the whole span from the first sender
// parking to the last one leaving, counted once.
func (d *delivery) leaveParkedLocked() {
	d.parked--
	if d.parked == 0 {
		d.counters.addBlocked(d.clock.Now().Sub(d.episodeStart))
	}
}

// leaveParked is leaveParkedLocked with its own locking, for the discard paths.
func (d *delivery) leaveParked() {
	d.mu.Lock()
	d.leaveParkedLocked()
	d.mu.Unlock()
}

// waitUntilBlocked blocks until a send is parked on back-pressure. It is a
// test-only rendezvous — the positive observable that lets a test assert a send
// did not complete without a wall-clock wait. It returns immediately if a send
// is already parked.
func (d *delivery) waitUntilBlocked() {
	d.mu.Lock()
	if d.parked > 0 {
		d.mu.Unlock()
		return
	}
	w := make(chan struct{})
	d.blockWaiters = append(d.blockWaiters, w)
	d.mu.Unlock()

	<-w
}
