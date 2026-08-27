package sukko

import (
	"testing"
	"time"
)

// The recovery FSM is a pure action machine (the sukko-py RecoveryEngine shape,
// §XVIII): handle* methods mutate the per-channel state and return the replay
// sends (and, on the deadline/reset/failure paths, the interrupted channels) the
// owner must act on, with no I/O of their own. That purity is what makes the
// epoch-gate, the arrival-order/floor/coalescing rules, and the recovery-deadline
// park-suspension table-testable here, rather than only through schedule-dependent
// fakeWS interleavings.
//
// `*epoch` values are used purely as identity tokens in these tests — the FSM
// compares them by pointer and never dereferences them, so a bare &epoch{} is a
// sufficient stand-in for "the epoch that produced this event".

var fsmBase = time.Unix(1_000_000, 0)

// newTestFSM builds an FSM with a 10s floor and a 1h deadline — far beyond any
// Slice-2 test's timescale, so the recovery deadline never interferes with the
// gap/floor/coalescing/re-drive assertions. The deadline-specific tests below
// construct their own FSM with a short deadline.
func newTestFSM() *recoveryFSM { return newRecoveryFSM(10*time.Second, time.Hour) }

// tickAt builds a tick at `now` with `current` as the live epoch and no
// back-pressure (episodes 0, not parked) — the default for tests that do not
// exercise the deadline's park-suspension.
func tickAt(now time.Time, current *epoch) tick {
	return tick{now: now, current: current}
}

// TestRecoveryFSMGapReplaysImmediatelyWhenFloorOpen: a gap on an idle channel
// whose floor is open (no prior replay) drives exactly one replay from the gap's
// own last_pos, on the current epoch, and moves the channel to REPLAYING.
func TestRecoveryFSMGapReplaysImmediatelyWhenFloorOpen(t *testing.T) {
	f := newTestFSM()
	e := &epoch{}

	acts := f.handleGap("t.a", "pos-1", e, tickAt(fsmBase, e))

	if len(acts) != 1 {
		t.Fatalf("handleGap actions = %d, want 1", len(acts))
	}
	if acts[0].channel != "t.a" || acts[0].fromPos != "pos-1" || acts[0].epoch != e {
		t.Errorf("action = %+v, want {t.a, pos-1, e}", acts[0])
	}
	if rec := f.channels["t.a"]; rec == nil || rec.phase != recReplaying {
		t.Errorf("phase = %v, want recReplaying", f.channels["t.a"])
	}
}

// TestRecoveryFSMGapWaitsWhenFloorClosed: a gap arriving within the floor of a
// prior replay does NOT send immediately — it enters FLOOR_WAIT — and due() fires
// exactly one replay once the floor elapses, from the gap's anchor (T128/T137).
func TestRecoveryFSMGapWaitsWhenFloorClosed(t *testing.T) {
	f := newTestFSM()
	e := &epoch{}

	f.handleGap("t.a", "pos-1", e, tickAt(fsmBase, e))
	f.handleReplayComplete("t.a", e, tickAt(fsmBase, e))

	within := fsmBase.Add(4 * time.Second)
	if acts := f.handleGap("t.a", "pos-2", e, tickAt(within, e)); len(acts) != 0 {
		t.Fatalf("gap within floor produced %d actions, want 0 (FLOOR_WAIT)", len(acts))
	}
	if wake, purpose, ok := f.nextWake(); !ok || purpose != purposeReplayFloor || !wake.Equal(fsmBase.Add(10*time.Second)) {
		t.Errorf("nextWake = %v/%v (ok=%v), want %v/replay_floor", wake, purpose, ok, fsmBase.Add(10*time.Second))
	}

	if out := f.due(tickAt(fsmBase.Add(9*time.Second), e)); len(out.replays) != 0 {
		t.Fatalf("due() before floor produced %d replays, want 0", len(out.replays))
	}
	out := f.due(tickAt(fsmBase.Add(10*time.Second), e))
	if len(out.replays) != 1 || out.replays[0].fromPos != "pos-2" {
		t.Fatalf("due() at floor = %+v, want one replay from pos-2", out.replays)
	}
}

// TestRecoveryFSMCoalescesInFloorWait: multiple gaps arriving during FLOOR_WAIT
// coalesce to ONE replay from the FIRST-arrived anchor (T127).
func TestRecoveryFSMCoalescesInFloorWait(t *testing.T) {
	f := newTestFSM()
	e := &epoch{}
	f.handleGap("t.a", "pos-1", e, tickAt(fsmBase, e))
	f.handleReplayComplete("t.a", e, tickAt(fsmBase, e))

	f.handleGap("t.a", "first", e, tickAt(fsmBase.Add(1*time.Second), e))
	f.handleGap("t.a", "second", e, tickAt(fsmBase.Add(2*time.Second), e))
	f.handleGap("t.a", "third", e, tickAt(fsmBase.Add(3*time.Second), e))

	out := f.due(tickAt(fsmBase.Add(10*time.Second), e))
	if len(out.replays) != 1 || out.replays[0].fromPos != "first" {
		t.Fatalf("coalesced replay = %+v, want ONE from 'first' (first-arrived anchor)", out.replays)
	}
}

// TestRecoveryFSMCoalescingIgnoresPosOrder: gaps whose anchor values' order
// disagrees with arrival order still coalesce to the first-ARRIVED anchor, proving
// no pos comparison is performed (T141).
func TestRecoveryFSMCoalescingIgnoresPosOrder(t *testing.T) {
	f := newTestFSM()
	e := &epoch{}
	f.handleGap("t.a", "pos-1", e, tickAt(fsmBase, e))
	f.handleReplayComplete("t.a", e, tickAt(fsmBase, e))

	f.handleGap("t.a", "9-999", e, tickAt(fsmBase.Add(1*time.Second), e))
	f.handleGap("t.a", "9-1", e, tickAt(fsmBase.Add(2*time.Second), e))

	out := f.due(tickAt(fsmBase.Add(10*time.Second), e))
	if len(out.replays) != 1 || out.replays[0].fromPos != "9-999" {
		t.Fatalf("replay = %+v, want from '9-999' (first-arrived, not the lexically-earlier '9-1')", out.replays)
	}
}

// TestRecoveryFSMFollowupAfterReplayComplete: a gap arriving mid-replay is
// retained as the follow-up anchor and drives one replay after replay_complete
// (floor permitting) (T137).
func TestRecoveryFSMFollowupAfterReplayComplete(t *testing.T) {
	f := newTestFSM()
	e := &epoch{}
	f.handleGap("t.a", "head", e, tickAt(fsmBase, e))

	if acts := f.handleGap("t.a", "tail", e, tickAt(fsmBase.Add(1*time.Second), e)); len(acts) != 0 {
		t.Fatalf("gap mid-replay produced %d actions, want 0", len(acts))
	}

	acts := f.handleReplayComplete("t.a", e, tickAt(fsmBase.Add(11*time.Second), e))
	if len(acts) != 1 || acts[0].fromPos != "tail" {
		t.Fatalf("follow-up replay = %+v, want one from 'tail'", acts)
	}
}

// TestRecoveryFSMReplayCompleteNoFollowupIdles: replay_complete with no retained
// gap returns the channel to idle.
func TestRecoveryFSMReplayCompleteNoFollowupIdles(t *testing.T) {
	f := newTestFSM()
	e := &epoch{}
	f.handleGap("t.a", "head", e, tickAt(fsmBase, e))
	if acts := f.handleReplayComplete("t.a", e, tickAt(fsmBase.Add(1*time.Second), e)); len(acts) != 0 {
		t.Fatalf("replay_complete produced %d actions, want 0", len(acts))
	}
	if rec := f.channels["t.a"]; rec.phase != recIdle {
		t.Errorf("phase = %v, want recIdle", rec.phase)
	}
}

// TestRecoveryFSMStaleReplayCompleteIgnored: a replay_complete for a channel that
// is not REPLAYING — never seen, or already idle after a prior complete — is a
// stale/duplicate terminator and must not fabricate or mutate state.
func TestRecoveryFSMStaleReplayCompleteIgnored(t *testing.T) {
	f := newTestFSM()
	e := &epoch{}

	if acts := f.handleReplayComplete("t.a", e, tickAt(fsmBase, e)); len(acts) != 0 {
		t.Fatalf("replay_complete for an unknown channel = %d actions, want 0", len(acts))
	}
	f.handleGap("t.a", "g-1", e, tickAt(fsmBase, e))
	f.handleReplayComplete("t.a", e, tickAt(fsmBase, e)) // → idle
	if acts := f.handleReplayComplete("t.a", e, tickAt(fsmBase, e)); len(acts) != 0 {
		t.Fatalf("duplicate replay_complete = %d actions, want 0", len(acts))
	}
	if rec := f.channels["t.a"]; rec.phase != recIdle {
		t.Errorf("phase = %v after a duplicate complete, want recIdle (unchanged)", rec.phase)
	}
}

// TestRecoveryFSMRedriveOnGrantAfterEpochDeath: a replay in flight when the epoch
// dies is retained (anchor kept, awaiting grant) and re-driven from that SAME
// anchor when the channel is re-granted on the new epoch — never from the cursor
// (T130/T142). The re-drive is grant-triggered, not reset-triggered.
func TestRecoveryFSMRedriveOnGrantAfterEpochDeath(t *testing.T) {
	f := newTestFSM()
	e1 := &epoch{}
	f.handleGap("t.a", "anchor-1", e1, tickAt(fsmBase, e1)) // → REPLAYING on e1

	f.handleReset(tickAt(fsmBase, nil))
	rec := f.channels["t.a"]
	if rec.phase != recAwaitingGrant || rec.anchor != "anchor-1" {
		t.Fatalf("after reset: phase=%v anchor=%q, want recAwaitingGrant / anchor-1", rec.phase, rec.anchor)
	}
	// The reset alone must NOT re-drive (no floor timer arms a replay); re-drive is
	// grant-triggered. The unwedge deadline IS armed, but it fires an interrupt, not
	// a replay — so the pending wake must not be a floor wake.
	if _, purpose, ok := f.nextWake(); ok && purpose == purposeReplayFloor {
		t.Error("a floor timer is armed after reset; re-drive must be grant-triggered, not floored")
	}

	e2 := &epoch{}
	acts := f.handleGrant([]string{"t.a"}, e2, tickAt(fsmBase.Add(1*time.Second), e2))
	if len(acts) != 1 || acts[0].fromPos != "anchor-1" || acts[0].epoch != e2 {
		t.Fatalf("re-drive = %+v, want one from anchor-1 on e2", acts)
	}
}

// TestRecoveryFSMFloorResetsWithEpoch: the per-channel floor is reset by an epoch
// boundary, so a re-drive on the new epoch is not throttled by the old epoch's
// replay time (T128/T142).
func TestRecoveryFSMFloorResetsWithEpoch(t *testing.T) {
	f := newTestFSM()
	e1 := &epoch{}
	f.handleGap("t.a", "anchor-1", e1, tickAt(fsmBase, e1)) // replay at fsmBase
	f.handleReplayComplete("t.a", e1, tickAt(fsmBase, e1))  // → idle, lastReplayAt still fsmBase

	f.handleReset(tickAt(fsmBase, nil)) // floor reset → lastReplayAt zeroed

	// A fresh gap on the new epoch only 1s later: routed through handleGap's floor
	// check. Without the floor reset this is inside the 10s floor → FLOOR_WAIT (no
	// send); with it the floor is open → immediate replay.
	e2 := &epoch{}
	acts := f.handleGap("t.a", "anchor-2", e2, tickAt(fsmBase.Add(1*time.Second), e2))
	if len(acts) != 1 {
		t.Fatalf("gap 1s after the prior replay produced %d actions, want 1 (floor reset with the epoch)", len(acts))
	}
}

// TestRecoveryFSMFollowupCollapsesOnReset: a REPLAYING channel holding a follow-up
// gap when the epoch dies re-drives ONLY the head anchor on the next grant — the
// follow-up collapses into it (one frame per channel), since replaying from the
// earlier anchor covers the later gap (T130).
func TestRecoveryFSMFollowupCollapsesOnReset(t *testing.T) {
	f := newTestFSM()
	e1 := &epoch{}
	f.handleGap("t.a", "head", e1, tickAt(fsmBase, e1))
	f.handleGap("t.a", "tail", e1, tickAt(fsmBase.Add(1*time.Second), e1)) // retained follow-up

	f.handleReset(tickAt(fsmBase.Add(1*time.Second), nil))
	rec := f.channels["t.a"]
	if rec.hasFollowup {
		t.Error("follow-up survived the reset; it must collapse into the head anchor")
	}

	e2 := &epoch{}
	acts := f.handleGrant([]string{"t.a"}, e2, tickAt(fsmBase.Add(2*time.Second), e2))
	if len(acts) != 1 || acts[0].fromPos != "head" {
		t.Fatalf("re-drive = %+v, want a single replay from 'head'", acts)
	}
}

// TestRecoveryFSMGapRedrivesAwaitingGrant: a gap arriving on the NEW live epoch
// for a channel still awaiting a re-grant proves the channel is subscribed there,
// so the retained replay is re-driven immediately rather than stalling for a grant
// event a dropped resume ack might never deliver. It re-drives from the RETAINED
// anchor (conservative — covers the new gap too), and the later grant is a no-op.
func TestRecoveryFSMGapRedrivesAwaitingGrant(t *testing.T) {
	f := newTestFSM()
	e1 := &epoch{}
	f.handleGap("t.a", "anchor-1", e1, tickAt(fsmBase, e1)) // → REPLAYING on e1
	f.handleReset(tickAt(fsmBase, nil))                     // → awaitingGrant, anchor-1 retained

	e2 := &epoch{}
	acts := f.handleGap("t.a", "anchor-2", e2, tickAt(fsmBase.Add(1*time.Second), e2))
	if len(acts) != 1 || acts[0].fromPos != "anchor-1" || acts[0].epoch != e2 {
		t.Fatalf("awaitingGrant gap = %+v, want one re-drive from anchor-1 (retained) on e2", acts)
	}
	if got := f.handleGrant([]string{"t.a"}, e2, tickAt(fsmBase.Add(2*time.Second), e2)); len(got) != 0 {
		t.Errorf("grant after gap re-drive = %d actions, want 0 (already replaying)", len(got))
	}
}

// ─── epoch-gate (the advisor's W2 fix: an epoch-scoped event must never send on
// another epoch's socket) ───

// TestRecoveryFSMStaleGapDoesNotSend: a gap whose epoch is no longer current is
// NOT replayed on the live epoch (the server would reject a replay for a channel
// not yet re-subscribed there); its anchor is parked for a grant-triggered
// re-drive.
func TestRecoveryFSMStaleGapDoesNotSend(t *testing.T) {
	f := newTestFSM()
	dead, live := &epoch{}, &epoch{}

	acts := f.handleGap("t.a", "anchor-1", dead, tickAt(fsmBase, live)) // current=live, evEpoch=dead
	if len(acts) != 0 {
		t.Fatalf("stale gap produced %d actions, want 0 (no send on a foreign epoch)", len(acts))
	}
	rec := f.channels["t.a"]
	if rec.phase != recAwaitingGrant || rec.anchor != "anchor-1" {
		t.Errorf("stale gap: phase=%v anchor=%q, want recAwaitingGrant / anchor-1", rec.phase, rec.anchor)
	}
	if got := f.handleGrant([]string{"t.a"}, live, tickAt(fsmBase.Add(1*time.Second), live)); len(got) != 1 || got[0].fromPos != "anchor-1" {
		t.Errorf("grant re-drive = %+v, want one from anchor-1", got)
	}
}

// TestRecoveryFSMStaleGrantDropped: a grant that belongs to a superseded epoch is
// dropped — the live epoch's own resume grant re-triggers the re-drive, so nothing
// is stranded.
func TestRecoveryFSMStaleGrantDropped(t *testing.T) {
	f := newTestFSM()
	e1, live := &epoch{}, &epoch{}
	f.handleGap("t.a", "anchor-1", e1, tickAt(fsmBase, e1))
	f.handleReset(tickAt(fsmBase, nil)) // → awaitingGrant

	stale := &epoch{}
	if acts := f.handleGrant([]string{"t.a"}, stale, tickAt(fsmBase.Add(1*time.Second), live)); len(acts) != 0 {
		t.Fatalf("stale grant produced %d actions, want 0", len(acts))
	}
	if rec := f.channels["t.a"]; rec.phase != recAwaitingGrant {
		t.Errorf("phase = %v after a stale grant, want recAwaitingGrant (still pending)", rec.phase)
	}
}

// TestRecoveryFSMDueForeignEpochParks: a FLOOR_WAIT channel whose epoch died
// before the floor timer fired is parked for re-grant, not sent on the live epoch.
func TestRecoveryFSMDueForeignEpochParks(t *testing.T) {
	f := newTestFSM()
	e1 := &epoch{}
	f.handleGap("t.a", "pos-1", e1, tickAt(fsmBase, e1))
	f.handleReplayComplete("t.a", e1, tickAt(fsmBase, e1))
	f.handleGap("t.a", "pos-2", e1, tickAt(fsmBase.Add(1*time.Second), e1)) // → FLOOR_WAIT on e1

	live := &epoch{}
	if out := f.due(tickAt(fsmBase.Add(10*time.Second), live)); len(out.replays) != 0 {
		t.Fatalf("due() on a foreign epoch produced %d replays, want 0", len(out.replays))
	}
	if rec := f.channels["t.a"]; rec.phase != recAwaitingGrant {
		t.Errorf("phase = %v, want recAwaitingGrant", rec.phase)
	}
}

// ─── Slice 3a: recovery deadline + *RecoveryInterruptedError (replay half) ───
//
// These tests use an explicit (floor, deadline) so the deadline fires within the
// test's timescale — unlike newTestFSM, whose 1h deadline never interferes.

// TestRecoveryFSMDeadlineInterruptsStuckReplay: an in-flight replay whose
// replay_complete never arrives is declared interrupted once the recovery deadline
// elapses, and the channel is reset to idle with its anchor dropped (T129).
func TestRecoveryFSMDeadlineInterruptsStuckReplay(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	f.handleGap("t.a", "g-1", e, tickAt(fsmBase, e)) // → REPLAYING, deadline = fsmBase+30s

	if out := f.due(tickAt(fsmBase.Add(29*time.Second), e)); len(out.interrupts) != 0 {
		t.Fatalf("due before the deadline = %d interrupts, want 0", len(out.interrupts))
	}
	out := f.due(tickAt(fsmBase.Add(30*time.Second), e))
	if len(out.interrupts) != 1 || out.interrupts[0] != "t.a" {
		t.Fatalf("due at the deadline = %+v, want an interrupt for t.a", out.interrupts)
	}
	if rec := f.channels["t.a"]; rec.phase != recIdle || rec.anchor != "" {
		t.Errorf("after the deadline: phase=%v anchor=%q, want recIdle / empty (anchor dropped)", rec.phase, rec.anchor)
	}
}

// TestRecoveryFSMDeadlineSuspendedWhileConsumerStalled: the deadline measures
// SERVER silence, not consumer speed — so a back-pressure episode during the window
// (parked at fire time, OR the episode count changed since arming) suspends the
// deadline (re-arm, no fire), mirroring the heartbeat's pong-deadline suspension.
// A clean window then fires it. Both sides.
func TestRecoveryFSMDeadlineSuspendedWhileConsumerStalled(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	f.handleGap("t.a", "g-1", e, tickAt(fsmBase, e)) // REPLAYING, armEpisodes=0, deadline=+30s

	// Parked at fire time → suspend, re-arm to +60s. No interrupt.
	parked := tick{now: fsmBase.Add(30 * time.Second), current: e, episodes: 0, parked: true}
	if out := f.due(parked); len(out.interrupts) != 0 {
		t.Fatalf("due while parked = %d interrupts, want 0 (suspended)", len(out.interrupts))
	}
	// Not parked now, but a park episode opened during the window (count 0→1) → suspend
	// again, re-arm to +90s, armEpisodes→1.
	bumped := tick{now: fsmBase.Add(60 * time.Second), current: e, episodes: 1, parked: false}
	if out := f.due(bumped); len(out.interrupts) != 0 {
		t.Fatalf("due after a park episode = %d interrupts, want 0 (suspended)", len(out.interrupts))
	}
	// A clean window (not parked, episode count unchanged from the arm) → interrupt.
	clean := tick{now: fsmBase.Add(90 * time.Second), current: e, episodes: 1, parked: false}
	if out := f.due(clean); len(out.interrupts) != 1 {
		t.Fatalf("due after a clean window = %+v, want 1 interrupt", out.interrupts)
	}
}

// TestRecoveryFSMReplayFailureInterruptsExactlyOnce: a server replay rejection
// interrupts the in-flight replay and drops the anchor; the epoch's subsequent
// reset must NOT re-interrupt (the channel is already idle) — exactly one interrupt.
func TestRecoveryFSMReplayFailureInterruptsExactlyOnce(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	f.handleGap("t.a", "g-1", e, tickAt(fsmBase, e)) // REPLAYING

	if got := f.handleReplayFailure("t.a"); len(got) != 1 || got[0] != "t.a" {
		t.Fatalf("handleReplayFailure = %+v, want [t.a]", got)
	}
	if rec := f.channels["t.a"]; rec.phase != recIdle || rec.anchor != "" {
		t.Errorf("after failure: phase=%v anchor=%q, want recIdle / empty (anchor dropped)", rec.phase, rec.anchor)
	}
	if got := f.handleReset(tickAt(fsmBase, nil)); len(got) != 0 {
		t.Errorf("reset after a failure = %+v interrupts, want 0 (exactly-once: already idle)", got)
	}
	if got := f.handleReplayFailure("t.a"); got != nil {
		t.Errorf("handleReplayFailure on an idle channel = %+v, want nil", got)
	}
}

// TestRecoveryFSMResetInterruptsOnlyInFlightReplays: an epoch boundary interrupts a
// channel with a replay IN FLIGHT (REPLAYING), but NOT one merely floor-waiting
// (nothing in flight) — while both retain their anchors and re-drive on the next
// grant (interrupt-and-re-drive, spec §III + T130).
func TestRecoveryFSMResetInterruptsOnlyInFlightReplays(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	f.handleGap("t.a", "a-1", e, tickAt(fsmBase, e)) // t.a → REPLAYING (in flight)
	// t.b → FLOOR_WAIT: replay once, complete, then a gap inside the floor.
	f.handleGap("t.b", "b-0", e, tickAt(fsmBase, e))
	f.handleReplayComplete("t.b", e, tickAt(fsmBase, e))
	f.handleGap("t.b", "b-1", e, tickAt(fsmBase.Add(1*time.Second), e)) // FLOOR_WAIT

	interrupts := f.handleReset(tickAt(fsmBase.Add(2*time.Second), nil))
	if len(interrupts) != 1 || interrupts[0] != "t.a" {
		t.Fatalf("reset interrupts = %+v, want only [t.a] (t.b was floor-waiting, nothing in flight)", interrupts)
	}
	e2 := &epoch{}
	acts := f.handleGrant([]string{"t.a", "t.b"}, e2, tickAt(fsmBase.Add(3*time.Second), e2))
	if len(acts) != 2 {
		t.Errorf("re-drive after reset = %d replays, want 2 (both channels retained their anchors)", len(acts))
	}
}

// TestRecoveryFSMAwaitingGrantDeadlineInterrupts: the Slice-2→3 unwedge — a channel
// left awaiting a re-grant whose grant never arrives is declared interrupted once
// its (epoch-armed) deadline elapses, rather than stalling forever (Fable Finding-1).
func TestRecoveryFSMAwaitingGrantDeadlineInterrupts(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	live := &epoch{}
	f.handleGap("t.a", "a-1", e, tickAt(fsmBase, e))        // REPLAYING on e
	f.handleReset(tickAt(fsmBase.Add(1*time.Second), live)) // → awaitingGrant, armEpoch=live, deadline +31s

	// The re-grant never comes on `live`; past the deadline under the SAME epoch it is
	// interrupted (a full window ran on a live connection with no grant).
	out := f.due(tickAt(fsmBase.Add(31*time.Second), live))
	if len(out.interrupts) != 1 || out.interrupts[0] != "t.a" {
		t.Fatalf("awaiting-grant deadline = %+v, want an interrupt for t.a", out.interrupts)
	}
	if rec := f.channels["t.a"]; rec.phase != recIdle {
		t.Errorf("phase = %v after the awaiting-grant deadline, want recIdle", rec.phase)
	}
}

// TestRecoveryFSMDeadlineSuspendedWhileDisconnected: the recovery deadline measures
// CONNECTED time. A channel awaiting a re-grant while the client is DISCONNECTED
// (no live epoch, current==nil during backoff/dial) must NOT be interrupted — the
// wall-clock spent reconnecting is not the server failing to deliver, and dropping
// the anchor before the new epoch can re-drive it would silently lose the gapped
// window (the core outage scenario). The deadline suspends until an epoch is live
// and the grant genuinely never arrives.
func TestRecoveryFSMDeadlineSuspendedWhileDisconnected(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	f.handleGap("t.a", "a-1", e, tickAt(fsmBase, e))       // REPLAYING on e
	f.handleReset(tickAt(fsmBase.Add(1*time.Second), nil)) // → awaitingGrant, armed disconnected, deadline +31s

	// Past the deadline but still disconnected (current==nil): suspend, re-arm to +61s.
	if out := f.due(tick{now: fsmBase.Add(31 * time.Second), current: nil}); len(out.interrupts) != 0 {
		t.Fatalf("due while disconnected = %d interrupts, want 0 (backoff time is not a recovery failure)", len(out.interrupts))
	}
	// An epoch is now live, but the deadline was armed while disconnected (armEpoch !=
	// this epoch): the first fire under it suspends once, so a FULL window runs under
	// the live epoch — giving the resume grant time to arrive — re-arming to +91s.
	live := &epoch{}
	if out := f.due(tick{now: fsmBase.Add(61 * time.Second), current: live}); len(out.interrupts) != 0 {
		t.Fatalf("first fire under a freshly-live epoch = %d interrupts, want 0 (a full window must run under it)", len(out.interrupts))
	}
	// A full window later, still on the same epoch and still no grant → interrupt.
	if out := f.due(tick{now: fsmBase.Add(91 * time.Second), current: live}); len(out.interrupts) != 1 {
		t.Fatalf("fire after a full window under the live epoch = %+v, want 1 interrupt", out.interrupts)
	}
}

// TestRecoveryFSMResetReArmsAwaitingGrantDeadline: a second epoch death while a
// channel is already awaiting a re-grant re-arms its deadline, so each new epoch
// gets a full window rather than inheriting a nearly-expired one. (The reset is
// processed while the successor epoch is already live — the fast-reconnect case —
// so armEpoch matches the epoch the due() fires under.)
func TestRecoveryFSMResetReArmsAwaitingGrantDeadline(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	live := &epoch{}
	f.handleGap("t.a", "a-1", e, tickAt(fsmBase, e))         // REPLAYING on e
	f.handleReset(tickAt(fsmBase.Add(1*time.Second), live))  // awaitingGrant, armEpoch=live, deadline +31s
	f.handleReset(tickAt(fsmBase.Add(21*time.Second), live)) // 2nd death → re-arm to +51s, armEpoch=live

	if out := f.due(tickAt(fsmBase.Add(31*time.Second), live)); len(out.interrupts) != 0 {
		t.Fatalf("due at the pre-re-arm deadline = %d interrupts, want 0 (re-armed by the 2nd reset)", len(out.interrupts))
	}
	if out := f.due(tickAt(fsmBase.Add(51*time.Second), live)); len(out.interrupts) != 1 {
		t.Fatalf("due at the re-armed deadline = %+v, want 1 interrupt", out.interrupts)
	}
}

// TestRecoveryFSMCompletedReplayDoesNotInterrupt: replay_complete clears the
// deadline, so a channel that completed its replay is never falsely interrupted
// long after — the discriminating guard for the deadline-clear on completion.
func TestRecoveryFSMCompletedReplayDoesNotInterrupt(t *testing.T) {
	f := newRecoveryFSM(10*time.Second, 30*time.Second)
	e := &epoch{}
	f.handleGap("t.a", "g-1", e, tickAt(fsmBase, e))                        // REPLAYING, deadline=+30s
	f.handleReplayComplete("t.a", e, tickAt(fsmBase.Add(1*time.Second), e)) // → idle, deadline cleared

	if out := f.due(tickAt(fsmBase.Add(60*time.Second), e)); len(out.interrupts) != 0 {
		t.Fatalf("due long after completion = %+v interrupts, want 0 (deadline cleared on complete)", out.interrupts)
	}
}
