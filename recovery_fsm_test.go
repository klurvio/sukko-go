package sukko

import (
	"testing"
	"time"
)

// The recovery FSM is a pure action machine (the sukko-py RecoveryEngine shape,
// §XVIII): handle* methods mutate the per-channel state and return the replay
// sends the owner must perform, with no I/O of their own. That purity is what
// makes the epoch-gate and the arrival-order/floor/coalescing rules table-
// testable here, rather than only through schedule-dependent fakeWS interleavings.
//
// `*epoch` values are used purely as identity tokens in these tests — the FSM
// compares them by pointer and never dereferences them, so a bare &epoch{} is a
// sufficient stand-in for "the epoch that produced this event".

var fsmBase = time.Unix(1_000_000, 0)

// TestRecoveryFSMGapReplaysImmediatelyWhenFloorOpen: a gap on an idle channel
// whose floor is open (no prior replay) drives exactly one replay from the gap's
// own last_pos, on the current epoch, and moves the channel to REPLAYING.
func TestRecoveryFSMGapReplaysImmediatelyWhenFloorOpen(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e := &epoch{}

	acts := f.handleGap("t.a", "pos-1", fsmBase, e, e)

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
	f := newRecoveryFSM(10 * time.Second)
	e := &epoch{}

	// First gap replays immediately and stamps lastReplayAt = fsmBase.
	f.handleGap("t.a", "pos-1", fsmBase, e, e)
	// The replay completes, returning the channel to idle at fsmBase.
	f.handleReplayComplete("t.a", fsmBase, e, e)

	// A second gap 4s later is inside the 10s floor → FLOOR_WAIT, no send.
	within := fsmBase.Add(4 * time.Second)
	if acts := f.handleGap("t.a", "pos-2", within, e, e); len(acts) != 0 {
		t.Fatalf("gap within floor produced %d actions, want 0 (FLOOR_WAIT)", len(acts))
	}
	if wake, ok := f.nextFloorWake(); !ok || !wake.Equal(fsmBase.Add(10*time.Second)) {
		t.Errorf("nextFloorWake = %v (ok=%v), want %v", wake, ok, fsmBase.Add(10*time.Second))
	}

	// due() before the floor elapses fires nothing.
	if acts := f.due(fsmBase.Add(9*time.Second), e); len(acts) != 0 {
		t.Fatalf("due() before floor produced %d actions, want 0", len(acts))
	}
	// due() at the floor fires exactly one replay from the pending anchor.
	acts := f.due(fsmBase.Add(10*time.Second), e)
	if len(acts) != 1 || acts[0].fromPos != "pos-2" {
		t.Fatalf("due() at floor = %+v, want one replay from pos-2", acts)
	}
}

// TestRecoveryFSMCoalescesInFloorWait: multiple gaps arriving during FLOOR_WAIT
// coalesce to ONE replay from the FIRST-arrived anchor (T127).
func TestRecoveryFSMCoalescesInFloorWait(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e := &epoch{}
	f.handleGap("t.a", "pos-1", fsmBase, e, e)
	f.handleReplayComplete("t.a", fsmBase, e, e)

	// Three gaps inside the floor; all coalesce.
	f.handleGap("t.a", "first", fsmBase.Add(1*time.Second), e, e)
	f.handleGap("t.a", "second", fsmBase.Add(2*time.Second), e, e)
	f.handleGap("t.a", "third", fsmBase.Add(3*time.Second), e, e)

	acts := f.due(fsmBase.Add(10*time.Second), e)
	if len(acts) != 1 || acts[0].fromPos != "first" {
		t.Fatalf("coalesced replay = %+v, want ONE from 'first' (first-arrived anchor)", acts)
	}
}

// TestRecoveryFSMCoalescingIgnoresPosOrder: gaps whose anchor values' order
// disagrees with arrival order still coalesce to the first-ARRIVED anchor, proving
// no pos comparison is performed (T141).
func TestRecoveryFSMCoalescingIgnoresPosOrder(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e := &epoch{}
	f.handleGap("t.a", "pos-1", fsmBase, e, e)
	f.handleReplayComplete("t.a", fsmBase, e, e)

	// "9-999" arrives first though it sorts AFTER "9-1"; the anchor must be it.
	f.handleGap("t.a", "9-999", fsmBase.Add(1*time.Second), e, e)
	f.handleGap("t.a", "9-1", fsmBase.Add(2*time.Second), e, e)

	acts := f.due(fsmBase.Add(10*time.Second), e)
	if len(acts) != 1 || acts[0].fromPos != "9-999" {
		t.Fatalf("replay = %+v, want from '9-999' (first-arrived, not the lexically-earlier '9-1')", acts)
	}
}

// TestRecoveryFSMFollowupAfterReplayComplete: a gap arriving mid-replay is
// retained as the follow-up anchor and drives one replay after replay_complete
// (floor permitting) (T137).
func TestRecoveryFSMFollowupAfterReplayComplete(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e := &epoch{}
	f.handleGap("t.a", "head", fsmBase, e, e) // → REPLAYING from head

	// A gap while REPLAYING is retained, not sent.
	if acts := f.handleGap("t.a", "tail", fsmBase.Add(1*time.Second), e, e); len(acts) != 0 {
		t.Fatalf("gap mid-replay produced %d actions, want 0", len(acts))
	}

	// replay_complete 11s later: the floor (from fsmBase) is open → follow-up fires.
	acts := f.handleReplayComplete("t.a", fsmBase.Add(11*time.Second), e, e)
	if len(acts) != 1 || acts[0].fromPos != "tail" {
		t.Fatalf("follow-up replay = %+v, want one from 'tail'", acts)
	}
}

// TestRecoveryFSMReplayCompleteNoFollowupIdles: replay_complete with no retained
// gap returns the channel to idle.
func TestRecoveryFSMReplayCompleteNoFollowupIdles(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e := &epoch{}
	f.handleGap("t.a", "head", fsmBase, e, e)
	if acts := f.handleReplayComplete("t.a", fsmBase.Add(1*time.Second), e, e); len(acts) != 0 {
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
	f := newRecoveryFSM(10 * time.Second)
	e := &epoch{}

	if acts := f.handleReplayComplete("t.a", fsmBase, e, e); len(acts) != 0 {
		t.Fatalf("replay_complete for an unknown channel = %d actions, want 0", len(acts))
	}
	f.handleGap("t.a", "g-1", fsmBase, e, e)     // → REPLAYING
	f.handleReplayComplete("t.a", fsmBase, e, e) // → idle
	if acts := f.handleReplayComplete("t.a", fsmBase, e, e); len(acts) != 0 {
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
	f := newRecoveryFSM(10 * time.Second)
	e1 := &epoch{}
	f.handleGap("t.a", "anchor-1", fsmBase, e1, e1) // → REPLAYING on e1

	// Epoch dies: the channel retains its anchor, awaiting a re-grant.
	f.handleReset()
	rec := f.channels["t.a"]
	if rec.phase != recAwaitingGrant || rec.anchor != "anchor-1" {
		t.Fatalf("after reset: phase=%v anchor=%q, want recAwaitingGrant / anchor-1", rec.phase, rec.anchor)
	}
	// The reset alone must NOT re-drive.
	if _, ok := f.nextFloorWake(); ok {
		t.Error("a floor is armed after reset; re-drive must be grant-triggered, not floored")
	}

	// The new epoch re-grants the channel → exactly one re-driven replay from the
	// retained anchor, on the new epoch.
	e2 := &epoch{}
	acts := f.handleGrant([]string{"t.a"}, fsmBase.Add(1*time.Second), e2, e2)
	if len(acts) != 1 || acts[0].fromPos != "anchor-1" || acts[0].epoch != e2 {
		t.Fatalf("re-drive = %+v, want one from anchor-1 on e2", acts)
	}
}

// TestRecoveryFSMFloorResetsWithEpoch: the per-channel floor is reset by an epoch
// boundary, so a re-drive on the new epoch is not throttled by the old epoch's
// replay time (T128/T142).
func TestRecoveryFSMFloorResetsWithEpoch(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e1 := &epoch{}
	f.handleGap("t.a", "anchor-1", fsmBase, e1, e1) // replay at fsmBase, lastReplayAt = fsmBase
	f.handleReplayComplete("t.a", fsmBase, e1, e1)  // → idle, lastReplayAt still fsmBase

	f.handleReset() // floor reset → lastReplayAt zeroed

	// A fresh gap on the new epoch only 1s later: routed through handleGap's floor
	// check (unlike a grant re-drive, which is unconditional). Without the floor
	// reset this is inside the 10s floor → FLOOR_WAIT (no send); with it the floor
	// is open → immediate replay.
	e2 := &epoch{}
	acts := f.handleGap("t.a", "anchor-2", fsmBase.Add(1*time.Second), e2, e2)
	if len(acts) != 1 {
		t.Fatalf("gap 1s after the prior replay produced %d actions, want 1 (floor reset with the epoch)", len(acts))
	}
}

// TestRecoveryFSMFollowupCollapsesOnReset: a REPLAYING channel holding a follow-up
// gap when the epoch dies re-drives ONLY the head anchor on the next grant — the
// follow-up collapses into it (one frame per channel), since replaying from the
// earlier anchor covers the later gap (T130).
func TestRecoveryFSMFollowupCollapsesOnReset(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e1 := &epoch{}
	f.handleGap("t.a", "head", fsmBase, e1, e1)
	f.handleGap("t.a", "tail", fsmBase.Add(1*time.Second), e1, e1) // retained follow-up

	f.handleReset()
	rec := f.channels["t.a"]
	if rec.hasFollowup {
		t.Error("follow-up survived the reset; it must collapse into the head anchor")
	}

	e2 := &epoch{}
	acts := f.handleGrant([]string{"t.a"}, fsmBase.Add(2*time.Second), e2, e2)
	if len(acts) != 1 || acts[0].fromPos != "head" {
		t.Fatalf("re-drive = %+v, want a single replay from 'head'", acts)
	}
}

// TestRecoveryFSMGapRedrivesAwaitingGrant: a gap arriving on the NEW live epoch
// for a channel still awaiting a re-grant proves the channel is subscribed there
// (the server only gaps subscribed channels), so the retained replay is re-driven
// immediately rather than stalling for a grant event a dropped resume ack might
// never deliver. It re-drives from the RETAINED anchor (conservative — covers the
// new gap too), and the later grant is then a no-op.
func TestRecoveryFSMGapRedrivesAwaitingGrant(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e1 := &epoch{}
	f.handleGap("t.a", "anchor-1", fsmBase, e1, e1) // → REPLAYING on e1
	f.handleReset()                                 // → awaitingGrant, anchor-1 retained

	e2 := &epoch{}
	acts := f.handleGap("t.a", "anchor-2", fsmBase.Add(1*time.Second), e2, e2)
	if len(acts) != 1 || acts[0].fromPos != "anchor-1" || acts[0].epoch != e2 {
		t.Fatalf("awaitingGrant gap = %+v, want one re-drive from anchor-1 (retained) on e2", acts)
	}
	// The channel is now REPLAYING on e2, so the resume grant that follows is a no-op.
	if got := f.handleGrant([]string{"t.a"}, fsmBase.Add(2*time.Second), e2, e2); len(got) != 0 {
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
	f := newRecoveryFSM(10 * time.Second)
	dead, live := &epoch{}, &epoch{}

	acts := f.handleGap("t.a", "anchor-1", fsmBase, live, dead) // current=live, evEpoch=dead
	if len(acts) != 0 {
		t.Fatalf("stale gap produced %d actions, want 0 (no send on a foreign epoch)", len(acts))
	}
	rec := f.channels["t.a"]
	if rec.phase != recAwaitingGrant || rec.anchor != "anchor-1" {
		t.Errorf("stale gap: phase=%v anchor=%q, want recAwaitingGrant / anchor-1", rec.phase, rec.anchor)
	}
	// The retained anchor re-drives when the channel is granted on the live epoch.
	if got := f.handleGrant([]string{"t.a"}, fsmBase.Add(1*time.Second), live, live); len(got) != 1 || got[0].fromPos != "anchor-1" {
		t.Errorf("grant re-drive = %+v, want one from anchor-1", got)
	}
}

// TestRecoveryFSMStaleGrantDropped: a grant that belongs to a superseded epoch is
// dropped — the live epoch's own resume grant re-triggers the re-drive, so nothing
// is stranded.
func TestRecoveryFSMStaleGrantDropped(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e1, live := &epoch{}, &epoch{}
	f.handleGap("t.a", "anchor-1", fsmBase, e1, e1)
	f.handleReset() // → awaitingGrant

	stale := &epoch{}
	if acts := f.handleGrant([]string{"t.a"}, fsmBase.Add(1*time.Second), live, stale); len(acts) != 0 {
		t.Fatalf("stale grant produced %d actions, want 0", len(acts))
	}
	if rec := f.channels["t.a"]; rec.phase != recAwaitingGrant {
		t.Errorf("phase = %v after a stale grant, want recAwaitingGrant (still pending)", rec.phase)
	}
}

// TestRecoveryFSMDueForeignEpochParks: a FLOOR_WAIT channel whose epoch died
// before the floor timer fired is parked for re-grant, not sent on the live epoch.
func TestRecoveryFSMDueForeignEpochParks(t *testing.T) {
	f := newRecoveryFSM(10 * time.Second)
	e1 := &epoch{}
	f.handleGap("t.a", "pos-1", fsmBase, e1, e1)
	f.handleReplayComplete("t.a", fsmBase, e1, e1)
	f.handleGap("t.a", "pos-2", fsmBase.Add(1*time.Second), e1, e1) // → FLOOR_WAIT on e1

	live := &epoch{}
	if acts := f.due(fsmBase.Add(10*time.Second), live); len(acts) != 0 {
		t.Fatalf("due() on a foreign epoch produced %d actions, want 0", len(acts))
	}
	if rec := f.channels["t.a"]; rec.phase != recAwaitingGrant {
		t.Errorf("phase = %v, want recAwaitingGrant", rec.phase)
	}
}
