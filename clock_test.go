package sukko

import (
	"testing"
	"time"
)

// The Clock seam is what makes the timing tests deterministic. Its contract has
// three parts, and all three matter: Now is the SDK's only wall-clock source,
// timers are armed under a purpose name so a test can rendezvous with the one
// it is about to fire, and Advance delivers due timers before it returns.
//
// The named rendezvous replaces a global "wait for N timers" primitive, which
// couples every timing test to the total number of timers the client happens to
// have armed and hangs rather than fails when that count is wrong.

func TestTimerPurposesAreClosedAndUnique(t *testing.T) {
	t.Parallel()

	purposes := allTimerPurposes()
	if len(purposes) != 12 {
		t.Errorf("expected 12 timer purposes, got %d", len(purposes))
	}

	seen := make(map[timerPurpose]bool, len(purposes))
	for _, p := range purposes {
		if seen[p] {
			t.Errorf("duplicate timer purpose %q", p)
		}
		seen[p] = true
		if p == "" {
			t.Error("a timer purpose must not be empty — it is the rendezvous key")
		}
	}

	// Every named path the spec enumerates must be present. An unlisted timing
	// path is a real sleep waiting to happen.
	for _, want := range []timerPurpose{
		purposeBackoff, purposeHeartbeat, purposePong, purposeRefresh,
		purposeReplayFloor, purposeRecoveryDeadline, purposeAckTimeout,
		purposeBlockedWarn, purposeTokenSource, purposeSSEIdle, purposeDial,
		purposeDeliveryProbe,
	} {
		if !seen[want] {
			t.Errorf("timer purpose %q is missing from the closed list", want)
		}
	}
}

// The production clock reports real time. It is asserted loosely — the point is
// that it tracks the wall clock, not that it matches to the nanosecond.
func TestRealClockTracksWallTime(t *testing.T) {
	t.Parallel()

	c := realClock{}
	before := time.Now()
	got := c.Now()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("realClock.Now() = %v, outside [%v, %v]", got, before, after)
	}
}

// BlockUntilTimer is a test rendezvous. On the production clock it must be a
// no-op that returns immediately rather than blocking a live client forever.
func TestRealClockBlockUntilTimerIsANoop(t *testing.T) {
	t.Parallel()

	done := make(chan struct{})
	go func() {
		realClock{}.BlockUntilTimer(purposeBackoff)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("realClock.BlockUntilTimer blocked; on the production path it must return immediately")
	}
}

// A real timer fires, and its channel carries the firing time.
func TestRealClockTimerFires(t *testing.T) {
	t.Parallel()

	timer := realClock{}.NewTimer(time.Millisecond, purposeBackoff)
	defer timer.Stop()

	select {
	case <-timer.C():
	case <-time.After(time.Second):
		t.Fatal("a 1ms timer did not fire within a second")
	}
}

// Stop reports whether it beat the timer. A stopped timer must not then fire —
// the epoch teardown path relies on this to leave no timer running past its
// epoch.
func TestRealClockTimerStop(t *testing.T) {
	t.Parallel()

	timer := realClock{}.NewTimer(time.Hour, purposeDial)
	if !timer.Stop() {
		t.Error("Stop on a long-running timer should report that it stopped it")
	}
	select {
	case <-timer.C():
		t.Error("a stopped timer fired")
	case <-time.After(10 * time.Millisecond):
	}
}

// The production Rand must produce values in [0,1) — the range the jitter
// formula assumes. A value outside it would push a computed backoff delay
// outside its own clamp.
func TestRealRandInRange(t *testing.T) {
	t.Parallel()

	r := realRand{}
	for range 1000 {
		v := r.Float64()
		if v < 0 || v >= 1 {
			t.Fatalf("Float64() = %v, want [0,1)", v)
		}
	}
}

// The seams must be satisfiable by something other than the production
// implementations — that is their entire purpose. This is a compile-time check
// that the interfaces stay small enough for a fake to implement.
func TestSeamsAreImplementable(t *testing.T) {
	t.Parallel()

	var _ Clock = realClock{}
	var _ Rand = realRand{}
	var _ Clock = (*stubClock)(nil)
	var _ Rand = (*stubRand)(nil)
}

// stubClock is the minimal Clock implementation, standing in for the fake the
// test harness will build. It exists here to prove the interface is small
// enough to implement without dragging in the harness.
type stubClock struct{ now time.Time }

func (c *stubClock) Now() time.Time                               { return c.now }
func (c *stubClock) NewTimer(time.Duration, timerPurpose) Timer   { return nil }
func (c *stubClock) NewTicker(time.Duration, timerPurpose) Ticker { return nil }
func (c *stubClock) BlockUntilTimer(timerPurpose)                 {}

type stubRand struct{}

func (stubRand) Float64() float64 { return 0.5 }
