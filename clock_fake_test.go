package sukko

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is the deterministic Clock used by every timing test.
//
// The contract it implements is the one clock.go documents, and the reason it
// exists is that a fake clock alone is not enough: if a test advances time
// before the goroutine under test has armed its timer, the advance lands on
// nothing and the test hangs or passes for the wrong reason. So timers register
// under a purpose name, and BlockUntilTimer parks until one with that name is
// armed. A test rendezvouses first, then advances.
//
// Advance delivers due timers before it returns, so a test that advances past a
// deadline can assert on the consequence immediately rather than polling for it.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	// waiters holds one channel per purpose that a BlockUntilTimer caller is
	// parked on. Closing it releases every waiter for that purpose.
	waiters map[timerPurpose]chan struct{}
}

// newFakeClock returns a clock started at a fixed instant. The instant is
// arbitrary but stable, so a test asserting an absolute deadline gets the same
// answer on every run and on every machine.
func newFakeClock() *fakeClock {
	return &fakeClock{
		now:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		waiters: make(map[timerPurpose]chan struct{}),
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration, purpose timerPurpose) Timer {
	return c.arm(d, purpose, false)
}

func (c *fakeClock) NewTicker(d time.Duration, purpose timerPurpose) Ticker {
	return &fakeTicker{t: c.arm(d, purpose, true)}
}

// arm registers a timer and releases anyone waiting on its purpose.
func (c *fakeClock) arm(d time.Duration, purpose timerPurpose, repeating bool) *fakeTimer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &fakeTimer{
		clock:     c,
		purpose:   purpose,
		deadline:  c.now.Add(d),
		interval:  d,
		repeating: repeating,
		// Buffered so a fire never blocks on a receiver that has not arrived
		// yet — the SDK arms timers it may not immediately select on.
		ch: make(chan time.Time, 1),
	}
	c.timers = append(c.timers, t)

	// Release BlockUntilTimer callers waiting for this purpose.
	if w, ok := c.waiters[purpose]; ok {
		close(w)
		delete(c.waiters, purpose)
	}
	return t
}

// BlockUntilTimer parks until a timer with this purpose is armed. If one is
// already armed it returns immediately, so a test cannot deadlock by
// rendezvousing after the fact.
func (c *fakeClock) BlockUntilTimer(purpose timerPurpose) {
	c.mu.Lock()
	for _, t := range c.timers {
		if t.purpose == purpose && !t.stopped {
			c.mu.Unlock()
			return
		}
	}
	w, ok := c.waiters[purpose]
	if !ok {
		w = make(chan struct{})
		c.waiters[purpose] = w
	}
	c.mu.Unlock()

	<-w
}

// Advance moves time forward and delivers every timer that becomes due,
// in deadline order. A repeating timer re-arms at its interval and can fire
// several times within one advance.
//
// Delivery happens before Advance returns, so the caller can assert on the
// effect of a fired deadline without polling.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)

	type firing struct {
		t  *fakeTimer
		at time.Time
	}
	var due []firing

	for {
		// Find the earliest un-fired timer at or before the target.
		var next *fakeTimer
		for _, t := range c.timers {
			if t.stopped || t.deadline.After(target) {
				continue
			}
			if next == nil || t.deadline.Before(next.deadline) {
				next = t
			}
		}
		if next == nil {
			break
		}

		at := next.deadline
		due = append(due, firing{next, at})
		if next.repeating {
			next.deadline = next.deadline.Add(next.interval)
		} else {
			next.stopped = true
		}
	}

	c.now = target
	c.mu.Unlock()

	// Deliver outside the lock: a receiver may arm another timer in response,
	// which needs the lock.
	for _, f := range due {
		select {
		case f.t.ch <- f.at:
		default:
			// A previous fire is still unread. Dropping matches time.Ticker's
			// own behavior for a slow receiver.
		}
	}
}

// armedTimers reports how many live timers carry a purpose, for harness
// self-tests and for asserting that epoch teardown left nothing running.
func (c *fakeClock) armedTimers(purpose timerPurpose) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	var n int
	for _, t := range c.timers {
		if t.purpose == purpose && !t.stopped {
			n++
		}
	}
	return n
}

// fakeTimer is a timer owned by fakeClock, satisfying both Timer and Ticker.
type fakeTimer struct {
	clock     *fakeClock
	purpose   timerPurpose
	deadline  time.Time
	interval  time.Duration
	repeating bool
	stopped   bool
	ch        chan time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

// Stop marks the timer dead, reporting whether it beat the firing.
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()

	was := !t.stopped
	t.stopped = true
	return was
}

// fakeTicker adapts a fakeTimer to Ticker, whose Stop returns nothing. The two
// interfaces differ deliberately — Timer.Stop reports whether it beat the
// firing, which a repeating timer has no meaningful answer for.
type fakeTicker struct{ t *fakeTimer }

func (k *fakeTicker) C() <-chan time.Time { return k.t.ch }
func (k *fakeTicker) Stop()               { k.t.Stop() }

// fakeRand is a seeded Rand returning a scripted sequence, so a backoff test
// asserts an exact duration rather than a range. The sequence repeats once
// exhausted, so a test need not supply a draw per attempt it does not assert.
type fakeRand struct {
	mu     sync.Mutex
	values []float64
	next   int
}

// newFakeRand returns a Rand yielding the given values in order. With no values
// it always returns 0.5, the midpoint — which makes the jitter term vanish and
// leaves the pure exponential delay, the easiest case to assert.
func newFakeRand(values ...float64) *fakeRand {
	if len(values) == 0 {
		values = []float64{0.5}
	}
	return &fakeRand{values: values}
}

func (r *fakeRand) Float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	v := r.values[r.next%len(r.values)]
	r.next++
	return v
}

// draws reports how many values have been taken, so a test can assert the
// "exactly one draw per attempt, none when jitter is zero" rule.
func (r *fakeRand) draws() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.next
}

// ─── harness self-tests ───
//
// The harness is load-bearing for every later phase, so its own behavior is
// asserted here rather than assumed.

func TestFakeClockAdvanceFiresDueTimers(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	timer := c.NewTimer(10*time.Second, purposeBackoff)

	c.Advance(9 * time.Second)
	select {
	case <-timer.C():
		t.Fatal("timer fired before its deadline")
	default:
	}

	c.Advance(time.Second)
	select {
	case got := <-timer.C():
		if want := c.Now(); !got.Equal(want) {
			t.Errorf("fired at %v, want %v", got, want)
		}
	default:
		t.Fatal("timer did not fire at its deadline")
	}
}

// Delivery must happen before Advance returns; otherwise every timing test
// needs a poll loop, which reintroduces the flakiness the fake clock removes.
func TestFakeClockAdvanceDeliversBeforeReturning(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	timer := c.NewTimer(time.Second, purposePong)
	c.Advance(time.Second)

	select {
	case <-timer.C():
	default:
		t.Fatal("Advance returned before delivering a due timer")
	}
}

func TestFakeClockBlockUntilTimerReleasesOnArm(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	released := make(chan struct{})
	go func() {
		c.BlockUntilTimer(purposeHeartbeat)
		close(released)
	}()

	// The waiter must still be parked: nothing has armed that purpose.
	select {
	case <-released:
		t.Fatal("BlockUntilTimer returned before any timer was armed")
	case <-time.After(20 * time.Millisecond):
	}

	c.NewTicker(time.Second, purposeHeartbeat)

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("BlockUntilTimer did not release when its timer was armed")
	}
}

// Rendezvousing after the timer is already armed must return immediately — a
// test that checks state before waiting would otherwise deadlock.
func TestFakeClockBlockUntilTimerReturnsIfAlreadyArmed(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	c.NewTimer(time.Second, purposeDial)

	done := make(chan struct{})
	go func() {
		c.BlockUntilTimer(purposeDial)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("BlockUntilTimer blocked on an already-armed timer")
	}
}

// Waiting on one purpose must not be released by a different one, or a test
// would advance against a timer it was not waiting for.
func TestFakeClockBlockUntilTimerIsPerPurpose(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	released := make(chan struct{})
	go func() {
		c.BlockUntilTimer(purposeRefresh)
		close(released)
	}()

	c.NewTimer(time.Second, purposeBackoff)

	select {
	case <-released:
		t.Fatal("a timer of a different purpose released the waiter")
	case <-time.After(20 * time.Millisecond):
	}

	// Release the waiter before returning. Leaving it parked would abandon a
	// goroutine for the rest of the run — which the package's own leak check
	// duly reports, and which would be this test lecturing the SDK about
	// lifecycle hygiene while not practicing it.
	c.NewTimer(time.Second, purposeRefresh)
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("the waiter did not release once its own purpose was armed")
	}
}

func TestFakeClockTickerRepeats(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	ticker := c.NewTicker(time.Second, purposeHeartbeat)
	defer ticker.Stop()

	// One advance spanning three intervals fires the ticker each time; the
	// channel holds the most recent, matching time.Ticker's slow-receiver
	// behavior.
	c.Advance(3 * time.Second)
	select {
	case <-ticker.C():
	default:
		t.Fatal("ticker did not fire across a multi-interval advance")
	}
}

func TestFakeClockStoppedTimerDoesNotFire(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	timer := c.NewTimer(time.Second, purposeAckTimeout)
	if !timer.Stop() {
		t.Error("Stop should report that it beat the firing")
	}

	c.Advance(time.Hour)
	select {
	case <-timer.C():
		t.Error("a stopped timer fired")
	default:
	}
	if n := c.armedTimers(purposeAckTimeout); n != 0 {
		t.Errorf("armedTimers = %d after Stop, want 0", n)
	}
}

func TestFakeClockNowAdvances(t *testing.T) {
	t.Parallel()

	c := newFakeClock()
	start := c.Now()
	c.Advance(90 * time.Second)

	if got := c.Now().Sub(start); got != 90*time.Second {
		t.Errorf("clock advanced %v, want 90s", got)
	}
}

func TestFakeRandSequenceAndDrawCount(t *testing.T) {
	t.Parallel()

	r := newFakeRand(0.0, 0.25, 0.75)
	for i, want := range []float64{0.0, 0.25, 0.75, 0.0} { // wraps
		if got := r.Float64(); got != want {
			t.Errorf("draw %d = %v, want %v", i, got, want)
		}
	}
	if got := r.draws(); got != 4 {
		t.Errorf("draws() = %d, want 4", got)
	}
}

// The default fake returns the midpoint, which cancels the jitter term and
// leaves the pure exponential delay — the value a backoff test asserts first.
func TestFakeRandDefaultsToMidpoint(t *testing.T) {
	t.Parallel()

	if got := newFakeRand().Float64(); got != 0.5 {
		t.Errorf("default fake Rand = %v, want 0.5", got)
	}
}

// The seams must accept the fakes — the whole point of the interfaces.
func TestFakesSatisfyTheSeams(t *testing.T) {
	t.Parallel()

	var _ Clock = newFakeClock()
	var _ Rand = newFakeRand()
}
