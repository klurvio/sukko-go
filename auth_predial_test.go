package sukko

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// TestTokenSourceOnlyFetchesBeforeFirstDial pins B1's core behavior: a
// TokenSource-only client (no WithToken, so an empty credential store) must fetch
// a credential from the owner BEFORE the first dial, so the handshake carries it.
// Without the pre-dial fetch the dial would go out bare and earn a 401.
func TestTokenSourceOnlyFetchesBeforeFirstDial(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	ts := func(context.Context) (Token, error) { return Token{Value: "dial-jwt"}, nil }
	c, _ := authClient(t, f, WithTokenSource(ts))
	connectThenClose(t, c)

	if got := f.authAt(1).authorization; got != "Bearer dial-jwt" {
		t.Errorf("dial Authorization = %q, want the pre-dial fetched token", got)
	}
}

// TestTokenSourceDialFailuresPastMaxStayNonTerminal is the discriminating FR-005
// line-63 test: a pre-dial fetch failure is non-terminal FOREVER. The token
// endpoint is down across many reconnect attempts — well past
// MaxTokenSourceAttempts — yet the client keeps backing off and re-attempting,
// never manufacturing its own terminal state.
func TestTokenSourceDialFailuresPastMaxStayNonTerminal(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	ts := func(context.Context) (Token, error) { return Token{}, errors.New("token endpoint down") }
	c, fc := authClient(t, f, WithTokenSource(ts), WithRand(newFakeRand()))
	ec := collectEvents(c)
	_ = c.Connect(context.Background()) // attempt 1 (pre-dial) fails
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Close(ctx)
		ec.waitClosed(t)
	}()

	// Drive reconnect dials well past Max; each does a failing pre-dial fetch and
	// the counter never resets (no success intervenes).
	for range MaxTokenSourceAttempts + 1 {
		fc.BlockUntilTimer(purposeBackoff)
		fc.Advance(DefaultBackoff.Max)
	}

	// A *TokenSourceError with an attempt past Max must have surfaced — and the
	// client is still reconnecting, never terminal.
	highest := 0
	deadline := time.After(3 * time.Second)
	for highest <= MaxTokenSourceAttempts {
		for _, ev := range ec.snapshot() {
			if e, ok := ev.(*TokenSourceError); ok && e.Attempt > highest {
				highest = e.Attempt
			}
		}
		select {
		case <-deadline:
			t.Fatalf("highest *TokenSourceError attempt = %d, want > %d", highest, MaxTokenSourceAttempts)
		case <-time.After(2 * time.Millisecond):
		}
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil (dial-path fetch failures are non-terminal forever)", err)
	}
	if got := c.State(); got != StateReconnecting {
		t.Errorf("State() = %v, want reconnecting", got)
	}
}

// TestTokenSourceDialSuccessResetsThenConnectedExhausts proves the counter is
// SHARED across the dial and connected contexts and RESETS on any success: two
// pre-dial fetches fail (attempts 1, 2), the third succeeds (resetting the
// counter and connecting), then every connected refresh fails — so the terminal
// fires on the 5th CONNECTED strike, not the 3rd. If the success had not reset
// the shared counter, the terminal would fire two strikes early.
func TestTokenSourceDialSuccessResetsThenConnectedExhausts(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	var calls atomic.Int64
	ts := func(context.Context) (Token, error) {
		if calls.Add(1) == 3 {
			return Token{Value: "dial-jwt"}, nil // the third fetch connects
		}
		return Token{}, errors.New("token endpoint down")
	}
	c, fc := authClient(t, f, WithTokenSource(ts),
		WithRand(newFakeRand()), WithHeartbeatInterval(time.Hour))
	ec := collectEvents(c)
	_ = c.Connect(context.Background()) // call 1 (pre-dial) fails

	// Two reconnect dials: call 2 fails, call 3 succeeds → connected.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Max)
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Max)
	waitForState(t, c, StateConnected)

	// Exhaust the connected path: five strikes → terminal.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	for range MaxTokenSourceAttempts {
		fc.BlockUntilTimer(purposeRefresh)
		fc.Advance(DefaultRefreshMinInterval)
	}

	ec.waitClosed(t)
	if err := c.Err(); !errors.Is(err, ErrTokenSourceFailed) {
		t.Errorf("Err() = %v, want ErrTokenSourceFailed", err)
	}
	// The full attempt sequence: dial-path 1,2 then a reset to 1..5 on the
	// connected path. Terminal on the connected 5th strike proves the reset.
	var attempts []int
	for _, ev := range ec.snapshot() {
		if e, ok := ev.(*TokenSourceError); ok {
			attempts = append(attempts, e.Attempt)
		}
	}
	if want := []int{1, 2, 1, 2, 3, 4, 5}; !slices.Equal(attempts, want) {
		t.Errorf("TokenSourceError attempts = %v, want %v", attempts, want)
	}
}

// TestTokenSourceCloseDuringPreDialFetchCleanStop pins that a Close landing while
// a pre-dial fetch is in flight is a CLEAN stop: the fetch is aborted by owner
// shutdown, so it counts no strike and surfaces no *TokenSourceError, and Err()
// stays nil.
func TestTokenSourceCloseDuringPreDialFetchCleanStop(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	entered := make(chan struct{}, 1)
	ts := func(ctx context.Context) (Token, error) {
		entered <- struct{}{}
		<-ctx.Done() // block the fetch in flight until Close cancels the owner
		return Token{}, ctx.Err()
	}
	c, _ := authClient(t, f, WithTokenSource(ts))
	ec := collectEvents(c)
	go func() { _ = c.Connect(context.Background()) }()

	<-entered // the pre-dial fetch is now in flight
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ec.waitClosed(t)

	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil (Close mid-fetch is a clean stop)", err)
	}
	for _, ev := range ec.snapshot() {
		if tse, ok := ev.(*TokenSourceError); ok {
			t.Errorf("a *TokenSourceError surfaced for a Close-aborted fetch: %+v", tse)
		}
	}
}

// panicOnRefreshClock is a fake clock that panics when the auth-owner arms its
// proactive refresh timer — the injectable seam for forcing an in-owner panic on
// the exact goroutine B1's dial rendezvous depends on. Every other timer purpose
// (backoff, dial, heartbeat) delegates unchanged.
type panicOnRefreshClock struct{ *fakeClock }

func (c panicOnRefreshClock) NewTimer(d time.Duration, p timerPurpose) Timer {
	if p == purposeRefresh {
		panic("clock boom: refresh timer")
	}
	return c.fakeClock.NewTimer(d, p)
}

// TestAuthOwnerPanicTerminates pins Finding 1's fix: an auth-owner panic (here the
// proactive-arm during a pre-dial fetch, which panics while the supervisor is
// parked on the ensure-token reply) must terminate the client with an
// *InternalError — never leave a TokenSource client wedged in StateReconnecting
// on a silently-dead owner.
func TestAuthOwnerPanicTerminates(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	// A non-zero Token.Expiry makes the pre-dial fetch arm a proactive refresh
	// timer — the clock panics there, killing the owner mid-rendezvous.
	ts := func(context.Context) (Token, error) {
		return Token{Value: "tok", Expiry: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}, nil
	}
	fc := panicOnRefreshClock{newFakeClock()}
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithTokenSource(ts))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	_ = c.Connect(context.Background()) // the pre-dial fetch panics the owner

	ec.waitClosed(t)
	var ie *InternalError
	if !errors.As(c.Err(), &ie) {
		t.Fatalf("Err() = %v, want an *InternalError from the owner panic", c.Err())
	}
	if ie.Op != "auth-owner" {
		t.Errorf("InternalError.Op = %q, want auth-owner", ie.Op)
	}
	// A failure-caused teardown must land in StateError, not the StateClosed of a
	// clean stop — the supervisor discriminates on the recorded cause.
	if got := c.State(); got != StateError {
		t.Errorf("State() = %v, want error (owner panic is a terminal failure)", got)
	}
	// *Terminal must carry the same cause.
	var termErr error
	for _, ev := range ec.snapshot() {
		if term, ok := ev.(*Terminal); ok {
			termErr = term.Err
		}
	}
	if !errors.As(termErr, &ie) {
		t.Errorf("Terminal.Err = %v, want the *InternalError", termErr)
	}
}
