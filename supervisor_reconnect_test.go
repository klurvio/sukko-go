package sukko

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// eventCollector drains a client's Messages() into a slice on its own goroutine,
// so a test can drive the fake clock without also having to be the consumer.
type eventCollector struct {
	mu     sync.Mutex
	events []Event
	closed chan struct{}
}

func collectEvents(c *Client) *eventCollector {
	ec := &eventCollector{closed: make(chan struct{})}
	go func() {
		for ev := range c.Messages() {
			ec.mu.Lock()
			ec.events = append(ec.events, ev)
			ec.mu.Unlock()
		}
		close(ec.closed)
	}()
	return ec
}

func (ec *eventCollector) snapshot() []Event {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return append([]Event(nil), ec.events...)
}

// waitForMessage polls the collected events for a *Message with the given seq.
func (ec *eventCollector) waitForMessage(t *testing.T, seq int64) *Message {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		for _, ev := range ec.snapshot() {
			if m, ok := ev.(*Message); ok && m.Seq == seq {
				return m
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for *Message seq %d", seq)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (ec *eventCollector) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-ec.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Messages() to close")
	}
}

// stateChanges returns the ordered {From,To} pairs from the collected events.
func (ec *eventCollector) stateChanges() [][2]ConnectionState {
	var out [][2]ConnectionState
	for _, ev := range ec.snapshot() {
		if sc, ok := ev.(*StateChange); ok {
			out = append(out, [2]ConnectionState{sc.From, sc.To})
		}
	}
	return out
}

// newClockedClient builds a fakeWS-pointed client and returns it with the fake
// clock the test drives.
func newClockedClient(t *testing.T, f *fakeWS, opts ...Option) (*Client, *fakeClock) {
	t.Helper()
	fc := newFakeClock()
	base := []Option{WithHTTPClient(f.client()), WithNoAuth(), WithClock(fc)}
	c, err := NewClient(context.Background(), f.URL(), append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, fc
}

// TestClientReconnectAfterRemoteClose is the reconnect happy path: epoch 1
// delivers a message then the server closes 1011 (a reconnect-class remote
// close); the client surfaces a *CloseError, transitions connected→reconnecting,
// backs off, re-dials, transitions reconnecting→connected, and delivers epoch
// 2's message on the new socket. The attempt counter having reset on the first
// handshake makes the backoff exactly Initial.
func TestClientReconnectAfterRemoteClose(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			onConnect:   []string{`{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`},
			closeAfter:  websocket.StatusCode(1011),
			closeReason: "server restart",
		},
		epochScript{
			onConnect: []string{`{"type":"message","channel":"acme.x","seq":2,"ts":2,"data":{}}`},
		},
	)

	// A midpoint fake rand makes the jitter term vanish, so the first reconnect's
	// backoff is exactly DefaultBackoff.Initial and the advance below is precise.
	c, fc := newClockedClient(t, f, WithRand(newFakeRand()))
	ec := collectEvents(c)

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// The supervisor arms the backoff timer only after epoch 1 closed and the
	// *CloseError + state change were emitted. Rendezvous, then release it.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)

	ec.waitForMessage(t, 2)

	if got := f.dialCount(); got != 2 {
		t.Errorf("dialCount = %d, want 2 (one reconnect)", got)
	}

	// A reconnect-class *CloseError for the 1011 was surfaced.
	var sawClose bool
	for _, ev := range ec.snapshot() {
		if ce, ok := ev.(*CloseError); ok {
			sawClose = true
			if ce.Code != 1011 {
				t.Errorf("CloseError.Code = %d, want 1011", ce.Code)
			}
		}
	}
	if !sawClose {
		t.Error("no *CloseError surfaced for the reconnect-class close")
	}

	// State path: disconnected→connecting→connected→reconnecting→connected.
	want := [][2]ConnectionState{
		{StateDisconnected, StateConnecting},
		{StateConnecting, StateConnected},
		{StateConnected, StateReconnecting},
		{StateReconnecting, StateConnected},
	}
	if got := ec.stateChanges(); !equalStatePairs(got, want) {
		t.Errorf("state changes = %v, want %v", got, want)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ec.waitClosed(t)
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil after a clean close", err)
	}
}

// TestClientHandshake429RetryAfter pins T072/T088: a 429 TENANT_LIMIT_EXCEEDED
// with a Retry-After header overrides the NEXT reconnect attempt's backoff delay
// and takes ZERO Rand draws, while an absent header falls back to the pinned
// formula (and its one draw). Discrimination is by how far the clock must advance
// to release the backoff timer: 3s does not fire on a 500ms wait and vice-versa.
func TestClientHandshake429RetryAfter(t *testing.T) {
	const tenantLimitBody = `{"code":"TENANT_LIMIT_EXCEEDED","message":"cap"}`

	tests := []struct {
		name       string
		retryAfter string // "" = header absent
		shortWait  time.Duration
		fullWait   time.Duration
		wantZero   bool // the override attempt draws no randomness at all
	}{
		// Retry-After:3 → 3s wait; 500ms must NOT release it, 3s must. The override
		// skips the formula entirely, so it never draws.
		{name: "retry-after 3s overrides", retryAfter: "3", shortWait: 500 * time.Millisecond, fullWait: 3 * time.Second, wantZero: true},
		// Absent → the pinned formula, whose midpoint draw yields exactly
		// DefaultBackoff.Initial and takes at least one draw.
		{name: "absent uses pinned formula", retryAfter: "", shortWait: 0, fullWait: DefaultBackoff.Initial, wantZero: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeWS(t)
			f.rejectUpgrade(429, tenantLimitBody)
			if tc.retryAfter != "" {
				f.setUpgradeHeader("Retry-After", tc.retryAfter)
			}
			r := newFakeRand() // midpoint draws → jitter vanishes, delay is exact
			c, fc := newClockedClient(t, f, WithRand(r))
			ec := collectEvents(c)

			var he *HandshakeError
			if err := c.Connect(context.Background()); !errors.As(err, &he) {
				t.Fatalf("Connect = %v, want *HandshakeError(429)", err)
			}
			if f.dialCount() != 1 {
				t.Fatalf("dialCount = %d, want 1 before backoff released", f.dialCount())
			}

			fc.BlockUntilTimer(purposeBackoff)
			if tc.shortWait > 0 {
				fc.Advance(tc.shortWait)
				if got := f.dialCount(); got != 1 {
					t.Errorf("dialCount = %d after a %v partial advance, want 1 (backoff not yet due)", got, tc.shortWait)
				}
			}
			fc.Advance(tc.fullWait - tc.shortWait)
			waitForDialCount(t, f, 2)

			switch got := r.draws(); {
			case tc.wantZero && got != 0:
				t.Errorf("Rand draws = %d, want 0 — a Retry-After override skips the jitter formula", got)
			case !tc.wantZero && got < 1:
				t.Errorf("Rand draws = %d, want ≥1 — the pinned formula draws jitter", got)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := c.Close(ctx); err != nil {
				t.Fatalf("Close: %v", err)
			}
			ec.waitClosed(t)
		})
	}
}

// waitForDialCount polls until the fake has seen n upgrade attempts.
func waitForDialCount(t *testing.T, f *fakeWS, n int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for f.dialCount() < n {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for dialCount %d (have %d)", n, f.dialCount())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestClientReconnectDisabledCleanStop pins T074's WithReconnect(false) downgrade
// over a reconnect-class close: the SDK emits its *CloseError, then a final
// *Terminal carrying the close cause, then closes — with no reconnect (dialCount
// stays 1) and Err() NIL even though Terminal.Err is the cause. That divergence
// is the whole point of the second cause field.
func TestClientReconnectDisabledCleanStop(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{
		onConnect:   []string{`{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`},
		closeAfter:  websocket.StatusCode(1011),
		closeReason: "server restart",
	})

	c, _ := newClockedClient(t, f, WithReconnect(false))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ec.waitClosed(t)

	if got := f.dialCount(); got != 1 {
		t.Errorf("dialCount = %d, want 1 — WithReconnect(false) must not re-dial", got)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil — the caller asked for no reconnect and got a clean stop", err)
	}
	if got := c.State(); got != StateClosed {
		t.Errorf("State() = %v, want closed", got)
	}

	events := ec.snapshot()
	// *CloseError precedes *Terminal, and *Terminal.Err is that close cause.
	var closeIdx, termIdx = -1, -1
	var term *Terminal
	for i, ev := range events {
		switch e := ev.(type) {
		case *CloseError:
			if closeIdx < 0 {
				closeIdx = i
			}
		case *Terminal:
			termIdx = i
			term = e
		}
	}
	if closeIdx < 0 || termIdx < 0 || closeIdx > termIdx {
		t.Fatalf("expected *CloseError before *Terminal; closeIdx=%d termIdx=%d", closeIdx, termIdx)
	}
	var ce *CloseError
	if !errors.As(term.Err, &ce) {
		t.Errorf("Terminal.Err = %v, want the *CloseError cause", term.Err)
	} else if ce.Code != 1011 {
		t.Errorf("Terminal.Err code = %d, want 1011", ce.Code)
	}
}

// TestClientReconnectDialTimeout exercises the reconnect-dial watcher (T081 WS
// leg): the first dial succeeds, the epoch closes 1011, and the reconnect dial
// hangs. The injectable "dial" timer bounds it — advancing DialTimeout aborts the
// dial, which classifies as a reconnect-class network failure, so the loop backs
// off and tries again rather than hanging forever. Asserted deterministically via
// BlockUntilTimer("dial") → Advance(DialTimeout).
func TestClientReconnectDialTimeout(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{closeAfter: websocket.StatusCode(1011), closeReason: "restart"})
	release := f.stallFrom(2) // let dial 1 succeed; hang every reconnect dial
	defer release()

	c, fc := newClockedClient(t, f, WithRand(newFakeRand()))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Reconnect #1: release the backoff, which starts the (stalled) reconnect dial.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2) // the reconnect dial reached the server and parked

	// The dial is bounded by the "dial" timer: advancing it aborts the hung dial.
	fc.BlockUntilTimer(purposeDial)
	fc.Advance(DefaultDialTimeout)

	// The aborted dial is reconnect-class, so the loop arms a SECOND backoff
	// (attempt 1) rather than terminating — proving the watcher unblocked the dial
	// and the failure was classified, not fatal.
	fc.BlockUntilTimer(purposeBackoff)

	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil while still reconnecting", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ec.waitClosed(t)
}

// TestClientCloseDuringBackoff pins that a Close landing while the supervisor is
// parked in the backoff wait (no clock advance) unparks it promptly via the root
// cancellation, ending as a clean stop: Err() nil, Terminal.Err nil, no extra
// dial.
func TestClientCloseDuringBackoff(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{closeAfter: websocket.StatusCode(1011), closeReason: "restart"})

	c, fc := newClockedClient(t, f, WithRand(newFakeRand()))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Wait until the supervisor is parked in the backoff, then Close without ever
	// advancing the clock: root cancellation must unpark it.
	fc.BlockUntilTimer(purposeBackoff)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close during backoff: %v (must unpark promptly)", err)
	}
	ec.waitClosed(t)

	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil — a Close during backoff is a clean stop", err)
	}
	if got := c.State(); got != StateClosed {
		t.Errorf("State() = %v, want closed", got)
	}
	if got := f.dialCount(); got != 1 {
		t.Errorf("dialCount = %d, want 1 — Close during backoff must not re-dial", got)
	}
	if term := drainLastTerminal(ec); term == nil {
		t.Error("no *Terminal before close")
	} else if term.Err != nil {
		t.Errorf("Terminal.Err = %v, want nil", term.Err)
	}
}

func drainLastTerminal(ec *eventCollector) *Terminal {
	var term *Terminal
	for _, ev := range ec.snapshot() {
		if tm, ok := ev.(*Terminal); ok {
			term = tm
		}
	}
	return term
}

func equalStatePairs(a, b [][2]ConnectionState) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
