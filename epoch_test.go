package sukko

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTransport hands back pre-built conns in order (the last repeats), so a
// white-box test can drive the supervisor with connection behavior a real socket
// cannot script — a Read that panics, for instance.
type fakeTransport struct {
	mu    sync.Mutex
	opens int
	conns []Conn
}

func (t *fakeTransport) Open(context.Context) (Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	i := min(t.opens, len(t.conns)-1)
	t.opens++
	return t.conns[i], nil
}

func (t *fakeTransport) Capabilities() Capabilities { return capabilitiesFor(TransportWebSocket) }

func (t *fakeTransport) openCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.opens
}

// panicReadConn panics on Read — the decode-goroutine panic vector (hostile
// server frame ≈ a panic on the two-pass decode). It records whether Close was
// called, so a test can assert the epoch teardown does not leak it.
type panicReadConn struct{ closed atomic.Bool }

func (*panicReadConn) Read(context.Context) ([]byte, error) { panic("boom: crafted decode panic") }
func (*panicReadConn) Send(context.Context, []byte) error   { return nil }
func (c *panicReadConn) Close(int, string) error {
	c.closed.Store(true)
	return nil
}

// oneShotConn delivers one frame, then blocks until its context ends.
type oneShotConn struct {
	frame []byte
	sent  atomic.Bool
}

func (c *oneShotConn) Read(ctx context.Context) ([]byte, error) {
	if c.sent.CompareAndSwap(false, true) {
		return c.frame, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *oneShotConn) Send(context.Context, []byte) error { return nil }
func (c *oneShotConn) Close(int, string) error            { return nil }

// TestClientDecodePanicContainedAndReconnects pins T077/T086: a panic in the
// decode goroutine is recovered — it does NOT propagate to the test process —
// surfaces an in-band *InternalError{Op,Value,Stack}, and tears the epoch down to
// reconnect on the normal backoff path (a panic ≈ disconnect, §IV), rather than
// terminating. Err() stays nil throughout.
func TestClientDecodePanicContainedAndReconnects(t *testing.T) {
	fc := newFakeClock()
	c, err := NewClient(context.Background(), "wss://example.test/ws",
		WithNoAuth(), WithClock(fc), WithRand(newFakeRand()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	pc := &panicReadConn{}
	c.transport = &fakeTransport{conns: []Conn{
		pc,
		&oneShotConn{frame: []byte(`{"type":"message","channel":"acme.x","seq":9,"ts":1,"data":{}}`)},
	}}
	tr := c.transport.(*fakeTransport)
	ec := collectEvents(c)

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Epoch 1's Read panics; recoverEpoch contains it and arms a backoff.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)

	// Epoch 2 delivers the message — proving the panic reconnected, not crashed.
	ec.waitForMessage(t, 9)
	if got := tr.openCount(); got != 2 {
		t.Errorf("openCount = %d, want 2 (panic then reconnect)", got)
	}
	// The panicked conn must be closed by the epoch teardown, not leaked — a
	// decode panic leaves no in-flight Read for ctx-cancel to close the socket
	// through, so the teardown must close it explicitly.
	if !pc.closed.Load() {
		t.Error("the panicked connection was not closed — leaked fd + tenant slot")
	}

	var ie *InternalError
	for _, evt := range ec.snapshot() {
		if e, ok := evt.(*InternalError); ok {
			ie = e
		}
	}
	if ie == nil {
		t.Fatal("no *InternalError surfaced for the recovered panic")
	}
	if ie.Op != "decode" {
		t.Errorf("InternalError.Op = %q, want \"decode\"", ie.Op)
	}
	if !strings.Contains(ie.Value, "boom") {
		t.Errorf("InternalError.Value = %q, want it to carry the panic value", ie.Value)
	}
	if ie.Stack == "" {
		t.Error("InternalError.Stack is empty, want a captured stack")
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil while reconnecting", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ec.waitClosed(t)
}

// TestClientHeartbeatKeepsAlive pins the healthy half of T073: each
// HeartbeatInterval the client sends a `heartbeat`, and a server that replies
// `pong` disarms the pong deadline — so the connection stays up and never
// re-dials. The disarm is observed via the fake clock (the pong timer stops).
func TestClientHeartbeatKeepsAlive(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{typeHeartbeat: {`{"type":"pong"}`}}})

	c, fc := newClockedClient(t, f)
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	for i := 1; i <= 3; i++ {
		fc.BlockUntilTimer(purposeHeartbeat)
		fc.Advance(DefaultHeartbeatInterval)
		waitForFrameCount(t, f, typeHeartbeat, i)
		// The pong round-trips and disarms the deadline: the pong timer stops.
		waitForArmedTimers(t, fc, purposePong, 0)
	}

	if got := f.dialCount(); got != 1 {
		t.Errorf("dialCount = %d, want 1 — a pong-answered heartbeat must not reconnect", got)
	}
	for _, evt := range ec.snapshot() {
		switch evt.(type) {
		case *CloseError, *InternalError:
			t.Errorf("unexpected %T on a healthy heartbeat path", evt)
		}
	}
	if got := c.State(); got != StateConnected {
		t.Errorf("State() = %v, want connected", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ec.waitClosed(t)
}

// TestClientHeartbeatTimeoutReconnects pins the timeout half of T073: a server
// that withholds the pong lets the pong deadline fire, which closes locally with
// CloseCodeHeartbeatTimeout (4999) — classified DIRECTLY as {4999, local}
// reconnect-class, not read back as a remote close — so the client surfaces a
// *CloseError{4999} and reconnects.
func TestClientHeartbeatTimeoutReconnects(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // no respond for heartbeat → pong is withheld

	c, fc := newClockedClient(t, f, WithRand(newFakeRand()))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Send a heartbeat, then let its pong deadline expire.
	fc.BlockUntilTimer(purposeHeartbeat)
	fc.Advance(DefaultHeartbeatInterval)
	waitForFrameCount(t, f, typeHeartbeat, 1)
	fc.BlockUntilTimer(purposePong)
	fc.Advance(DefaultPongTimeout)

	// The 4999 close is reconnect-class: the client backs off and re-dials.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)

	var closeCode int
	for _, evt := range ec.snapshot() {
		if ce, ok := evt.(*CloseError); ok {
			closeCode = ce.Code
		}
	}
	if closeCode != CloseCodeHeartbeatTimeout {
		t.Errorf("surfaced CloseError code = %d, want %d (heartbeat timeout)", closeCode, CloseCodeHeartbeatTimeout)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil while reconnecting", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ec.waitClosed(t)
}

// floodConn returns a message frame on every Read (until its context ends), so a
// test can fill the delivery channel and park the decode loop on a forward.
type floodConn struct{ frame []byte }

func (c *floodConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return c.frame, nil
	}
}
func (c *floodConn) Send(context.Context, []byte) error { return nil }
func (c *floodConn) Close(int, string) error            { return nil }

// TestClientHeartbeatSuspendedUnderBackpressure pins T073's suspension clause and
// review finding 3's window logic: while the decode loop is parked on
// back-pressure (a slow consumer), a fired pong deadline must NOT close 4999 —
// the server's own 1008 owns the slow-client verdict, and the SDK must not
// preempt it. The deadline re-arms instead, and no reconnect occurs.
func TestClientHeartbeatSuspendedUnderBackpressure(t *testing.T) {
	fc := newFakeClock()
	c, err := NewClient(context.Background(), "wss://example.test/ws",
		WithNoAuth(), WithClock(fc), WithRand(newFakeRand()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tr := &fakeTransport{conns: []Conn{
		&floodConn{frame: []byte(`{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`)},
	}}
	c.transport = tr

	// Deliberately do NOT drain Messages(): the flood fills the channel and parks
	// the decode loop on a forward.
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c.delivery.waitUntilBlocked()

	// Send a heartbeat and let its pong deadline expire while parked — no pong can
	// arrive because the decode loop is not reading.
	fc.BlockUntilTimer(purposeHeartbeat)
	fc.Advance(DefaultHeartbeatInterval)
	fc.BlockUntilTimer(purposePong)
	fc.Advance(DefaultPongTimeout)

	// The deadline must have re-armed (suspended), not closed: the pong timer is
	// live again and no reconnect happened.
	waitForArmedTimers(t, fc, purposePong, 1)
	if got := tr.openCount(); got != 1 {
		t.Errorf("openCount = %d, want 1 — a parked consumer must not trigger a heartbeat-timeout reconnect", got)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil (still connected, merely back-pressured)", err)
	}
	if got := c.State(); got != StateConnected {
		t.Errorf("State() = %v, want connected", got)
	}

	// Close unparks the decode loop via root cancellation → clean stop. The final
	// *Terminal lands in its reserved slot even with the data region full, so
	// Close completes without a draining consumer; drain afterward to confirm.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ec := collectEvents(c)
	ec.waitClosed(t)
	for _, evt := range ec.snapshot() {
		if ce, ok := evt.(*CloseError); ok && ce.Code == CloseCodeHeartbeatTimeout {
			t.Error("a 4000 heartbeat-timeout close was surfaced despite back-pressure suspension")
		}
	}
}

// waitForFrameCount polls until the fake has received n frames of the given type.
func waitForFrameCount(t *testing.T, f *fakeWS, typ string, n int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for len(f.framesOfType(typ)) < n {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d %q frames (have %d)", n, typ, len(f.framesOfType(typ)))
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// waitForArmedTimers polls until exactly n timers of the given purpose are armed.
func waitForArmedTimers(t *testing.T, fc *fakeClock, purpose timerPurpose, n int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for fc.armedTimers(purpose) != n {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d armed %q timers (have %d)", n, purpose, fc.armedTimers(purpose))
		case <-time.After(2 * time.Millisecond):
		}
	}
}
