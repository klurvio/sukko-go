package sukko

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newTestClient builds a client pointed at a fakeWS: no auth, the fake's TLS
// client, and a fake clock for determinism. The lifetime context is the caller's
// to cancel via Close.
func newTestClient(t *testing.T, f *fakeWS, opts ...Option) *Client {
	t.Helper()
	base := []Option{
		WithHTTPClient(f.client()),
		WithNoAuth(),
		WithClock(newFakeClock()),
	}
	c, err := NewClient(context.Background(), f.URL(), append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// drainUntilMessage reads events until a *Message arrives, skipping the
// lifecycle events (state changes) that precede it.
func drainUntilMessage(t *testing.T, c *Client) *Message {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-c.Messages():
			if !ok {
				t.Fatal("Messages() closed before a *Message arrived")
			}
			if m, isMsg := ev.(*Message); isMsg {
				return m
			}
		case <-timeout:
			t.Fatal("timed out waiting for a *Message")
		}
	}
}

// drainUntilClosed reads events until Messages() closes, returning the last
// *Terminal seen (nil if none).
func drainUntilClosed(t *testing.T, c *Client) *Terminal {
	t.Helper()
	var term *Terminal
	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-c.Messages():
			if !ok {
				return term
			}
			if tm, isTerm := ev.(*Terminal); isTerm {
				term = tm
			}
		case <-timeout:
			t.Fatal("timed out waiting for Messages() to close")
		}
	}
}

// TestClientConnectReceiveClose is the happy-path lifecycle: connect, receive a
// message, close cleanly. After a clean Close, Err() is nil and the stream ends
// with a *Terminal carrying no cause.
func TestClientConnectReceiveClose(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{onConnect: []string{
		`{"type":"message","channel":"acme.x","seq":7,"ts":1,"data":{"k":1}}`,
	}})

	c := newTestClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	msg := drainUntilMessage(t, c)
	if msg.Channel != "acme.x" || msg.Seq != 7 {
		t.Errorf("message = %+v, want channel acme.x seq 7", msg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil after a clean close", err)
	}
	if got := c.State(); got != StateClosed {
		t.Errorf("State() = %v, want closed", got)
	}
	if term := drainUntilClosed(t, c); term == nil {
		t.Error("no *Terminal before Messages() closed")
	} else if term.Err != nil {
		t.Errorf("Terminal.Err = %v, want nil on a clean close", term.Err)
	}
}

// TestClientCloseWithoutConnect is the fix the checkpoint surfaced: a client that
// was never connected must Close cleanly and promptly — not hang until the ctx
// expires — with Messages() closed and Err() nil.
func TestClientCloseWithoutConnect(t *testing.T) {
	f := newFakeWS(t)
	c := newTestClient(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close on a never-connected client: %v (a clean close must not error)", err)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}

	// The stream still terminates properly: a *Terminal{nil}, then close.
	if term := drainUntilClosed(t, c); term == nil {
		t.Error("no *Terminal on a never-connected close")
	} else if term.Err != nil {
		t.Errorf("Terminal.Err = %v, want nil", term.Err)
	}

	// A second Close is idempotent.
	if err := c.Close(ctx); err != nil {
		t.Errorf("second Close = %v, want nil (idempotent)", err)
	}
}

// TestClientTerminalFailure covers a terminal handshake failure (401): Connect
// returns the typed error, the client ends in StateError with Err() non-nil, and
// the final *Terminal carries the cause. This is the discrimination T080 pins —
// distinct from a clean Close, which leaves Err() nil.
func TestClientTerminalFailure(t *testing.T) {
	f := newFakeWS(t)
	f.rejectUpgrade(401, `{"code":"UNAUTHORIZED","message":"bad token"}`)

	c := newTestClient(t, f)
	err := c.Connect(context.Background())

	var he *HandshakeError
	if !errors.As(err, &he) {
		t.Fatalf("Connect error = %v (%T), want *HandshakeError", err, err)
	}
	if he.Status != 401 {
		t.Errorf("status = %d, want 401", he.Status)
	}

	term := drainUntilClosed(t, c)
	if term == nil {
		t.Fatal("no *Terminal before close")
	}
	var termCause *HandshakeError
	if !errors.As(term.Err, &termCause) {
		t.Errorf("Terminal.Err = %v, want a *HandshakeError cause", term.Err)
	}

	if got := c.Err(); !errors.As(got, &termCause) {
		t.Errorf("Err() = %v, want the terminal *HandshakeError", got)
	}
	if got := c.State(); got != StateError {
		t.Errorf("State() = %v, want error", got)
	}

	// A terminal client is single-use: re-Connect is refused.
	if err := c.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("re-Connect after terminal = %v, want ErrClosed", err)
	}
}

// TestClientCloseDuringDial covers the discrimination fix: a Close (or lifetime
// cancel) that aborts an in-flight dial is a CLEAN stop, not a terminal failure —
// Err() nil, StateClosed, *Terminal{nil} — even though the aborted dial surfaces
// an error. Cause, not outcome.
func TestClientCloseDuringDial(t *testing.T) {
	f := newFakeWS(t)
	release := f.stall() // the handshake hangs until released
	defer release()

	c := newTestClient(t, f)

	connErr := make(chan error, 1)
	go func() { connErr <- c.Connect(context.Background()) }()

	// Let Connect reach the parked dial, then Close mid-dial.
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close during dial: %v", err)
	}

	<-connErr // Connect returns (value is racy under concurrent Close; not asserted)

	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil — a Close-aborted dial is a clean stop", err)
	}
	if got := c.State(); got != StateClosed {
		t.Errorf("State() = %v, want closed", got)
	}
	if term := drainUntilClosed(t, c); term != nil && term.Err != nil {
		t.Errorf("Terminal.Err = %v, want nil", term.Err)
	}
}

// TestClientLifetimeCancelBeforeConnect covers the AfterFunc fix: canceling the
// NewClient lifetime context on a client that never connected must tear it down
// exactly like Close — Messages() closes, Err() nil — rather than stranding a
// ranging caller forever.
func TestClientLifetimeCancelBeforeConnect(t *testing.T) {
	f := newFakeWS(t)
	lifeCtx, cancelLife := context.WithCancel(context.Background())
	c, err := NewClient(lifeCtx, f.URL(), WithHTTPClient(f.client()), WithNoAuth(), WithClock(newFakeClock()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	cancelLife() // tear down before Connect

	// Messages() must close (with a final *Terminal{nil}) rather than hang.
	if term := drainUntilClosed(t, c); term == nil {
		t.Error("no *Terminal after lifetime cancel")
	} else if term.Err != nil {
		t.Errorf("Terminal.Err = %v, want nil", term.Err)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
	if err := c.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Connect after lifetime cancel = %v, want ErrClosed", err)
	}
}

// TestClientLifetimeCancelWhileConnected confirms the connected path too: a live
// client whose lifetime context is canceled tears down cleanly through the
// epoch context, the same as Close.
func TestClientLifetimeCancelWhileConnected(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{onConnect: []string{
		`{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`,
	}})
	lifeCtx, cancelLife := context.WithCancel(context.Background())
	c, err := NewClient(lifeCtx, f.URL(), WithHTTPClient(f.client()), WithNoAuth(), WithClock(newFakeClock()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = drainUntilMessage(t, c)

	cancelLife() // tear down a live client via its lifetime

	if term := drainUntilClosed(t, c); term == nil {
		t.Error("no *Terminal after lifetime cancel of a connected client")
	} else if term.Err != nil {
		t.Errorf("Terminal.Err = %v, want nil on a lifetime-cancel clean stop", term.Err)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
}
