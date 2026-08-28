package sukko

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// immediateCloseConn returns a remote 1008 on the first Read — a clean epoch (no data
// delivered, so no back-pressure park) that dies with the slow-client code.
type immediateCloseConn struct{}

func (immediateCloseConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return nil, &CloseError{Code: 1008, Direction: directionRemote, Reason: "slow client"}
}
func (immediateCloseConn) Send(context.Context, []byte) error { return nil }
func (immediateCloseConn) Close(int, string) error            { return nil }

// floodThenCloseConn floods the same frame on every Read (filling the delivery
// channel to park the decode loop) until closeNow is set, then returns a remote 1008
// — the server's slow-client verdict.
type floodThenCloseConn struct {
	frame    []byte
	closeNow atomic.Bool
}

func (c *floodThenCloseConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if c.closeNow.Load() {
		return nil, &CloseError{Code: 1008, Direction: directionRemote, Reason: "slow client"}
	}
	return c.frame, nil
}
func (c *floodThenCloseConn) Send(context.Context, []byte) error { return nil }
func (c *floodThenCloseConn) Close(int, string) error            { return nil }

// floodThenLocalCloseConn floods the same frame (parking the decode loop) until
// closeNow is set, then dies with a LOCAL 1006 — a network-class reconnect that does
// NOT carry the server's slow-client verdict, even though this epoch parked.
type floodThenLocalCloseConn struct {
	frame    []byte
	closeNow atomic.Bool
}

func (c *floodThenLocalCloseConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if c.closeNow.Load() {
		return nil, &CloseError{Code: 1006, Direction: directionLocal, Reason: "network"}
	}
	return c.frame, nil
}
func (c *floodThenLocalCloseConn) Send(context.Context, []byte) error { return nil }
func (c *floodThenLocalCloseConn) Close(int, string) error            { return nil }

// backpressureStep is the pure counting rule behind ErrConsumerTooSlow (T132): a
// slow-client 1008 that backed up increments (and terminates at max), a clean epoch
// resets (the consumer resumed), and a non-1008 death that backed up freezes the run.
func TestBackpressureStep(t *testing.T) {
	const limit = 3
	tests := []struct {
		name                     string
		prev                     int
		countsTowardBackpressure bool
		backpressured            bool
		want                     bpAction
	}{
		{"1008 + back-pressure counts", 0, true, true, bpAction{count: 1, counted: true, terminate: false}},
		{"reaching the limit terminates", limit - 1, true, true, bpAction{count: limit, counted: true, terminate: true}},
		{"1008 without back-pressure resets (consumer resumed)", 2, true, false, bpAction{count: 0}},
		{"a clean non-1008 epoch resets", 2, false, false, bpAction{count: 0}},
		{"1006 mid-back-pressure freezes the run", 2, false, true, bpAction{count: 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := backpressureStep(tc.prev, tc.countsTowardBackpressure, tc.backpressured, limit); got != tc.want {
				t.Errorf("backpressureStep = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestBackpressureStepAlternation pins the freeze: a server alternating 1008/1006
// closes against a persistently-slow consumer never resets, but only the 1008 cycles
// count — so it takes more than `max` closes to terminate (the count freezes on 1006).
func TestBackpressureStepAlternation(t *testing.T) {
	const limit = 3
	count := 0
	step := func(counts, bp bool) bpAction {
		a := backpressureStep(count, counts, bp, limit)
		count = a.count
		return a
	}

	if a := step(true, true); count != 1 || a.terminate { // 1008+park → 1
		t.Fatalf("after 1008 #1: count=%d terminate=%v, want 1/false", count, a.terminate)
	}
	if a := step(false, true); count != 1 || a.terminate { // 1006+park → freeze at 1
		t.Fatalf("after 1006: count=%d terminate=%v, want 1/false (frozen, not reset)", count, a.terminate)
	}
	if a := step(true, true); count != 2 || a.terminate { // 1008+park → 2
		t.Fatalf("after 1008 #2: count=%d terminate=%v, want 2/false", count, a.terminate)
	}
	if a := step(true, true); count != 3 || !a.terminate { // 1008+park → 3 → terminate
		t.Fatalf("after 1008 #3: count=%d terminate=%v, want 3/true", count, a.terminate)
	}
}

// TestBackpressureReconnectReachesConsumerTooSlow is the end-to-end wiring proof: a
// consumer that never drains fills the channel (a park episode), the epoch dies with
// a slow-client 1008, and — with the tolerance set to one — that back-pressure-induced
// reconnect terminates the client with ErrConsumerTooSlow, surfacing the tipping 1008
// *CloseError before the final *Terminal.
func TestBackpressureReconnectReachesConsumerTooSlow(t *testing.T) {
	// A real clock here: the tolerance is one, so there is no reconnect backoff to
	// drive deterministically, and the delivery re-probe timer (which unparks the
	// decode loop once the consumer drains) must fire on its own rather than needing a
	// fake-clock advance interleaved with a live draining goroutine.
	c, err := NewClient(context.Background(), "wss://example.test/ws",
		WithNoAuth(), WithQueueSize(117), WithHistoryLimit(1), WithMaxBackpressureReconnects(1))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	conn := &floodThenCloseConn{frame: []byte(`{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`)}
	c.transport = &fakeTransport{conns: []Conn{conn}}

	// Do NOT drain yet: the flood fills the delivery channel and parks the decode loop.
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c.delivery.waitUntilBlocked() // a back-pressure episode has occurred this epoch

	// Now let the epoch die with a slow-client 1008 and start draining, so the decode
	// loop unparks, reads the 1008, and the reconnect is classified back-pressure-induced.
	conn.closeNow.Store(true)
	ec := collectEvents(c)
	ec.waitClosed(t)

	if got := c.Err(); !errors.Is(got, ErrConsumerTooSlow) {
		t.Fatalf("Err() = %v, want ErrConsumerTooSlow", got)
	}
	if got := c.State(); got != StateError {
		t.Errorf("State() = %v, want error", got)
	}
	var close1008s, sawTerminal int
	for _, ev := range ec.snapshot() {
		if ce, ok := ev.(*CloseError); ok && ce.Code == 1008 {
			close1008s++
		}
		if term, ok := ev.(*Terminal); ok && errors.Is(term.Err, ErrConsumerTooSlow) {
			sawTerminal++
		}
	}
	if close1008s != 1 {
		t.Errorf("*CloseError{1008} count = %d, want exactly 1 (the tipping close, surfaced once)", close1008s)
	}
	if sawTerminal != 1 {
		t.Errorf("*Terminal{ErrConsumerTooSlow} count = %d, want exactly 1", sawTerminal)
	}
	if got := c.Stats().BackpressureReconnects; got != 1 {
		t.Errorf("Stats.BackpressureReconnects = %d, want 1", got)
	}
}

// TestBackpressureCleanEpochResetsAfterParkedEpoch is the Finding-1 discriminator:
// episodesAtUp must be re-snapshotted per epoch. Epoch 1 parks and dies 1008
// (count=1); epoch 2 runs CLEAN (no park) and dies 1008 — its back-pressure witness
// must be false (parkEpisodes unchanged since ITS handshake), resetting the count, so
// the client reconnects forever rather than terminating. A stale episodesAtUp (not
// re-snapshotted) would carry epoch 1's park into epoch 2 and wrongly terminate a
// recovered consumer.
func TestBackpressureCleanEpochResetsAfterParkedEpoch(t *testing.T) {
	c, err := NewClient(context.Background(), "wss://example.test/ws",
		WithNoAuth(), WithQueueSize(117), WithHistoryLimit(1), WithMaxBackpressureReconnects(2),
		WithBackoff(BackoffConfig{Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2, Jitter: 0}))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	conn1 := &floodThenCloseConn{frame: []byte(`{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`)}
	tr := &fakeTransport{conns: []Conn{conn1, immediateCloseConn{}}}
	c.transport = tr

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c.delivery.waitUntilBlocked() // epoch 1 parked
	conn1.closeNow.Store(true)    // epoch 1 dies 1008 (count=1)
	collectEvents(c)              // drain so epoch 1 and every clean epoch make progress

	// Epoch 2+ (immediateCloseConn) are clean 1008 closes → reset → reconnect forever.
	// Wait until the client has opened past epoch 2 without terminating.
	deadline := time.After(3 * time.Second)
	for tr.openCount() < 3 {
		select {
		case <-deadline:
			t.Fatalf("openCount stuck at %d — a clean 1008 epoch failed to reset the run (wrongly terminated)", tr.openCount())
		case <-time.After(2 * time.Millisecond):
		}
	}
	if got := c.Err(); got != nil {
		t.Fatalf("Err() = %v, want nil (a clean 1008 epoch resets — never ErrConsumerTooSlow)", got)
	}
	if got := c.State(); got == StateError {
		t.Errorf("State() = error, want a live/reconnecting state")
	}
	if got := c.Stats().BackpressureReconnects; got != 1 {
		t.Errorf("Stats.BackpressureReconnects = %d, want 1 (only the parked epoch counted)", got)
	}
	closeClient(t, c)
}

// TestBackpressureParkedNon1008DoesNotCount discriminates the countsTowardBackpressure
// input in its TRUE-blocking direction: a parked epoch that dies with a LOCAL 1006
// (network, not the server's slow-client verdict) must NOT count toward the
// ErrConsumerTooSlow tally, even though back-pressure was witnessed this epoch — the
// client reconnects forever. Hardcoding the count flag true (or marking the 1006-local
// policy row as counting) would tip the freeze into a wrongful termination here.
func TestBackpressureParkedNon1008DoesNotCount(t *testing.T) {
	c, err := NewClient(context.Background(), "wss://example.test/ws",
		WithNoAuth(), WithQueueSize(117), WithHistoryLimit(1), WithMaxBackpressureReconnects(1),
		WithBackoff(BackoffConfig{Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 2, Jitter: 0}))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	conn1 := &floodThenLocalCloseConn{frame: []byte(`{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":{}}`)}
	// Epoch 2 (oneShotConn) stays open after one frame — a live reconnect that proves
	// the client survived the parked-then-1006 epoch rather than terminating.
	tr := &fakeTransport{conns: []Conn{
		conn1,
		&oneShotConn{frame: []byte(`{"type":"message","channel":"acme.x","seq":2,"ts":2,"data":{}}`)},
	}}
	c.transport = tr

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c.delivery.waitUntilBlocked() // epoch 1 parked
	conn1.closeNow.Store(true)    // epoch 1 dies LOCAL 1006 (parked, but non-1008)
	collectEvents(c)              // drain so the decode loop unparks and reads the close

	deadline := time.After(3 * time.Second)
	for tr.openCount() < 2 {
		select {
		case <-deadline:
			t.Fatalf("openCount stuck at %d — a parked non-1008 epoch failed to reconnect (wrongly terminated)", tr.openCount())
		case <-time.After(2 * time.Millisecond):
		}
	}
	if got := c.Err(); got != nil {
		t.Fatalf("Err() = %v, want nil (a parked non-1008 death freezes the count, never ErrConsumerTooSlow)", got)
	}
	if got := c.State(); got == StateError {
		t.Errorf("State() = error, want a live/reconnecting state")
	}
	if got := c.Stats().BackpressureReconnects; got != 0 {
		t.Errorf("Stats.BackpressureReconnects = %d, want 0 (a non-1008 close does not count)", got)
	}
	closeClient(t, c)
}
