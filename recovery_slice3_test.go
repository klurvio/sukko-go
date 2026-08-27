package sukko

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// waitForRecoveryInterrupted polls the collected events for a
// *RecoveryInterruptedError on the given channel.
func waitForRecoveryInterrupted(t *testing.T, ec *eventCollector, channel string) *RecoveryInterruptedError {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		for _, ev := range ec.snapshot() {
			if ri, ok := ev.(*RecoveryInterruptedError); ok && ri.Channel == channel {
				return ri
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for *RecoveryInterruptedError on channel %s", channel)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// subscribeAndReplay connects a client that subscribes t.a and receives a gap,
// so exactly one replay goes out and the channel is left REPLAYING (the fake never
// answers the replay). Returns the client, its fake clock, and its event collector.
func subscribeAndReplay(t *testing.T, f *fakeWS) (*Client, *fakeClock, *eventCollector) {
	t.Helper()
	c, fc := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForFrameCount(t, f, typeReplay, 1)
	return c, fc, ec
}

// TestRecoveryDeadlineInterruptsStuckReplay (T129/T139 — deadline trigger): a
// replay whose replay_complete never arrives is declared interrupted once the
// recovery deadline elapses, surfacing *RecoveryInterruptedError{replay, t.a}.
func TestRecoveryDeadlineInterruptsStuckReplay(t *testing.T) {
	f := newFakeWS(t)
	// ack + gap → replay sent; the replay is never answered (no replay_complete).
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":9,"ts":1}`,
		},
	}})
	c, fc, ec := subscribeAndReplay(t, f)
	defer closeClient(t, c)

	// The recovery deadline is the only pending wake for the REPLAYING channel.
	fc.BlockUntilTimer(purposeRecoveryDeadline)
	fc.Advance(DefaultRecoveryDeadline)

	ri := waitForRecoveryInterrupted(t, ec, "t.a")
	if ri.Kind != RecoveryKindReplay {
		t.Errorf("Kind = %q, want %q", ri.Kind, RecoveryKindReplay)
	}
}

// TestRecoveryReplayRejectionInterrupts (T139 — server-rejection trigger): a
// server `error` frame carrying the channel (a replay rejection) surfaces BOTH the
// raw *ReplayError (carrying the code) and the recovery-level
// *RecoveryInterruptedError.
func TestRecoveryReplayRejectionInterrupts(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":9,"ts":1}`,
		},
		// The replay is rejected with a per-channel error.
		typeReplay: {`{"type":"error","code":"offset_out_of_range","channel":"t.a","message":"gone"}`},
	}})
	c, _, ec := subscribeAndReplay(t, f)
	defer closeClient(t, c)

	ri := waitForRecoveryInterrupted(t, ec, "t.a")
	if ri.Kind != RecoveryKindReplay {
		t.Errorf("Kind = %q, want %q", ri.Kind, RecoveryKindReplay)
	}
	// The raw *ReplayError (with the server code) is surfaced too — the two events
	// carry different information (the code vs the recovery notice).
	var sawReplayErr bool
	for _, ev := range ec.snapshot() {
		if re, ok := ev.(*ReplayError); ok && re.Channel == "t.a" && re.Code == "offset_out_of_range" {
			sawReplayErr = true
		}
	}
	if !sawReplayErr {
		t.Error("no *ReplayError{offset_out_of_range, t.a} surfaced alongside the interrupt")
	}
}

// TestRecoveryEpochDeathInterruptsInFlightReplay (T139 — epoch-termination
// trigger): an ordinary epoch termination while a replay is in flight surfaces
// *RecoveryInterruptedError{replay, t.a} — the truncated recovery is typed, never a
// bare disconnect (spec §III) — while the channel still re-drives on the new epoch.
func TestRecoveryEpochDeathInterruptsInFlightReplay(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1: ack + gap → replay sent (frame 2: subscribe, replay); close after it, so
		// the channel dies mid-REPLAYING with no replay_complete.
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":9,"ts":1}`,
			}},
			closeAfterFrames: 2,
			closeAfter:       websocket.StatusCode(1011),
		},
		// E2: reconnect + resume (so the client re-drives the replay), then leave it.
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
			typeReplay:    {`{"type":"replay_complete","channel":"t.a","messages_replayed":2}`},
		}},
	)
	c, fc, ec := subscribeAndReplay(t, f)
	defer closeClient(t, c)

	// The epoch died mid-replay → the interrupt surfaces (from the stream-ordered reset).
	ri := waitForRecoveryInterrupted(t, ec, "t.a")
	if ri.Kind != RecoveryKindReplay {
		t.Errorf("Kind = %q, want %q", ri.Kind, RecoveryKindReplay)
	}

	// And recovery continues: the reconnect re-drives the replay on epoch 2 (T130).
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForFrameCount(t, f, typeReplay, 2)
	if f.framesOfType(typeReplay)[1].epoch != 2 {
		t.Errorf("re-driven replay on epoch %d, want 2", f.framesOfType(typeReplay)[1].epoch)
	}
}
