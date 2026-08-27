package sukko

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// replayPayloadOf decodes a recorded `replay` frame's payload for assertions on
// channel and from_pos.
func replayPayloadOf(t *testing.T, fr recordedFrame) replayPayload {
	t.Helper()
	var w wireReplay
	if err := json.Unmarshal([]byte(fr.raw), &w); err != nil {
		t.Fatalf("unmarshal replay frame %q: %v", fr.raw, err)
	}
	return w.Data
}

// TestGapDrivesReplay is the basic wire path (T127): a server gap advisory on a
// subscribed channel, with the floor open, drives exactly one replay{from_pos =
// the gap's last_pos}, and bumps the gaps/replays counters.
func TestGapDrivesReplay(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":4,"ts":1}`,
		},
		typeReplay: {`{"type":"replay_complete","channel":"t.a","messages_replayed":3}`},
	}})
	c, _ := subClient(t, f, WithClientID("cid"))
	collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	waitForFrameCount(t, f, typeReplay, 1)
	frames := f.framesOfType(typeReplay)
	if p := replayPayloadOf(t, frames[0]); p.Channel != "t.a" || p.FromPos != "g-1" {
		t.Errorf("replay payload = %+v, want {t.a, g-1}", p)
	}
	if got := c.Stats(); got.Gaps != 1 || got.Replays != 1 {
		t.Errorf("Stats = {Gaps:%d Replays:%d}, want {1 1}", got.Gaps, got.Replays)
	}
}

// TestFollowupReplayFiresAfterFloor drives the follow-up path end-to-end through
// the real replay-floor timer (T128/T137): a gap while REPLAYING is retained, and
// its replay fires only once the per-channel floor elapses — proving the owner's
// single floor timer (purposeReplayFloor) drives the deferred replay, and that the
// retained anchor is the first-arrived follow-up, not a later one.
func TestFollowupReplayFiresAfterFloor(t *testing.T) {
	f := newFakeWS(t)
	// ack, then two gaps back-to-back: g-1 replays immediately (floor open), g-2
	// lands while REPLAYING and is retained as the follow-up. The replay reply is
	// only replay_complete (no gap), so nothing re-triggers.
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":2,"ts":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-2","from_seq":3,"to_seq":4,"ts":2}`,
		},
		typeReplay: {`{"type":"replay_complete","channel":"t.a","messages_replayed":1}`},
	}})
	c, fc := subClient(t, f, WithClientID("cid"))
	collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// The immediate replay (g-1) goes out; its replay_complete then floor-gates the
	// follow-up (g-2) — no second replay until the floor elapses.
	waitForFrameCount(t, f, typeReplay, 1)
	if p := replayPayloadOf(t, f.framesOfType(typeReplay)[0]); p.FromPos != "g-1" {
		t.Fatalf("first replay from_pos = %q, want g-1", p.FromPos)
	}

	// The follow-up is parked in FLOOR_WAIT: advancing the floor timer fires it.
	fc.BlockUntilTimer(purposeReplayFloor)
	fc.Advance(DefaultReplayFloor)
	waitForFrameCount(t, f, typeReplay, 2)
	if p := replayPayloadOf(t, f.framesOfType(typeReplay)[1]); p.FromPos != "g-2" {
		t.Errorf("follow-up replay from_pos = %q, want g-2", p.FromPos)
	}
}

// TestReplayRedrivenAcrossReconnect is the core re-drive test (T130): a replay in
// flight when the epoch dies is re-driven on the new epoch — but only AFTER the
// resume subscribe re-grants the channel (the server rejects a replay for an
// unsubscribed channel), and from the SAME anchor. It asserts the re-driven replay
// lands on epoch 2 and carries the original gap's last_pos.
func TestReplayRedrivenAcrossReconnect(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1: grant t.a, then a gap → the client sends replay(g-1); E1 closes after
		// receiving that replay (frame 2: subscribe, replay), so the channel dies
		// mid-REPLAYING with no replay_complete.
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":2,"ts":1}`,
			}},
			closeAfterFrames: 2,
			closeAfter:       websocket.StatusCode(1011),
		},
		// E2: reconnect, resume subscribe re-grants t.a → the retained replay is
		// re-driven; the re-driven replay completes.
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
			typeReplay:    {`{"type":"replay_complete","channel":"t.a","messages_replayed":2}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("cid"))
	collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// E1 sent one replay before dying.
	waitForFrameCount(t, f, typeReplay, 1)

	// Reconnect to E2; the re-drive lands after the resume subscribe re-grants t.a.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForFrameCount(t, f, typeReplay, 2)

	redrive := f.framesOfType(typeReplay)[1]
	if redrive.epoch != 2 {
		t.Errorf("re-driven replay on epoch %d, want 2", redrive.epoch)
	}
	if p := replayPayloadOf(t, redrive); p.FromPos != "g-1" {
		t.Errorf("re-driven replay from_pos = %q, want g-1 (the retained anchor)", p.FromPos)
	}
}

// TestRedriveUsesAnchorNotCursor is the discriminating test for T142: between the
// gap and the epoch death, live traffic advances the pos cursor past the gapped
// window. On reconnect the RECONNECT frame carries the (advanced) cursor, but the
// re-driven REPLAY must carry the gap's own anchor — never the cursor, which would
// silently skip the lost records. Re-driving from the cursor fails this test.
func TestRedriveUsesAnchorNotCursor(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":2,"ts":1}`,
				// A live record AFTER the gap advances the cursor to L-9.
				`{"type":"message","channel":"t.a","seq":9,"pos":"L-9","data":{"v":1}}`,
			}},
			// subscribe(1), replay(2) → close after the replay so the channel dies
			// mid-REPLAYING with the cursor sitting at L-9.
			closeAfterFrames: 2,
			closeAfter:       websocket.StatusCode(1011),
		},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
			typeReplay:    {`{"type":"replay_complete","channel":"t.a","messages_replayed":2}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("cid"))
	collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForFrameCount(t, f, typeReplay, 1)

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForFrameCount(t, f, typeReplay, 2)

	// The reconnect carries the advanced cursor…
	waitForFrameCount(t, f, typeReconnect, 1)
	if got := reconnectPayloadOf(t, f.framesOfType(typeReconnect)[0]).LastPos["t.a"]; got != "L-9" {
		t.Errorf("reconnect last_pos[t.a] = %q, want L-9 (the live cursor)", got)
	}
	// …but the re-driven replay carries the gap's anchor, not the cursor.
	if p := replayPayloadOf(t, f.framesOfType(typeReplay)[1]); p.FromPos != "g-1" {
		t.Errorf("re-driven replay from_pos = %q, want g-1 (the gap anchor, NOT the cursor L-9)", p.FromPos)
	}
}

// panicOnFloorClock panics when the recovery owner arms its replay-floor timer —
// the injectable seam for forcing a panic on the recovery-owner goroutine. Every
// other timer purpose delegates unchanged.
type panicOnFloorClock struct{ *fakeClock }

func (c panicOnFloorClock) NewTimer(d time.Duration, p timerPurpose) Timer {
	if p == purposeReplayFloor {
		panic("clock boom: replay floor timer")
	}
	return c.fakeClock.NewTimer(d, p)
}

// TestRecoveryOwnerPanicTerminates: a panic on the recovery-owner goroutine (here
// arming the floor timer for a floor-gated follow-up) must terminate the client
// with an *InternalError{Op: "recovery-owner"} and land it in StateError — never
// leave a silently-dead owner (ADR-0010), mirroring the auth-owner/serializer
// contract.
func TestRecoveryOwnerPanicTerminates(t *testing.T) {
	f := newFakeWS(t)
	// ack + two gaps: g-1 replays immediately (REPLAYING), g-2 is retained as the
	// follow-up. The replay_complete then floor-gates g-2 → the owner arms the floor
	// timer, where the clock panics.
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":2,"ts":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-2","from_seq":3,"to_seq":4,"ts":2}`,
		},
		typeReplay: {`{"type":"replay_complete","channel":"t.a","messages_replayed":1}`},
	}})
	fc := panicOnFloorClock{newFakeClock()}
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithNoAuth(), WithClientID("cid"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ec.waitClosed(t)
	var ie *InternalError
	if !errors.As(c.Err(), &ie) {
		t.Fatalf("Err() = %v, want an *InternalError from the owner panic", c.Err())
	}
	if ie.Op != "recovery-owner" {
		t.Errorf("InternalError.Op = %q, want recovery-owner", ie.Op)
	}
	if got := c.State(); got != StateError {
		t.Errorf("State() = %v, want error (owner panic is a terminal failure)", got)
	}
}
