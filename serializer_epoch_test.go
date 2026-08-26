package sukko

import (
	"context"
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// subscribeFrameChannels decodes the `channels` list of a recorded `subscribe`
// frame, for asserting exactly which channels a resume/escalation re-subscribe put
// on the wire.
func subscribeFrameChannels(t *testing.T, fr recordedFrame) []string {
	t.Helper()
	var w wireSubscribe
	if err := json.Unmarshal([]byte(fr.raw), &w); err != nil {
		t.Fatalf("unmarshal subscribe frame %q: %v", fr.raw, err)
	}
	return w.Data.Channels
}

// TestSubscribeResumesDesiredOnReconnect covers the reconnect resume and the
// epoch-boundary accessors (T120): after the epoch dies Subscriptions() is empty
// and PendingSubscriptions() holds the whole desired set (granted was cleared);
// after the resume ack on the next epoch Subscriptions() is the re-granted set and
// PendingSubscriptions() the retained denial subset. The resume frame carries the
// whole desired set, not just the previously-granted channels.
func TestSubscribeResumesDesiredOnReconnect(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1 grants t.a (t.denied denied), then drops after answering the subscribe.
		epochScript{
			respond:    map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
			closeAfter: websocket.StatusCode(1011),
		},
		// E2 answers the resume subscribe the same way (t.denied stays denied).
		epochScript{
			respond: map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
		},
	)
	c, fc := subClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a", "t.denied"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 1) // E1 receives it and then drops

	// After the epoch reset the granted set is cleared; the whole desired set is
	// pending until the resume re-grants it.
	waitForCond(t, "granted cleared by the epoch reset", func() bool { return len(c.Subscriptions()) == 0 })
	if got := c.PendingSubscriptions(); !sortedEqual(got, []string{"t.a", "t.denied"}) {
		t.Errorf("PendingSubscriptions() after termination = %v, want the whole desired set [t.a t.denied]", got)
	}

	// Reconnect to E2: the resume re-subscribes the whole desired set.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)
	waitForFrameCount(t, f, typeSubscribe, 2)

	resume := f.framesOfType(typeSubscribe)[1]
	if resume.epoch != 2 {
		t.Errorf("resume subscribe on epoch %d, want 2", resume.epoch)
	}
	if got := subscribeFrameChannels(t, resume); !sortedEqual(got, []string{"t.a", "t.denied"}) {
		t.Errorf("resume subscribe channels = %v, want the whole desired set [t.a t.denied]", got)
	}

	// After the E2 ack the accessors settle to the re-granted set and the denial delta.
	waitForCond(t, "t.a re-granted on E2", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })
	if got := c.PendingSubscriptions(); !sortedEqual(got, []string{"t.denied"}) {
		t.Errorf("PendingSubscriptions() after resume = %v, want [t.denied]", got)
	}
}

// TestResumeServicedAfterRacingReset is the regression guard for the serviceResume
// stranded-resume wedge: a stale epoch reset and a resume signal arriving together
// must still re-subscribe the pending set, never leave the resume flag set with no
// server-side subscriptions. On a live, idle-connected serializer it injects the
// two signals back-to-back with GOMAXPROCS pinned to 1 — so both tokens buffer
// before the serializer can wake, guaranteeing its select sees BOTH ready and hits
// the ~50% coin-flip the real idle-reconnect path hits (which the fake-clock
// reconnect timing cannot reproduce — the serializer there drains the reset via the
// main select before the resume is enqueued). Under the old return-with-flag-
// retained serviceResume the coin-flip wedges ~half the time (no second frame);
// the retry-loop form services it every time. The internal loop makes a single
// `go test` run detect the wedge with probability ≈ 1 − 2^−iterations.
func TestResumeServicedAfterRacingReset(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	const iterations = 24
	for i := range iterations {
		f := newFakeWS(t)
		f.script(epochScript{respond: map[string][]string{
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}})
		c, _ := subClient(t, f)
		if err := c.Connect(context.Background()); err != nil {
			t.Fatalf("iter %d Connect: %v", i, err)
		}
		if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
			t.Fatalf("iter %d Subscribe: %v", i, err)
		}
		// The subscribe fully settles — slot released, serializer idle in its select.
		waitForCond(t, "t.a granted", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })
		waitForFrameCount(t, f, typeSubscribe, 1)

		// Inject a stale epoch reset immediately followed by a resume: with one P both
		// tokens buffer before the serializer wakes, so its select sees both ready. The
		// reset clears the grant; the resume must be serviced (re-subscribe the cleared
		// desired set on the still-live epoch), not stranded behind the reset the
		// coin-flip may make serviceResume drain.
		c.resetSubscribeSerializer()
		c.resumeSubscribeSerializer()

		waitForFrameCount(t, f, typeSubscribe, 2)
		waitForCond(t, "t.a re-granted after the racing reset+resume", func() bool {
			return sortedEqual(c.Subscriptions(), []string{"t.a"})
		})
		closeClient(t, c)
	}
}

// TestSubscribeResumesAfterIdleEpochReconnect covers the plain idle-epoch reconnect
// resume: the subscribe is fully acked (slot released, serializer idle) BEFORE the
// epoch drops, and the reconnect re-subscribes the desired set. (The reset/resume
// coin-flip that this path can hit is covered deterministically by
// TestResumeServicedAfterRacingReset — the fake-clock reconnect window here always
// drains the reset via the main select before E2's resume, so this test alone does
// not exercise the wedge.)
func TestSubscribeResumesAfterIdleEpochReconnect(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1 grants t.a and stays up (so the subscribe fully settles, serializer idle).
		epochScript{
			respond:          map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
			closeAfterFrames: 1,
			closeAfter:       websocket.StatusCode(1011),
		},
		// E2 answers the resume re-subscribe.
		epochScript{
			respond: map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
		},
	)
	c, fc := subClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// The subscribe is fully acked and the slot released (serializer idle) before the
	// epoch drops — E1 closes after the one subscribe frame, having answered it.
	waitForFrameCount(t, f, typeSubscribe, 1)

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)

	// The resume must re-subscribe the desired set on E2 (not wedge the flag).
	waitForFrameCount(t, f, typeSubscribe, 2)
	resume := f.framesOfType(typeSubscribe)[1]
	if resume.epoch != 2 {
		t.Errorf("resume subscribe on epoch %d, want 2", resume.epoch)
	}
	waitForCond(t, "t.a re-granted on E2", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })
}

// TestSubscribeInFlightRedrivenExactlyOnceOnReconnect covers T119's epoch-mid-flight
// leg: a subscribe that was outstanding when the epoch died is re-driven EXACTLY
// once on the next epoch — asserted by the frame count (one per epoch, never twice
// on the reconnect).
func TestSubscribeInFlightRedrivenExactlyOnceOnReconnect(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1 receives the subscribe, never acks it, then drops mid-flight.
		epochScript{closeAfterFrames: 1, closeAfter: websocket.StatusCode(1011)},
		// E2 acks the re-driven subscribe.
		epochScript{respond: map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}}},
	)
	c, fc := subClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 1) // E1 receives it (no ack), then drops

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)
	waitForFrameCount(t, f, typeSubscribe, 2) // re-driven on E2
	waitForCond(t, "t.a granted on E2", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })

	// Exactly two subscribe frames total — one per epoch — proving the in-flight
	// request was re-driven once, not duplicated.
	time.Sleep(50 * time.Millisecond)
	frames := f.framesOfType(typeSubscribe)
	if len(frames) != 2 {
		t.Fatalf("subscribe frames = %d, want exactly 2 (one per epoch)", len(frames))
	}
	if frames[0].epoch != 1 || frames[1].epoch != 2 {
		t.Errorf("subscribe frame epochs = [%d %d], want [1 2]", frames[0].epoch, frames[1].epoch)
	}
}

// TestEscalateResubscribesDenialSubsetOnly covers T120's escalation leg and T121's
// escalation-excludes-pruned leg: after a partial grant, an Escalate re-subscribes
// the DENIAL SUBSET ONLY (not the already-granted channels), and a denied channel
// the caller has since Unsubscribed is NOT in the escalation re-subscribe.
func TestEscalateResubscribesDenialSubsetOnly(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeAuth:      {`{"type":"auth_ack","data":{"exp":0}}`},
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}, // t.a granted; t.denied and t.gone denied
	}})
	c, _ := authClient(t, f, WithToken("jwt-orig"), WithRand(newFakeRand()), WithHeartbeatInterval(time.Hour))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a", "t.denied", "t.gone"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForCond(t, "t.a granted", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })
	waitForCond(t, "denials pending", func() bool { return sortedEqual(c.PendingSubscriptions(), []string{"t.denied", "t.gone"}) })

	// Unsubscribe a never-granted channel: pruned locally, no wire frame (both-set
	// pruning) — so it must not reappear in the escalation re-subscribe.
	if err := c.Unsubscribe(context.Background(), []string{"t.gone"}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	waitForCond(t, "t.gone pruned", func() bool { return sortedEqual(c.PendingSubscriptions(), []string{"t.denied"}) })

	// Escalate: its ack commits and re-subscribes the pending set — the denial subset
	// only, excluding the granted t.a and the pruned t.gone.
	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)      // the escalation frame
	waitForFrameCount(t, f, typeSubscribe, 2) // the escalation re-subscribe

	resub := f.framesOfType(typeSubscribe)[1]
	if got := subscribeFrameChannels(t, resub); !sortedEqual(got, []string{"t.denied"}) {
		t.Errorf("escalation re-subscribe channels = %v, want the denial subset only [t.denied]", got)
	}
}

// TestForcedUnsubscribeMovesChannelToPending covers T122's downgrade leg and the
// forced-unsubscribe granted-prune: a server-forced unsubscription_ack{forced:true}
// prunes the granted set ONLY (keeping desired), so the channel leaves
// Subscriptions() and reappears in PendingSubscriptions(); it does not release the
// serializer slot, but the channel's own subscription_ack already did, so the
// subscription surface is not wedged.
func TestForcedUnsubscribeMovesChannelToPending(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"unsubscription_ack","unsubscribed":["t.a"],"count":0,"forced":true}`,
		},
	}})
	c, _ := subClient(t, f)
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// t.a is granted then force-removed: it leaves Subscriptions() and, since desired
	// is retained, reappears in PendingSubscriptions().
	waitForCond(t, "t.a moved to pending by the forced removal", func() bool {
		return len(c.Subscriptions()) == 0 && sortedEqual(c.PendingSubscriptions(), []string{"t.a"})
	})

	// The forced ack surfaced *Unsubscribed{Forced:true} in receive order (delivered
	// asynchronously via the events channel, so poll rather than snapshot once).
	waitForCond(t, "*Unsubscribed{Forced:true} surfaced", func() bool {
		for _, ev := range ec.snapshot() {
			if u, ok := ev.(*Unsubscribed); ok && u.Forced {
				return true
			}
		}
		return false
	})

	// The forced ack did not release the slot, but the subscription_ack did — a
	// follow-up subscribe still reaches the wire (no wedge).
	if err := c.Subscribe(context.Background(), []string{"t.b"}); err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 2)
}
