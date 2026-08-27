package sukko

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// historyPayloadOf decodes a recorded `history` frame's payload.
func historyPayloadOf(t *testing.T, fr recordedFrame) historyPayload {
	t.Helper()
	var w wireHistory
	if err := json.Unmarshal([]byte(fr.raw), &w); err != nil {
		t.Fatalf("unmarshal history frame %q: %v", fr.raw, err)
	}
	return w.Data
}

// waitForEvent polls the collected events for the first one satisfying match.
func waitForEvent(t *testing.T, ec *eventCollector, what string, match func(Event) bool) Event {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		for _, ev := range ec.snapshot() {
			if match(ev) {
				return ev
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// connectedHistoryClient connects a client subscribed to t.a, ready to issue History.
func connectedHistoryClient(t *testing.T, f *fakeWS) (*Client, *fakeClock, *eventCollector) {
	t.Helper()
	c, fc := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForSubscriptionResult(t, ec)
	return c, fc, ec
}

// historyFlight is the client-level single-flight slot for History() — a leaf-lock
// shared between the caller (claim), the decode loop (channel-matched release), and
// the recovery owner (deadline/epoch-death interrupt). These tests pin its guards in
// isolation; `*epoch` values are identity tokens (never dereferenced).

// TestHistoryFlightSingleFlight: one history at a time — a second claim fails while
// one is in flight, and succeeds after the matching release.
func TestHistoryFlightSingleFlight(t *testing.T) {
	var h historyFlight
	e := &epoch{}

	if !h.claim("t.a", e, fsmBase.Add(10*time.Second), 0) {
		t.Fatal("first claim failed")
	}
	if h.claim("t.b", e, fsmBase.Add(10*time.Second), 0) {
		t.Error("second claim succeeded while a history was in flight (single-flight violated)")
	}
	if !h.releaseIfChannel("t.a") {
		t.Error("releaseIfChannel(t.a) did not release the matching flight")
	}
	if !h.claim("t.b", e, fsmBase.Add(10*time.Second), 0) {
		t.Error("claim after release failed")
	}
}

// TestHistoryFlightReleaseIsChannelMatched pins H1: a terminator for a DIFFERENT
// channel must not release the slot — otherwise a stale/duplicate history_complete
// for channel B would free channel A's flight, letting a second History run and put
// 2×HistoryLimit records on the delivery channel (the exact hazard single-flight
// exists to prevent).
func TestHistoryFlightReleaseIsChannelMatched(t *testing.T) {
	var h historyFlight
	e := &epoch{}
	h.claim("t.a", e, fsmBase.Add(10*time.Second), 0)

	if h.releaseIfChannel("t.b") {
		t.Error("releaseIfChannel(t.b) released a t.a flight (H1: release must be channel-matched)")
	}
	if h.claim("t.c", e, fsmBase.Add(10*time.Second), 0) {
		t.Error("the slot was freed by a foreign-channel release; single-flight is broken")
	}
}

// TestHistoryFlightReleaseIfIsIdentityMatched pins the send-failure release fix: a
// blockable send can be interrupted by the owner (deadline/epoch-death) and the slot
// re-claimed by a fresh History on another epoch while the caller is parked in Send.
// releaseIf must therefore free ONLY the exact (channel, epoch) the caller claimed —
// never an unrelated flight — or single-flight breaks (two histories, 2×HistoryLimit).
func TestHistoryFlightReleaseIfIsIdentityMatched(t *testing.T) {
	var h historyFlight
	e1, e2 := &epoch{}, &epoch{}
	h.claim("t.a", e1, fsmBase, 0)

	// The slot now holds a DIFFERENT flight (re-claimed on e2 after this caller's was
	// interrupted). This caller's send-fail release for (t.a, e1) must NOT free it.
	h.releaseIf("t.a", e1)         // frees the t.a/e1 flight (it is the current one)
	h.claim("t.b", e2, fsmBase, 0) // a fresh flight on e2
	if h.releaseIf("t.a", e1) {    // a straggler release for the old (t.a, e1)
		t.Error("releaseIf(t.a, e1) freed a t.b/e2 flight; release must be identity-matched (channel AND epoch)")
	}
	// A different epoch alone is enough to reject.
	if h.releaseIf("t.b", e1) {
		t.Error("releaseIf(t.b, e1) freed a t.b/e2 flight; the epoch must match too")
	}
	// The exact (channel, epoch) frees it.
	if !h.releaseIf("t.b", e2) {
		t.Error("releaseIf(t.b, e2) did not free its own flight")
	}
}

// TestHistoryFlightDeadlineInterrupts: a history whose history_complete never
// arrives is interrupted once the deadline elapses under a live epoch with a healthy
// consumer, and the slot is released.
func TestHistoryFlightDeadlineInterrupts(t *testing.T) {
	var h historyFlight
	e := &epoch{}
	h.claim("t.a", e, fsmBase.Add(10*time.Second), 0)

	if ch := h.due(tick{now: fsmBase.Add(9 * time.Second), current: e}, 10*time.Second); ch != "" {
		t.Fatalf("due before the deadline = %q, want empty", ch)
	}
	if ch := h.due(tick{now: fsmBase.Add(10 * time.Second), current: e}, 10*time.Second); ch != "t.a" {
		t.Fatalf("due at the deadline = %q, want t.a", ch)
	}
	if !h.claim("t.b", e, fsmBase, 0) {
		t.Error("slot not released after the deadline interrupt")
	}
}

// TestHistoryFlightDeadlineParkSuspension: the history deadline measures server
// silence — a back-pressure episode during the window suspends it (re-arm, no fire);
// a clean window then fires. Both sides.
func TestHistoryFlightDeadlineParkSuspension(t *testing.T) {
	var h historyFlight
	e := &epoch{}
	h.claim("t.a", e, fsmBase.Add(10*time.Second), 0) // armEpisodes=0

	if ch := h.due(tick{now: fsmBase.Add(10 * time.Second), current: e, parked: true}, 10*time.Second); ch != "" {
		t.Fatalf("due while parked = %q, want empty (suspended)", ch)
	}
	if ch := h.due(tick{now: fsmBase.Add(20 * time.Second), current: e, episodes: 1}, 10*time.Second); ch != "" {
		t.Fatalf("due after a park episode = %q, want empty (suspended)", ch)
	}
	if ch := h.due(tick{now: fsmBase.Add(30 * time.Second), current: e, episodes: 1}, 10*time.Second); ch != "t.a" {
		t.Fatalf("due after a clean window = %q, want t.a", ch)
	}
}

// TestHistoryFlightEpochDeathInterrupts: an in-flight history is interrupted when its
// epoch is no longer current (a reconnect) — history does not resume across an epoch
// boundary. The prompt reset path and the deadline path both interrupt; a same-epoch
// check is a no-op.
func TestHistoryFlightEpochDeathInterrupts(t *testing.T) {
	var h historyFlight
	e := &epoch{}
	h.claim("t.a", e, fsmBase.Add(10*time.Second), 0)

	// Reset path: the epoch changed → interrupt + release.
	if ch := h.interruptIfEpochDead(&epoch{}); ch != "t.a" {
		t.Fatalf("interruptIfEpochDead(other epoch) = %q, want t.a", ch)
	}
	if !h.claim("t.b", e, fsmBase.Add(10*time.Second), 0) {
		t.Fatal("slot not released after the epoch-death interrupt")
	}
	// A same-epoch reset (no epoch change) does not interrupt the current flight.
	if ch := h.interruptIfEpochDead(e); ch != "" {
		t.Errorf("interruptIfEpochDead(same epoch) = %q, want empty", ch)
	}

	// due() also interrupts on epoch death (disconnected) — the backstop when the
	// deadline elapses before the reset is processed. This is the history/replay
	// asymmetry: a replay would SUSPEND while disconnected; a history interrupts.
	if ch := h.due(tick{now: fsmBase.Add(10 * time.Second), current: nil}, 10*time.Second); ch != "t.b" {
		t.Fatalf("due while disconnected = %q, want t.b (history cannot resume across a reconnect)", ch)
	}
}

// TestHistoryRequestDelivers: History() emits history{channel, limit}, the records
// arrive as SourceHistory *Message events, and *HistoryComplete terminates them.
func TestHistoryRequestDelivers(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		typeHistory: {
			`{"type":"message","channel":"t.a","seq":1,"pos":"h-1","history":true,"data":{"n":1}}`,
			`{"type":"history_complete","channel":"t.a","count":1,"source":"kafka"}`,
		},
	}})
	c, _, ec := connectedHistoryClient(t, f)
	defer closeClient(t, c)

	if err := c.History(context.Background(), "t.a", 50); err != nil {
		t.Fatalf("History: %v", err)
	}
	waitForFrameCount(t, f, typeHistory, 1)
	if p := historyPayloadOf(t, f.framesOfType(typeHistory)[0]); p.Channel != "t.a" || p.Limit != 50 {
		t.Errorf("history payload = %+v, want {t.a, 50}", p)
	}
	m := waitForEvent(t, ec, "SourceHistory message", func(ev Event) bool {
		msg, ok := ev.(*Message)
		return ok && msg.Seq == 1
	}).(*Message)
	if m.Source != SourceHistory {
		t.Errorf("history record Source = %v, want SourceHistory", m.Source)
	}
	waitForEvent(t, ec, "*HistoryComplete", func(ev Event) bool {
		hc, ok := ev.(*HistoryComplete)
		return ok && hc.Channel == "t.a" && hc.Count == 1
	})
}

// TestHistorySingleFlightWire: a second History while one is in flight (no terminator
// yet) returns ErrHistoryInProgress; the first's frame carried the effective limit.
func TestHistorySingleFlightWire(t *testing.T) {
	f := newFakeWS(t)
	// history is withheld (no terminator), so the first stays in flight.
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
	}})
	c, _, _ := connectedHistoryClient(t, f)
	defer closeClient(t, c)

	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Fatalf("first History: %v", err)
	}
	waitForFrameCount(t, f, typeHistory, 1)
	if err := c.History(context.Background(), "t.a", 10); !errors.Is(err, ErrHistoryInProgress) {
		t.Errorf("second History = %v, want ErrHistoryInProgress", err)
	}
}

// TestHistorySingleFlightReleasesOnComplete: after a history_complete a second
// History succeeds — the single-flight slot was released.
func TestHistorySingleFlightReleasesOnComplete(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		typeHistory:   {`{"type":"history_complete","channel":"t.a","count":0,"source":"kafka"}`},
	}})
	c, _, ec := connectedHistoryClient(t, f)
	defer closeClient(t, c)

	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Fatalf("first History: %v", err)
	}
	waitForEvent(t, ec, "*HistoryComplete", func(ev Event) bool {
		_, ok := ev.(*HistoryComplete)
		return ok
	})
	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Errorf("second History after completion = %v, want nil (slot released)", err)
	}
}

// TestHistoryDeadlineInterrupts: a history whose history_complete never arrives is
// interrupted after the recovery deadline, surfacing *RecoveryInterruptedError with
// Kind=history, and the slot is released (a second History succeeds).
func TestHistoryDeadlineInterrupts(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
	}}) // history withheld
	c, fc, ec := connectedHistoryClient(t, f)
	defer closeClient(t, c)

	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Fatalf("History: %v", err)
	}
	waitForFrameCount(t, f, typeHistory, 1)

	fc.BlockUntilTimer(purposeRecoveryDeadline)
	fc.Advance(DefaultRecoveryDeadline)

	ri := waitForEvent(t, ec, "history *RecoveryInterruptedError", func(ev Event) bool {
		r, ok := ev.(*RecoveryInterruptedError)
		return ok && r.Channel == "t.a"
	}).(*RecoveryInterruptedError)
	if ri.Kind != RecoveryKindHistory {
		t.Errorf("Kind = %q, want %q", ri.Kind, RecoveryKindHistory)
	}
	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Errorf("second History after the deadline = %v, want nil (slot released)", err)
	}
}

// TestHistoryEpochDeathInterrupts: an epoch dying mid-history surfaces
// *RecoveryInterruptedError{history} and does NOT re-drive (history is one-shot).
func TestHistoryEpochDeathInterrupts(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1: grant t.a; the history frame is the 2nd client frame; close after it.
		epochScript{
			respond:          map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
			closeAfterFrames: 2,
			closeAfter:       websocket.StatusCode(1011),
		},
		// E2: reconnect + resume, but no history is re-sent.
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}},
	)
	c, fc, ec := connectedHistoryClient(t, f)
	defer closeClient(t, c)

	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Fatalf("History: %v", err)
	}
	waitForFrameCount(t, f, typeHistory, 1)

	ri := waitForEvent(t, ec, "history *RecoveryInterruptedError", func(ev Event) bool {
		r, ok := ev.(*RecoveryInterruptedError)
		return ok && r.Channel == "t.a"
	}).(*RecoveryInterruptedError)
	if ri.Kind != RecoveryKindHistory {
		t.Errorf("Kind = %q, want %q", ri.Kind, RecoveryKindHistory)
	}

	// Reconnect to E2 and confirm the history did NOT re-drive (one frame total).
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)
	time.Sleep(30 * time.Millisecond) // let any spurious re-send appear
	if n := len(f.framesOfType(typeHistory)); n != 1 {
		t.Errorf("history frames = %d, want 1 (history does not re-drive across a reconnect)", n)
	}
}

// TestHistoryErrorIsTerminatorNotInterrupt: a history_error ends the single-flight
// and surfaces *HistoryError, but NOT a *RecoveryInterruptedError — a refused history
// simply ended (the deliberate asymmetry with a replay rejection).
func TestHistoryErrorIsTerminatorNotInterrupt(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		typeHistory:   {`{"type":"history_error","code":"history_disabled","channel":"t.a","message":"nope"}`},
	}})
	c, _, ec := connectedHistoryClient(t, f)
	defer closeClient(t, c)

	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Fatalf("History: %v", err)
	}
	waitForEvent(t, ec, "*HistoryError", func(ev Event) bool {
		he, ok := ev.(*HistoryError)
		return ok && he.Channel == "t.a" && he.Code == "history_disabled"
	})
	// The slot released → a second History succeeds; this also fences the snapshot.
	if err := c.History(context.Background(), "t.a", 10); err != nil {
		t.Errorf("second History after history_error = %v, want nil (slot released)", err)
	}
	for _, ev := range ec.snapshot() {
		if _, ok := ev.(*RecoveryInterruptedError); ok {
			t.Error("history_error surfaced a *RecoveryInterruptedError; it is a terminator, not an interrupt")
		}
	}
}

// TestHistoryLifecycleErrors: the return-value table — limit overflow, not connected,
// and closed.
func TestHistoryLifecycleErrors(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c, _ := subClient(t, f, WithClientID("cid")) // not connected

	// Limit overflow is checked before state (default HistoryLimit is 100).
	if err := c.History(context.Background(), "t.a", MaxHistoryLimit+1); !errors.Is(err, ErrHistoryLimitExceeded) {
		t.Errorf("History(limit>max) = %v, want ErrHistoryLimitExceeded", err)
	}
	// Not connected → *NotConnectedError.
	var nce *NotConnectedError
	if err := c.History(context.Background(), "t.a", 10); !errors.As(err, &nce) {
		t.Errorf("History while disconnected = %v, want *NotConnectedError", err)
	}
	closeClient(t, c)
	if err := c.History(context.Background(), "t.a", 10); !errors.Is(err, ErrClosed) {
		t.Errorf("History after Close = %v, want ErrClosed", err)
	}
}
