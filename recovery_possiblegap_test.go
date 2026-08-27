package sukko

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// possibleGapsIn returns every *PossibleGap in the collected events, in order.
func possibleGapsIn(ec *eventCollector) []*PossibleGap {
	var out []*PossibleGap
	for _, ev := range ec.snapshot() {
		if pg, ok := ev.(*PossibleGap); ok {
			out = append(out, pg)
		}
	}
	return out
}

// waitForPossibleGap polls for the first *PossibleGap.
func waitForPossibleGap(t *testing.T, ec *eventCollector) *PossibleGap {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		if pgs := possibleGapsIn(ec); len(pgs) > 0 {
			return pgs[0]
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for a *PossibleGap")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// possibleGaps is the leaf-mutex tracker for the *PossibleGap snapshot lifecycle:
// at each epoch death the cursorless subset of the granted set unions into a pending
// set, drained (and emitted) when a reconnect completes or at teardown. These tests
// pin the union / cursorless-filter / clear-on-admission rules in isolation.

// TestPossibleGapsCursorlessUnion: add() unions only the granted channels that hold NO
// cursor (nothing to replay across the disconnect), and accumulates across calls.
func TestPossibleGapsCursorlessUnion(t *testing.T) {
	pg := newPossibleGaps()
	cur := newPosCursor()
	cur.advance("t.hascursor", "pos-1") // t.hascursor holds a cursor

	// Epoch A: granted {t.a, t.hascursor} → only t.a (cursorless) enters pending.
	pg.add([]string{"t.a", "t.hascursor"}, cur)
	// Epoch B: granted {t.b, t.hascursor} → union adds t.b.
	pg.add([]string{"t.b", "t.hascursor"}, cur)

	var got []string
	pg.drainIf(func(channels []string) bool {
		got = channels
		return true
	})
	if want := []string{"t.a", "t.b"}; !slices.Equal(got, want) {
		t.Errorf("drained = %v, want %v (cursorless union; t.hascursor excluded)", got, want)
	}
}

// TestPossibleGapsClearOnAdmission: drainIf clears the pending set ONLY when the emit
// succeeds; a failed emit retains it for a later drain (no silent loss).
func TestPossibleGapsClearOnAdmission(t *testing.T) {
	pg := newPossibleGaps()
	cur := newPosCursor()
	pg.add([]string{"t.a"}, cur)

	// A failed emit retains the pending set.
	if drained := pg.drainIf(func([]string) bool { return false }); drained {
		t.Error("drainIf reported drained on a failed emit")
	}
	// A second drain still sees t.a — nothing was lost.
	var got []string
	if !pg.drainIf(func(channels []string) bool { got = channels; return true }) {
		t.Fatal("drainIf did not drain a retained pending set")
	}
	if want := []string{"t.a"}; !slices.Equal(got, want) {
		t.Errorf("retained drain = %v, want %v", got, want)
	}
	// After a successful drain the set is empty — a further drain is a no-op.
	if pg.drainIf(func([]string) bool { t.Fatal("emit called on an empty pending set"); return true }) {
		t.Error("drainIf reported drained on an empty set")
	}
}

// TestPossibleGapDirectDegrade (T134 Direct row): a channel granted but cursorless
// (a message with no pos — the Direct backend) yields exactly one coalesced
// *PossibleGap{Channels} on reconnect, and the reconnect is answered with
// reconnect_ack{messages_replayed:0} (not not_available), per the real server.
func TestPossibleGapDirectDegrade(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"message","channel":"t.a","seq":1,"data":{"v":1}}`, // NO pos → cursorless
			}},
			closeAfter: websocket.StatusCode(1011),
		},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForMessages(t, ec, 1) // the no-pos message → t.a is cursorless

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)

	pg := waitForPossibleGap(t, ec)
	if !slices.Equal(pg.Channels, []string{"t.a"}) {
		t.Errorf("PossibleGap.Channels = %v, want [t.a]", pg.Channels)
	}
	// Exactly ONE *PossibleGap (coalesced, not per-channel) and no replay (Direct).
	time.Sleep(30 * time.Millisecond)
	if n := len(possibleGapsIn(ec)); n != 1 {
		t.Errorf("*PossibleGap count = %d, want exactly 1 (coalesced per reconnect)", n)
	}
	if n := len(f.framesOfType(typeReplay)); n != 0 {
		t.Errorf("replay frames = %d, want 0 (no cursor to replay from)", n)
	}
	if got := c.Stats().PossibleGaps; got != 1 {
		t.Errorf("Stats.PossibleGaps = %d, want 1 (counts emissions)", got)
	}
}

// TestPossibleGapKafkaNoGap (T134 Kafka row): a channel that held a cursor (a message
// WITH pos) is replayable across the reconnect — zero *PossibleGap.
func TestPossibleGapKafkaNoGap(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"message","channel":"t.a","seq":1,"pos":"1-100","data":{"v":1}}`, // pos → cursor held
			}},
			closeAfter: websocket.StatusCode(1011),
		},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForMessages(t, ec, 1)

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForFrameCount(t, f, typeReconnect, 1)
	waitForState(t, c, StateConnected)
	time.Sleep(30 * time.Millisecond)
	if n := len(possibleGapsIn(ec)); n != 0 {
		t.Errorf("*PossibleGap count = %d, want 0 (cursor held → replayable)", n)
	}
}

// TestPossibleGapNotAvailableDegrade (T134 not_available row): a
// reconnect_error{not_available} reaches the same degraded outcome — one
// *PossibleGap for the cursorless-granted snapshot.
func TestPossibleGapNotAvailableDegrade(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"message","channel":"t.a","seq":1,"data":{"v":1}}`, // cursorless
			}},
			closeAfter: websocket.StatusCode(1011),
		},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_error","code":"not_available","message":"direct"}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForMessages(t, ec, 1)

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	pg := waitForPossibleGap(t, ec)
	if !slices.Equal(pg.Channels, []string{"t.a"}) {
		t.Errorf("PossibleGap.Channels = %v, want [t.a]", pg.Channels)
	}
}

// TestPossibleGapNeverGrantedExcluded (T135): a channel that was DESIRED but never
// granted appears in no *PossibleGap — the snapshot is the granted set, not desired.
func TestPossibleGapNeverGrantedExcluded(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			// t.a is granted; t.denied is requested but the ack omits it (server filtered).
			respond:    map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
			closeAfter: websocket.StatusCode(1011),
		},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a", "t.denied"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForSubscriptionResult(t, ec)

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	pg := waitForPossibleGap(t, ec)
	// t.a is cursorless-granted → present; t.denied was never granted → absent.
	if !slices.Equal(pg.Channels, []string{"t.a"}) {
		t.Errorf("PossibleGap.Channels = %v, want [t.a] only (t.denied never granted)", pg.Channels)
	}
}

// TestPossibleGapUnanchorableGap (T135): a gap with an empty last_pos surfaces
// *Gap{LastPos:""} PLUS an immediate *PossibleGap for that channel, and drives ZERO
// replay frames.
func TestPossibleGapUnanchorableGap(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{typeSubscribe: {
		`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
		`{"type":"gap","channel":"t.a","last_pos":"","from_seq":1,"to_seq":9,"ts":1}`, // un-anchorable
	}}})
	c, _ := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	pg := waitForPossibleGap(t, ec)
	if !slices.Equal(pg.Channels, []string{"t.a"}) {
		t.Errorf("PossibleGap.Channels = %v, want [t.a]", pg.Channels)
	}
	var sawGap bool
	for _, ev := range ec.snapshot() {
		if g, ok := ev.(*Gap); ok && g.Channel == "t.a" && g.LastPos == "" {
			sawGap = true
		}
	}
	if !sawGap {
		t.Error("no *Gap{LastPos:\"\"} surfaced alongside the *PossibleGap")
	}
	time.Sleep(30 * time.Millisecond)
	if n := len(f.framesOfType(typeReplay)); n != 0 {
		t.Errorf("replay frames = %d, want 0 (an un-anchorable gap drives no replay)", n)
	}
}

// TestPossibleGapUnionAndClear (T135): union-on-retake across a failed reconnect
// dial, then clear-on-admission — the next clean reconnect emits none.
func TestPossibleGapUnionAndClear(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1: grant t.a cursorless, drop.
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"message","channel":"t.a","seq":1,"data":{"v":1}}`,
			}},
			closeAfter: websocket.StatusCode(1011),
		},
		// E2 (dial fails via upgrade rejection — set below), then E3 succeeds.
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("cid"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForMessages(t, ec, 1)

	// The reconnect dial fails once (rejected upgrade); the pending {t.a} persists.
	f.rejectUpgrade(503, `{"code":"UNAVAILABLE"}`)
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	// No *PossibleGap yet — no reconnect completed.
	if n := len(possibleGapsIn(ec)); n != 0 {
		t.Fatalf("*PossibleGap after a failed dial = %d, want 0 (nothing drained)", n)
	}
	// Clear the rejection; the next dial succeeds → union {t.a} emitted once.
	f.rejectUpgrade(0, "")
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial * 2) // attempt grew
	waitForDialCount(t, f, 3)
	pg := waitForPossibleGap(t, ec)
	if !slices.Equal(pg.Channels, []string{"t.a"}) {
		t.Errorf("PossibleGap.Channels = %v, want [t.a] (union-on-retake)", pg.Channels)
	}
	time.Sleep(30 * time.Millisecond)
	if n := len(possibleGapsIn(ec)); n != 1 {
		t.Errorf("*PossibleGap count = %d, want exactly 1 (cleared on admission)", n)
	}
}

// TestPossibleGapEmittedBeforeTerminal (T135): a client that dies with a pending
// cursorless-granted snapshot (reconnect disabled, so it never drains at a reconnect)
// still surfaces the *PossibleGap, and BEFORE the final *Terminal.
func TestPossibleGapEmittedBeforeTerminal(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{
		respond: map[string][]string{typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"message","channel":"t.a","seq":1,"data":{"v":1}}`, // cursorless
		}},
		closeAfter: websocket.StatusCode(1011),
	})
	c, _ := subClient(t, f, WithClientID("cid"), WithReconnect(false))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForMessages(t, ec, 1)
	ec.waitClosed(t) // the 1011 close with reconnect disabled → terminal

	events := ec.snapshot()
	pgIdx, termIdx := -1, -1
	for i, ev := range events {
		if pg, ok := ev.(*PossibleGap); ok && pgIdx == -1 {
			pgIdx = i
			if !slices.Equal(pg.Channels, []string{"t.a"}) {
				t.Errorf("terminal *PossibleGap.Channels = %v, want [t.a]", pg.Channels)
			}
		}
		if _, ok := ev.(*Terminal); ok {
			termIdx = i
		}
	}
	if pgIdx == -1 {
		t.Fatal("no *PossibleGap emitted for the pending snapshot at teardown")
	}
	if termIdx == -1 {
		t.Fatal("no *Terminal")
	}
	if pgIdx > termIdx {
		t.Errorf("*PossibleGap (idx %d) came AFTER *Terminal (idx %d); it must precede it", pgIdx, termIdx)
	}
}
