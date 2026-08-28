package sukko

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// reconnectPayloadOf decodes a recorded `reconnect` frame's payload for assertions
// on client_id and last_pos.
func reconnectPayloadOf(t *testing.T, fr recordedFrame) reconnectPayload {
	t.Helper()
	var w wireReconnect
	if err := json.Unmarshal([]byte(fr.raw), &w); err != nil {
		t.Fatalf("unmarshal reconnect frame %q: %v", fr.raw, err)
	}
	return w.Data
}

// waitForMessages polls the collected events for at least n *Message events and
// returns them in arrival order.
func waitForMessages(t *testing.T, ec *eventCollector, n int) []*Message {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		var msgs []*Message
		for _, ev := range ec.snapshot() {
			if m, ok := ev.(*Message); ok {
				msgs = append(msgs, m)
			}
		}
		if len(msgs) >= n {
			return msgs
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d *Message events (have %d)", n, len(msgs))
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestPosCursorRules pins the cursor rules directly (T125/T140): advance-on-live
// overwrites, history seeds only when absent and never overwrites, and an empty pos
// anchors nothing.
func TestPosCursorRules(t *testing.T) {
	p := newPosCursor()

	p.advance("c", "live-1")
	if got := p.snapshot()["c"]; got != "live-1" {
		t.Fatalf("advance: c = %q, want live-1", got)
	}
	// A history record must NOT overwrite an existing (live) cursor.
	p.seedIfAbsent("c", "hist-x")
	if got := p.snapshot()["c"]; got != "live-1" {
		t.Errorf("seedIfAbsent over a live cursor: c = %q, want live-1 (unchanged)", got)
	}
	// A history record on a cursorless channel seeds it.
	p.seedIfAbsent("d", "hist-1")
	if got := p.snapshot()["d"]; got != "hist-1" {
		t.Errorf("seedIfAbsent on cursorless d = %q, want hist-1", got)
	}
	// A later live record overwrites the seed (live is always newer).
	p.advance("d", "live-2")
	if got := p.snapshot()["d"]; got != "live-2" {
		t.Errorf("advance over a seed: d = %q, want live-2", got)
	}
	// An empty pos (Direct backend) anchors nothing.
	p.advance("e", "")
	if _, ok := p.snapshot()["e"]; ok {
		t.Error("advance with empty pos created a cursor for e")
	}
}

// TestReconnectFrameCarriesClientIDAndCursor covers the reconnect send (T126): the
// FIRST connect sends no reconnect frame; after a live message advances the cursor,
// the reconnect on the next epoch carries the configured client_id and the
// live-derived last_pos.
func TestReconnectFrameCarriesClientIDAndCursor(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"message","channel":"t.a","seq":1,"pos":"1-100","data":{"v":1}}`,
			}},
			closeAfter: websocket.StatusCode(1011),
		},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
			typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		}},
	)
	c, fc := subClient(t, f, WithClientID("test-client-42"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForMessages(t, ec, 1) // the live message was delivered → cursor advanced

	// Reconnect to E2; the reconnect frame carries the resume identity and cursor.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)
	waitForFrameCount(t, f, typeReconnect, 1)

	frames := f.framesOfType(typeReconnect)
	if len(frames) != 1 {
		t.Fatalf("reconnect frames = %d, want exactly 1 (none on the first connect)", len(frames))
	}
	if frames[0].epoch != 2 {
		t.Errorf("reconnect frame on epoch %d, want 2", frames[0].epoch)
	}
	p := reconnectPayloadOf(t, frames[0])
	if p.ClientID != "test-client-42" {
		t.Errorf("reconnect client_id = %q, want test-client-42", p.ClientID)
	}
	if got := p.LastPos["t.a"]; got != "1-100" {
		t.Errorf("reconnect last_pos[t.a] = %q, want 1-100 (the live cursor)", got)
	}

	// F1: reconnect{last_pos} MUST lead the resume subscribe on the wire (FR-006) —
	// the reconnect is sent before setConn publishes the conn to the serializer's send
	// gate. Assert the order on epoch 2's received frames.
	waitForFrameCount(t, f, typeSubscribe, 2) // the E2 resume subscribe
	all := f.frames()
	reconnectIdx, subscribeIdx := -1, -1
	for i, fr := range all {
		if fr.epoch != 2 {
			continue
		}
		if fr.typ == typeReconnect && reconnectIdx == -1 {
			reconnectIdx = i
		}
		if fr.typ == typeSubscribe && subscribeIdx == -1 {
			subscribeIdx = i
		}
	}
	if reconnectIdx == -1 || subscribeIdx == -1 {
		t.Fatalf("epoch-2 frames incomplete: reconnectIdx=%d subscribeIdx=%d", reconnectIdx, subscribeIdx)
	}
	if reconnectIdx > subscribeIdx {
		t.Errorf("reconnect frame (idx %d) came AFTER the resume subscribe (idx %d); reconnect must lead", reconnectIdx, subscribeIdx)
	}
}

// TestReplayWindowNormalizesSource covers the reconnect-replay window (T126/T143): a
// live-shaped `message` arriving before reconnect_ack is normalized to SourceReplay
// and does NOT advance the cursor; a message after the ack is SourceLive; *Resumed is
// emitted; and the cursor a later reconnect sends reflects only the live record.
func TestReplayWindowNormalizesSource(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		// E1 grants t.a, then drops (nothing delivered → cursor empty).
		epochScript{
			respond:    map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
			closeAfter: websocket.StatusCode(1011),
		},
		// E2: the reconnect is answered with a replayed record THEN reconnect_ack; the
		// resume subscribe is answered with its ack THEN a live record; then E2 drops
		// (after both inbound frames) so the test can capture E3's reconnect cursor.
		epochScript{
			respond: map[string][]string{
				typeReconnect: {
					`{"type":"message","channel":"t.a","seq":1,"pos":"9-1","data":{"r":1}}`, // in-window ⇒ SourceReplay
					`{"type":"reconnect_ack","status":"completed","messages_replayed":1}`,
				},
				typeSubscribe: {
					`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
					`{"type":"message","channel":"t.a","seq":2,"pos":"9-2","data":{"l":1}}`, // post-ack ⇒ SourceLive
				},
			},
			closeAfterFrames: 2, // close after answering both the reconnect and the resume subscribe
			closeAfter:       websocket.StatusCode(1011),
		},
		// E3 exists only to capture the reconnect frame's last_pos.
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

	// Reconnect to E2 and let the replay + live records flow.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	msgs := waitForMessages(t, ec, 2)

	// The replayed record is SourceReplay, the post-ack one SourceLive.
	var replay, live *Message
	for _, m := range msgs {
		switch m.Pos {
		case "9-1":
			replay = m
		case "9-2":
			live = m
		}
	}
	if replay == nil || replay.Source != SourceReplay {
		t.Errorf("in-window record: got %+v, want Source=SourceReplay", replay)
	}
	if live == nil || live.Source != SourceLive {
		t.Errorf("post-ack record: got %+v, want Source=SourceLive", live)
	}
	// *Resumed surfaced from reconnect_ack.
	var sawResumed bool
	for _, ev := range ec.snapshot() {
		if r, ok := ev.(*Resumed); ok && r.MessagesReplayed == 1 {
			sawResumed = true
		}
	}
	if !sawResumed {
		t.Error("no *Resumed{MessagesReplayed:1} surfaced from reconnect_ack")
	}

	// Reconnect again to E3: the cursor it carries must be the LIVE pos (9-2), never
	// the replayed one (9-1) — a replayed record must not advance the cursor.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 3)
	waitForFrameCount(t, f, typeReconnect, 2)
	last := f.framesOfType(typeReconnect)[1]
	if got := reconnectPayloadOf(t, last).LastPos["t.a"]; got != "9-2" {
		t.Errorf("cursor after replay = %q, want 9-2 (only the live record advances it)", got)
	}
}

// TestReconnectNotSentOnFirstConnectAfterFailedDial is the F3 regression guard: a
// failed first dial reports Connect but establishes no epoch, so the first
// SUCCESSFUL connect — even though it is the SECOND dial — must send NO reconnect
// frame (there is nothing to resume). It discriminates the hadEpoch gate from the
// old !wasFirst gate, which would (wrongly) send one.
func TestReconnectNotSentOnFirstConnectAfterFailedDial(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	f.rejectUpgrade(429, `{"code":"TENANT_LIMIT_EXCEEDED","message":"too many"}`)
	c, fc := subClient(t, f)
	defer closeClient(t, c)

	var he *HandshakeError
	if err := c.Connect(context.Background()); !errors.As(err, &he) {
		t.Fatalf("Connect = %v, want *HandshakeError(429)", err)
	}
	// The supervisor is now parked in backoff (fake clock), so clearing the rejection
	// cannot race the next dial: the reconnect dial will succeed.
	f.rejectUpgrade(0, "")
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)

	// The first successful connection ran no prior epoch → no reconnect frame.
	time.Sleep(50 * time.Millisecond) // let a would-be reconnect frame appear
	if got := len(f.framesOfType(typeReconnect)); got != 0 {
		t.Errorf("reconnect frames = %d after a failed first dial then a successful connect, want 0 (no prior epoch to resume)", got)
	}
}

// TestReplayDoesNotAdvanceCursor is the discriminating test for the Source-before-
// cursor order (T140): a replayed record is the ONLY pos-bearing record of its
// epoch, so if the cursor were (wrongly) keyed on the pre-override Source it would
// advance to the replay's pos — and the next reconnect would carry it. The assertion
// that the next reconnect's last_pos has NO entry for the channel fails against that
// inverted implementation and passes only when a replayed record advances nothing.
func TestReplayDoesNotAdvanceCursor(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond:    map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
			closeAfter: websocket.StatusCode(1011),
		},
		// E2: the reconnect is answered with a replayed record + reconnect_ack, then E2
		// drops after that first inbound frame — no live record ever arrives.
		epochScript{
			respond: map[string][]string{typeReconnect: {
				`{"type":"message","channel":"t.a","seq":1,"pos":"9-1","data":{"r":1}}`,
				`{"type":"reconnect_ack","status":"completed","messages_replayed":1}`,
			}},
			closeAfterFrames: 1,
			closeAfter:       websocket.StatusCode(1011),
		},
		// E3 captures the reconnect frame's cursor.
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

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForMessages(t, ec, 1) // the replayed record was delivered on E2

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 3)
	waitForFrameCount(t, f, typeReconnect, 2)
	last := reconnectPayloadOf(t, f.framesOfType(typeReconnect)[1])
	if got, ok := last.LastPos["t.a"]; ok {
		t.Errorf("last_pos[t.a] = %q after only a replayed record, want ABSENT (a replay must not advance the cursor)", got)
	}
}

// TestHistorySeedsCursor is the discriminating test for the history seed wiring: a
// SourceHistory record on a cursorless channel must seed the cursor, so the next
// reconnect carries that pos. Deleting the seedIfAbsent wiring from applyRecovery
// fails this (the reconnect last_pos would lack the channel).
func TestHistorySeedsCursor(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond: map[string][]string{typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"message","channel":"t.a","seq":1,"pos":"h-9","history":true,"data":{"h":1}}`,
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
	waitForMessages(t, ec, 1) // the history record seeded the cursor

	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForFrameCount(t, f, typeReconnect, 1)
	if got := reconnectPayloadOf(t, f.framesOfType(typeReconnect)[0]).LastPos["t.a"]; got != "h-9" {
		t.Errorf("last_pos[t.a] = %q, want h-9 (a history record seeds a cursorless channel)", got)
	}
}

// TestReconnectErrorClosesWindow covers the reconnect_error arm: it closes the
// replay window (so a subsequent record is live again) and surfaces *ReconnectError.
func TestReconnectErrorClosesWindow(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{
			respond:    map[string][]string{typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`}},
			closeAfter: websocket.StatusCode(1011),
		},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_error","code":"not_available","message":"direct backend"}`},
			typeSubscribe: {
				`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
				`{"type":"message","channel":"t.a","seq":1,"pos":"L-1","data":{"l":1}}`, // after reconnect_error ⇒ live, not replay
			},
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
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)

	msgs := waitForMessages(t, ec, 1)
	if msgs[0].Source != SourceLive {
		t.Errorf("record after reconnect_error: Source = %v, want SourceLive (the window closed)", msgs[0].Source)
	}
	var sawErr bool
	for _, ev := range ec.snapshot() {
		if re, ok := ev.(*ReconnectError); ok && re.Code == "not_available" {
			sawErr = true
		}
	}
	if !sawErr {
		t.Error("no *ReconnectError{not_available} surfaced from reconnect_error")
	}
}

// TestSourceForHistoryAndReplayMessage covers T143: a history:true `message` is
// SourceHistory (and seeds the cursor when absent), a `replay_message` is
// SourceReplay, and a replayed record duplicating a live one IS delivered (no
// SDK-side de-duplication).
func TestSourceForHistoryAndReplayMessage(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{typeSubscribe: {
		`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
		`{"type":"message","channel":"t.a","seq":1,"pos":"h-1","history":true,"data":{"h":1}}`, // SourceHistory, seeds cursor
		`{"type":"replay_message","channel":"t.a","seq":2,"pos":"r-1","data":{"x":1}}`,         // SourceReplay
		`{"type":"message","channel":"t.a","seq":3,"pos":"l-1","data":{"x":1}}`,                // SourceLive, dup data of the replay — still delivered
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

	msgs := waitForMessages(t, ec, 3)
	bySeq := map[int64]*Message{}
	for _, m := range msgs {
		bySeq[m.Seq] = m
	}
	if bySeq[1] == nil || bySeq[1].Source != SourceHistory {
		t.Errorf("history record: got %+v, want SourceHistory", bySeq[1])
	}
	if bySeq[2] == nil || bySeq[2].Source != SourceReplay {
		t.Errorf("replay_message: got %+v, want SourceReplay", bySeq[2])
	}
	if bySeq[3] == nil || bySeq[3].Source != SourceLive {
		t.Errorf("live record: got %+v, want SourceLive", bySeq[3])
	}
	// Per-channel FIFO within arrival order (seq 1,2,3 arrived in order).
	if msgs[0].Seq != 1 || msgs[1].Seq != 2 || msgs[2].Seq != 3 {
		t.Errorf("arrival order = %d,%d,%d, want 1,2,3 (per-channel FIFO)", msgs[0].Seq, msgs[1].Seq, msgs[2].Seq)
	}
}

// TestWithClientIDValidationAndGeneration covers the WithClientID contract: a
// whitespace-bearing id is rejected at NewClient, and an unset id is generated
// (non-empty) and appears in the reconnect frame.
func TestWithClientIDValidationAndGeneration(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})

	// Whitespace is rejected.
	if _, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithNoAuth(), WithClientID("has space")); err == nil {
		t.Error("WithClientID with whitespace: NewClient returned nil error, want a validation error")
	}

	// Unset → a generated, non-empty id carried in the reconnect frame.
	f2 := newFakeWS(t)
	f2.script(
		epochScript{closeAfter: websocket.StatusCode(1011)},
		epochScript{respond: map[string][]string{
			typeReconnect: {`{"type":"reconnect_ack","status":"completed","messages_replayed":0}`},
		}},
	)
	c, fc := subClient(t, f2) // no WithClientID → generated
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f2, 2)
	waitForFrameCount(t, f2, typeReconnect, 1)
	frame := f2.framesOfType(typeReconnect)[0]
	id := reconnectPayloadOf(t, frame).ClientID
	if strings.TrimSpace(id) == "" {
		t.Error("generated client_id is empty")
	}
	if len(id) != 32 {
		t.Errorf("generated client_id = %q (len %d), want 32 hex chars", id, len(id))
	}
	// A cursorless reconnect must carry an empty object, not null: the server reads a
	// null last_pos as malformed (wire.go). Asserted on the raw frame — unmarshal
	// collapses {} and null to the same nil map.
	if !strings.Contains(frame.raw, `"last_pos":{}`) {
		t.Errorf("cursorless reconnect frame = %s, want last_pos as {} (not null)", frame.raw)
	}
}

// TestSubscribeFromInsideMessagesLoopDoesNotDeadlock is T144 row 2 (SC-013): a
// Subscribe issued from INSIDE a draining `for ev := range client.Messages()` loop
// completes without deadlock. The serializer's queue is fire-and-forget and never
// waits on the consumer, so a re-entrant control-plane call from the delivery loop is
// safe — the proof is the second `subscribe` frame reaching the fake while the loop
// keeps draining. If Subscribe blocked on anything the loop must itself drain, the
// loop could never reach the second call and this would hang.
func TestSubscribeFromInsideMessagesLoopDoesNotDeadlock(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
	}})
	c, _ := subClient(t, f, WithClientID("cid"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	// Re-enter Subscribe on the t.a *SubscriptionResult specifically — not the first
	// event, which is a connect *StateChange: re-entering then would race the t.b
	// subscribe against the outstanding t.a one, and a t.b-first ordering would strand
	// t.a's flight (its ack fails the subset guard). Gating on the result makes the
	// ordering deterministic (t.a's ack is already reconciled) and exercises the claim.
	subErr := make(chan error, 1)
	go func() {
		reentrant := false
		for ev := range c.Messages() {
			if _, ok := ev.(*SubscriptionResult); ok && !reentrant {
				reentrant = true
				subErr <- c.Subscribe(context.Background(), []string{"t.b"})
			}
		}
	}()

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// The loop received the t.a *SubscriptionResult and re-entered Subscribe from within
	// the delivery loop without deadlocking.
	select {
	case err := <-subErr:
		if err != nil {
			t.Fatalf("re-entrant Subscribe: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumer loop never re-entered Subscribe — deadlock")
	}
	// The re-entrant subscribe frame reached the wire while the loop kept draining.
	waitForFrameCount(t, f, typeSubscribe, 2)
}

// TestControlPlaneResolvesWhileDataBacklogUndrained is T144 row 1 (SC-013): a
// subscription_ack that decode has READ resolves the control-plane state
// (Subscriptions/PendingSubscriptions) synchronously, even though the derived
// *SubscriptionResult — a DATA-class event — cannot yet be delivered because the data
// region is at its ceiling. The data region is filled first (white-box, decode idle)
// and the consumer is stopped, so decode reconciles the grant and then PARKS on the
// result emit: the grant is observable via Subscriptions() while the result is still
// undelivered. Asserting on Subscriptions() (not on the derived *SubscriptionResult, a
// data event the reserve keeps behind the backlog) is the point of the row.
func TestControlPlaneResolvesWhileDataBacklogUndrained(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
	}})
	c, _ := subClient(t, f, WithClientID("cid"), WithQueueSize(117), WithHistoryLimit(1))
	// No consumer: the connect preamble and the fill stay buffered.
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Fill the data region to its ceiling BEFORE subscribing, with decode idle, so the
	// ack's *SubscriptionResult emit is the send that parks.
	fillDataRegion(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Decode reads the ack, reconciles the grant, then parks on the result emit (the
	// data region is full).
	c.delivery.waitUntilBlocked()

	// The control-plane state is resolved even though the *SubscriptionResult is stuck
	// behind the full data region.
	if got := c.Subscriptions(); !slices.Contains(got, "t.a") {
		t.Errorf("Subscriptions() = %v, want to include t.a (grant resolved while its result is undelivered)", got)
	}
	if got := c.PendingSubscriptions(); len(got) != 0 {
		t.Errorf("PendingSubscriptions() = %v, want empty (t.a was granted)", got)
	}
	closeClient(t, c)
}
