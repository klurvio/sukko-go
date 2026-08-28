package sukko

import (
	"context"
	"testing"
	"time"
)

// The delivery tests drive the classed reserve (ADR-0006). The reserve's whole
// point is that data can never starve the safety advisories out of their slots,
// and that the final *Terminal always has one — so the tests assert exactly
// those boundaries, at a small QueueSize where the 16-slot headroom is easy to
// fill.
//
// A small QueueSize keeps AdvisoryHeadroom (a fixed 16) a large fraction of the
// channel: at QueueSize 20 the data ceiling is 4 and the safety ceiling 19, so
// four data events reach the reserve boundary.
const testQueueSize = 20

// fillData sends n data events that must all be admitted immediately (the
// consumer is stopped but the channel has room below the data ceiling).
func fillData(t *testing.T, d *delivery, n int) {
	t.Helper()
	for i := range n {
		r := d.send(context.Background(), context.Background(), &Message{Channel: "acme.x", Seq: int64(i)})
		if r != notDiscarded {
			t.Fatalf("fill send %d was discarded (%v); the channel should have had room", i, r)
		}
	}
}

// assertParked confirms a send launched in a goroutine has not completed. It is
// called only after waitUntilBlocked has returned — the deterministic sync point
// that the send has parked — so no sleep is needed to rule out a completion.
func assertParked(t *testing.T, done <-chan discardReason) {
	t.Helper()
	select {
	case r := <-done:
		t.Fatalf("send completed (%v) but should be parked at the class ceiling", r)
	default:
	}
}

// TestReserveAdmission is T040 — the three admission rows that are Phase 3's
// exit criterion. At reserve-boundary occupancy (the data ceiling filled,
// consumer stopped): a data send blocks, a safety-subset advisory succeeds into
// the reserve, and an unbounded caller-driven advisory blocks rather than
// consuming a reserved slot. Row 3 is the discriminating case: it proves the
// reserve is *classed*, not merely reserved.
func TestReserveAdmission(t *testing.T) {
	t.Run("data blocks at the boundary", func(t *testing.T) {
		d := newDelivery(testQueueSize, newFakeClock(), &counters{})
		fillData(t, d, d.dataCeiling)

		root, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan discardReason, 1)
		go func() {
			done <- d.send(root, context.Background(), &Message{Channel: "acme.x"})
		}()

		d.waitUntilBlocked()
		assertParked(t, done)

		// Release the parked goroutine so goleak stays clean: a canceled root
		// discards the parked event, distinguishably.
		cancel()
		if r := <-done; r != discardedRoot {
			t.Errorf("after root cancel got %v, want discardedRoot", r)
		}
	})

	t.Run("safety advisory succeeds at the boundary", func(t *testing.T) {
		d := newDelivery(testQueueSize, newFakeClock(), &counters{})
		fillData(t, d, d.dataCeiling)

		// A *Gap is in the safety subset, so its ceiling is QueueSize-1 — it
		// takes a reserve slot rather than blocking.
		r := d.send(context.Background(), context.Background(), &Gap{Channel: "acme.x"})
		if r != notDiscarded {
			t.Errorf("safety advisory at the data boundary got %v, want admitted into the reserve", r)
		}
	})

	t.Run("unbounded advisory blocks at the boundary", func(t *testing.T) {
		d := newDelivery(testQueueSize, newFakeClock(), &counters{})
		fillData(t, d, d.dataCeiling)

		root, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan discardReason, 1)
		go func() {
			// *PublishAccepted is caller-driven and unbounded, so it uses the
			// data ceiling — it must NOT dip into a safety producer's reserve.
			done <- d.send(root, context.Background(), &PublishAccepted{Channel: "acme.x"})
		}()

		d.waitUntilBlocked()
		assertParked(t, done)

		cancel()
		<-done
	})
}

// TestTerminalLandsWhenChannelFullOfSafety proves SC-014's structural guarantee:
// the *Terminal slot cannot be taken by any other producer, so a non-blocking
// send lands even when the rest of the channel is full to the safety ceiling.
func TestTerminalLandsWhenChannelFullOfSafety(t *testing.T) {
	d := newDelivery(testQueueSize, newFakeClock(), &counters{})

	// Fill to the safety ceiling: data up to its ceiling, then safety advisories
	// fill the reserve to one below capacity.
	fillData(t, d, d.dataCeiling)
	for i := d.dataCeiling; i < d.safetyCeiling; i++ {
		if r := d.send(context.Background(), context.Background(), &Gap{Channel: "acme.x"}); r != notDiscarded {
			t.Fatalf("safety fill %d was discarded (%v)", i, r)
		}
	}
	if got := len(d.messages()); got != d.safetyCeiling {
		t.Fatalf("channel occupancy = %d, want %d (safety ceiling)", got, d.safetyCeiling)
	}

	// The reserved slot is untouched, so the non-blocking Terminal send lands.
	if !d.sendTerminal(&Terminal{}) {
		t.Fatal("Terminal did not land in its reserved slot on a safety-full channel")
	}
	if got := len(d.messages()); got != d.queueSize {
		t.Errorf("occupancy after Terminal = %d, want %d (full)", got, d.queueSize)
	}
}

// TestParkedSendDiscardDistinguishesContexts pins T037's requirement that the
// send reports which context discarded a parked event — the supervisor needs to
// tell a root teardown from an epoch reconnect even though the event's fate is
// the same.
func TestParkedSendDiscardDistinguishesContexts(t *testing.T) {
	t.Run("root cancel", func(t *testing.T) {
		d := newDelivery(testQueueSize, newFakeClock(), &counters{})
		fillData(t, d, d.dataCeiling)

		root, cancelRoot := context.WithCancel(context.Background())
		epoch, cancelEpoch := context.WithCancel(context.Background())
		defer cancelEpoch()
		done := make(chan discardReason, 1)
		go func() { done <- d.send(root, epoch, &Message{Channel: "acme.x"}) }()

		d.waitUntilBlocked()
		cancelRoot()
		if r := <-done; r != discardedRoot {
			t.Errorf("got %v, want discardedRoot", r)
		}
	})

	t.Run("epoch cancel", func(t *testing.T) {
		d := newDelivery(testQueueSize, newFakeClock(), &counters{})
		fillData(t, d, d.dataCeiling)

		root, cancelRoot := context.WithCancel(context.Background())
		defer cancelRoot()
		epoch, cancelEpoch := context.WithCancel(context.Background())
		done := make(chan discardReason, 1)
		go func() { done <- d.send(root, epoch, &Message{Channel: "acme.x"}) }()

		d.waitUntilBlocked()
		cancelEpoch()
		if r := <-done; r != discardedEpoch {
			t.Errorf("got %v, want discardedEpoch", r)
		}
	})
}

// TestBackpressureAccountingAndProbeReadmit exercises the probe re-admit and its
// accounting: one continuous episode is counted once, the blocked duration is
// the elapsed fake-clock time, and the send is re-admitted after the consumer
// drains and the probe fires.
func TestBackpressureAccountingAndProbeReadmit(t *testing.T) {
	clock := newFakeClock()
	c := &counters{}
	d := newDelivery(testQueueSize, clock, c)
	fillData(t, d, d.dataCeiling)

	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan discardReason, 1)
	go func() { done <- d.send(root, context.Background(), &Message{Channel: "acme.x"}) }()

	d.waitUntilBlocked()
	if got := c.snapshot().BackpressureBlocks; got != 1 {
		t.Fatalf("BackpressureBlocks = %d after parking, want 1 episode", got)
	}

	// Make room, then fire the probe: the parked send re-checks and is admitted.
	<-d.messages()
	clock.BlockUntilTimer(purposeDeliveryProbe)
	clock.Advance(deliveryProbeInterval)

	if r := <-done; r != notDiscarded {
		t.Errorf("after drain + probe, got %v, want admitted", r)
	}

	got := c.snapshot()
	if got.Blocked != deliveryProbeInterval {
		t.Errorf("Blocked = %s, want %s (the parked interval)", got.Blocked, deliveryProbeInterval)
	}
	if got.BackpressureBlocks != 1 {
		t.Errorf("BackpressureBlocks = %d, want 1 — a re-probe within one episode must not recount", got.BackpressureBlocks)
	}
}

// TestConcurrentAdmitDoesNotCorruptParkedEpisode pins the two-sender accounting
// rule: a safety advisory admitting into the reserve while a data sender is
// parked must not end the parked sender's episode. The parked sender is the only
// one blocked; the admitting one never parked, so it has no episode to close and
// no blocked time to bank.
//
// Time is advanced by less than the probe interval, so the parked sender stays
// parked (its probe does not fire) while real wall time passes — which is what
// makes a wrongly-banked interval observable.
func TestConcurrentAdmitDoesNotCorruptParkedEpisode(t *testing.T) {
	clock := newFakeClock()
	c := &counters{}
	d := newDelivery(testQueueSize, clock, c)
	fillData(t, d, d.dataCeiling)

	rootB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	doneB := make(chan discardReason, 1)
	go func() { doneB <- d.send(rootB, context.Background(), &Message{Channel: "acme.x"}) }()
	d.waitUntilBlocked() // B parked; one episode open

	if got := c.snapshot().BackpressureBlocks; got != 1 {
		t.Fatalf("BackpressureBlocks = %d after B parks, want 1", got)
	}

	// Time passes, but not enough to fire B's probe.
	clock.Advance(deliveryProbeInterval / 2)

	// A safety advisory admits into the reserve while B is still parked.
	if r := d.send(context.Background(), context.Background(), &Gap{Channel: "acme.x"}); r != notDiscarded {
		t.Fatalf("safety admit while B parked: %v", r)
	}

	got := c.snapshot()
	if got.BackpressureBlocks != 1 {
		t.Errorf("BackpressureBlocks = %d after a non-parking admit, want 1 — A must not touch B's episode", got.BackpressureBlocks)
	}
	if got.Blocked != 0 {
		t.Errorf("Blocked = %s after a non-parking admit, want 0 — B's episode is still open, A banked nothing", got.Blocked)
	}

	cancelB()
	<-doneB
}

// TestReceiveOrderIsSendOrder is T042: data and advisory events sent through the
// one channel, by one sender, emerge in exact send order — the reservation
// governs admission, never ordering.
func TestReceiveOrderIsSendOrder(t *testing.T) {
	// This test is about FIFO order, not back-pressure, so the channel is sized
	// well above the data ceiling: at testQueueSize the ceiling is 4, and a
	// consumer that lags even briefly would push a data send past it and park it
	// on the probe timer — which this test never advances. A roomy channel keeps
	// every send non-blocking so the assertion is purely about ordering.
	d := newDelivery(64, newFakeClock(), &counters{})

	sent := []Event{
		&Message{Channel: "a", Seq: 1},
		&Gap{Channel: "a"}, // safety advisory, interleaved
		&Message{Channel: "a", Seq: 2},
		&PublishAccepted{Channel: "a"}, // unbounded advisory, interleaved
		&Message{Channel: "a", Seq: 3},
	}

	got := make([]Event, 0, len(sent))
	done := make(chan struct{})
	go func() {
		for range sent {
			got = append(got, <-d.messages())
		}
		close(done)
	}()

	for _, ev := range sent {
		if r := d.send(context.Background(), context.Background(), ev); r != notDiscarded {
			t.Fatalf("send of %T was discarded", ev)
		}
	}
	<-done

	if len(got) != len(sent) {
		t.Fatalf("received %d events, want %d", len(got), len(sent))
	}
	for i := range sent {
		if got[i] != sent[i] {
			t.Errorf("position %d: got %T, want %T (order not preserved)", i, got[i], sent[i])
		}
	}
}

// TestDeliveryTrySendReservesTerminalSlot pins trySend (the *PossibleGap reserve
// path): it admits safety events only below the safety ceiling, returns false once
// the ceiling is reached — reserving the last slot — and the final *Terminal still
// lands in that reserved slot afterward.
func TestDeliveryTrySendReservesTerminalSlot(t *testing.T) {
	const q = 4 // safetyCeiling = q-1 = 3
	d := newDelivery(q, newFakeClock(), &counters{})

	// Fill up to the safety ceiling: q-1 safety events land.
	for i := range q - 1 {
		if !d.trySend(&Gap{}) {
			t.Fatalf("trySend #%d below the ceiling failed", i)
		}
	}
	// At the ceiling, trySend refuses — the last slot is reserved for *Terminal.
	if d.trySend(&Gap{}) {
		t.Error("trySend at the safety ceiling succeeded; it must reserve the Terminal slot")
	}
	// The *Terminal still lands in its reserved slot.
	if !d.sendTerminal(&Terminal{}) {
		t.Error("sendTerminal did not land after trySend filled the reserve to the ceiling")
	}
}

// fillDataRegion pushes data events straight into the client's delivery channel until
// it sits at EXACTLY the data ceiling — a white-box fill (the delivery-layer tests
// already build occupancy this way with d.send) that leaves a data send one slot from
// parking while a safety send still has the reserve. The decode goroutine must be idle
// (no pending server frames), so these are the only sends in flight and nothing parks.
func fillDataRegion(t *testing.T, c *Client) {
	t.Helper()
	dc := c.delivery.dataCeiling
	for len(c.delivery.ch) < dc {
		if r := c.delivery.send(context.Background(), context.Background(), &Message{Channel: "t.fill", Seq: 1}); r != notDiscarded {
			t.Fatalf("fill send discarded (%v) before reaching the data ceiling", r)
		}
	}
}

// waitForChannelLen polls the client's delivery channel occupancy up to `want`. It is
// the white-box sync point for "the flood has been admitted" when the consumer is
// stopped and no wire frame marks completion (the events arrive server-driven).
func waitForChannelLen(t *testing.T, c *Client, want int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for len(c.delivery.ch) < want {
		select {
		case <-deadline:
			t.Fatalf("delivery channel len stuck at %d, want %d", len(c.delivery.ch), want)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestAdvisoriesCannotStarveTerminal is T148 (NFR-005(mmm)): a stream of publish
// advisories (*PublishAccepted — a DATA-class event, capped at the data ceiling) can
// never take the *Terminal's reserved slot. With the consumer stopped, the client is
// fed N `publish_ack` frames and then closed; the final *Terminal is always delivered
// as the last event before the channel closes and Err() is nil — whether N overfills
// the data region (the excess advisories park and are discarded on the root-cancel
// close) or sits below AdvisoryHeadroom. Both rows share one script and differ only in
// N, so the item discriminates on the CLASSING (the Terminal slot is inviolate), not on
// incidental capacity.
func TestAdvisoriesCannotStarveTerminal(t *testing.T) {
	const queueSize = 117 // dataCeiling = 101, AdvisoryHeadroom = 16
	tests := []struct {
		name     string
		n        int
		overfill bool
	}{
		{"more advisories than the data region holds", 110, true}, // > dataCeiling: advisories park
		{"fewer advisories than AdvisoryHeadroom", 10, false},     // < 16: never near a ceiling
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeWS(t)
			resp := make([]string, 0, tc.n+1)
			resp = append(resp, `{"type":"subscription_ack","subscribed":["t.a"],"count":1}`)
			for range tc.n {
				resp = append(resp, `{"type":"publish_ack","channel":"t.a"}`)
			}
			f.script(epochScript{respond: map[string][]string{typeSubscribe: resp}})

			c, _ := subClient(t, f, WithClientID("cid"), WithQueueSize(queueSize), WithHistoryLimit(1))
			if err := c.Connect(context.Background()); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			// Wait for the flood to settle: overfill parks a send at the data ceiling;
			// underfill admits all N advisories plus the one *SubscriptionResult.
			if tc.overfill {
				c.delivery.waitUntilBlocked()
			} else {
				// n advisories + the *SubscriptionResult + 2 connect *StateChange all
				// settle below the ceiling; wait for all of them so Close doesn't cancel
				// the epoch mid-burst and drop the tail advisories.
				waitForChannelLen(t, c, tc.n+3)
			}

			// Close returns within its bound even with the data region full of parked
			// advisories — the parked sends discard on the root cancel.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := c.Close(ctx); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := c.Err(); err != nil {
				t.Errorf("Err() = %v, want nil (clean close)", err)
			}

			// Drain what remains: the last event before channel-close must be *Terminal.
			var events []Event
			for ev := range c.Messages() {
				events = append(events, ev)
			}
			if len(events) == 0 {
				t.Fatal("no events delivered; expected at least the *Terminal")
			}
			if _, ok := events[len(events)-1].(*Terminal); !ok {
				t.Errorf("last event = %T, want *Terminal as the final event before channel-close", events[len(events)-1])
			}

			got := 0
			for _, ev := range events {
				if _, ok := ev.(*PublishAccepted); ok {
					got++
				}
			}
			if tc.overfill {
				// The excess advisories parked past the data ceiling and were discarded on
				// close; only what fit is delivered — but the Terminal lands regardless.
				if got >= tc.n {
					t.Errorf("delivered %d advisories, want < %d (the overflow must be discarded)", got, tc.n)
				}
			} else if got != tc.n {
				t.Errorf("delivered %d advisories, want all %d", got, tc.n)
			}
		})
	}
}

// TestReserveAdmissionEndToEnd is T149 (NFR-005(y) end-to-end row): with the data
// region filled to its ceiling and the consumer stopped, safety-class events still
// reach the caller through the reserve — the timer-driven *RecoveryInterruptedError
// and then the supervisor's *Terminal are both admitted while data delivery is at the
// ceiling. The producers are the recovery-deadline timer and the supervisor, never the
// idle decode goroutine.
//
// NOTE: the spec's literal "park the decode goroutine" clause predates Slice 3a, whose
// recovery deadline SUSPENDS while a send is parked (a stalled consumer must not
// preempt the server's own slow-client verdict — recovery.go due()). So this fills to
// EXACTLY the data ceiling: nothing parks, the deadline fires cleanly, and the true
// discriminator — safety admission while the data region is at capacity — is preserved.
func TestReserveAdmissionEndToEnd(t *testing.T) {
	f := newFakeWS(t)
	// ack t.a, then a gap → one replay goes out, is never answered, leaving t.a
	// REPLAYING with the recovery deadline armed.
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {
			`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`,
			`{"type":"gap","channel":"t.a","last_pos":"g-1","from_seq":1,"to_seq":9,"ts":1}`,
		},
	}})
	c, fc := subClient(t, f, WithClientID("cid"), WithQueueSize(117), WithHistoryLimit(1))
	// No consumer: the preamble and the fill stay buffered.
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForFrameCount(t, f, typeReplay, 1) // replay in flight, recovery deadline armed
	// The *Gap is forwarded AFTER admitGap on the decode goroutine, so the replay frame
	// alone does not prove decode finished the gap emit. Wait for all four preamble
	// events (2 connect *StateChange + *SubscriptionResult + *Gap) so decode is provably
	// idle before the fill — otherwise a stray *Gap send could race the fill past the
	// ceiling and park it on the (never-advanced) fake clock, deadlocking the test.
	waitForChannelLen(t, c, 4)

	// Fill the data region to EXACTLY its ceiling: a data send would now park, but
	// nothing is parked yet (so the deadline stays fireable — Slice 3a suspends it
	// while a send is parked).
	dc := c.delivery.dataCeiling
	fillDataRegion(t, c)
	waitForChannelLen(t, c, dc)

	// Fire the recovery deadline: the owner admits *RecoveryInterruptedError into the
	// reserve even though the data region is full — the reserve is what carries it.
	fc.BlockUntilTimer(purposeRecoveryDeadline)
	fc.Advance(DefaultRecoveryDeadline)
	waitForChannelLen(t, c, dc+1) // the interrupt landed in the reserve, data still full

	// Cancel the root: the supervisor admits *Terminal into its reserved slot.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// One drain: the interrupt was observed and the *Terminal is the final event.
	var events []Event
	for ev := range c.Messages() {
		events = append(events, ev)
	}
	sawInterrupt := false
	for _, ev := range events {
		if ri, ok := ev.(*RecoveryInterruptedError); ok && ri.Channel == "t.a" {
			sawInterrupt = true
		}
	}
	if !sawInterrupt {
		t.Error("no *RecoveryInterruptedError{t.a} observed — the timer-driven interrupt was not admitted through the reserve while the data region was full")
	}
	if len(events) == 0 {
		t.Fatal("no events drained")
	}
	if _, ok := events[len(events)-1].(*Terminal); !ok {
		t.Errorf("last event = %T, want *Terminal as the final event", events[len(events)-1])
	}
}
