package sukko

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// subClient builds a fakeWS-pointed, no-auth client (subscription tests don't
// exercise auth) and returns the fake clock.
func subClient(t *testing.T, f *fakeWS, opts ...Option) (*Client, *fakeClock) {
	t.Helper()
	fc := newFakeClock()
	base := []Option{WithHTTPClient(f.client()), WithClock(fc), WithNoAuth(), WithRand(newFakeRand())}
	c, err := NewClient(context.Background(), f.URL(), append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, fc
}

// waitForSubscriptionResult polls the collected events for the next
// *SubscriptionResult.
func waitForSubscriptionResult(t *testing.T, ec *eventCollector) *SubscriptionResult {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		for _, ev := range ec.snapshot() {
			if r, ok := ev.(*SubscriptionResult); ok {
				return r
			}
		}
		select {
		case <-deadline:
			t.Fatal("no *SubscriptionResult surfaced")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func sortedEqual(a, b []string) bool {
	a = slices.Clone(a)
	b = slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// TestSubscribeGrantedFully covers the happy path: Subscribe sends a `subscribe`
// frame, the subscription_ack grants all channels, and the SDK surfaces
// *SubscriptionResult with the full grant and updates Subscriptions().
func TestSubscribeGrantedFully(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a","t.b"],"count":2}`},
	}})
	c, _ := subClient(t, f)
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a", "t.b"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 1)

	r := waitForSubscriptionResult(t, ec)
	if !sortedEqual(r.Requested, []string{"t.a", "t.b"}) {
		t.Errorf("Requested = %v, want [t.a t.b]", r.Requested)
	}
	if !sortedEqual(r.Granted, []string{"t.a", "t.b"}) {
		t.Errorf("Granted = %v, want [t.a t.b]", r.Granted)
	}
	if len(r.NotGranted) != 0 {
		t.Errorf("NotGranted = %v, want empty", r.NotGranted)
	}
	if r.Count != 2 {
		t.Errorf("Count = %d, want 2", r.Count)
	}
	if got := c.Subscriptions(); !sortedEqual(got, []string{"t.a", "t.b"}) {
		t.Errorf("Subscriptions() = %v, want [t.a t.b]", got)
	}
	if got := c.PendingSubscriptions(); len(got) != 0 {
		t.Errorf("PendingSubscriptions() = %v, want empty", got)
	}
}

// TestSubscribePartialGrant covers the grant diff: the server grants a subset, so
// the ungranted channel is surfaced as NotGranted and retained as pending.
func TestSubscribePartialGrant(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
	}})
	c, _ := subClient(t, f)
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a", "t.denied"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	r := waitForSubscriptionResult(t, ec)
	if !sortedEqual(r.Granted, []string{"t.a"}) {
		t.Errorf("Granted = %v, want [t.a]", r.Granted)
	}
	if !sortedEqual(r.NotGranted, []string{"t.denied"}) {
		t.Errorf("NotGranted = %v, want [t.denied]", r.NotGranted)
	}
	if got := c.Subscriptions(); !sortedEqual(got, []string{"t.a"}) {
		t.Errorf("Subscriptions() = %v, want [t.a]", got)
	}
	if got := c.PendingSubscriptions(); !sortedEqual(got, []string{"t.denied"}) {
		t.Errorf("PendingSubscriptions() = %v, want [t.denied]", got)
	}
}

// waitForProtocolError polls the collected events for a *ProtocolError.
func waitForProtocolError(t *testing.T, ec *eventCollector) *ProtocolError {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		for _, ev := range ec.snapshot() {
			if pe, ok := ev.(*ProtocolError); ok {
				return pe
			}
		}
		select {
		case <-deadline:
			t.Fatal("no *ProtocolError surfaced")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestSubscribeAckTimeoutReleasesSlot pins the AckTimeout path: when a subscribe's
// ack never arrives, after AckTimeout (= the configured RecoveryDeadline) the
// serializer surfaces a *ProtocolError and releases the slot, so a queued
// subscribe still reaches the wire (the slot is not stranded).
func TestSubscribeAckTimeoutReleasesSlot(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // no subscription_ack → the subscribe stays outstanding
	c, fc := subClient(t, f)
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 1)

	// The ack never comes; advance to the AckTimeout.
	fc.BlockUntilTimer(purposeAckTimeout)
	fc.Advance(DefaultRecoveryDeadline)
	if pe := waitForProtocolError(t, ec); pe == nil {
		t.Fatal("no *ProtocolError for the ack timeout")
	}

	// The slot was released: a follow-up subscribe reaches the wire.
	if err := c.Subscribe(context.Background(), []string{"t.b"}); err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 2)

	// Exactly one *ProtocolError (the timeout), not one per re-check.
	var n int
	for _, ev := range ec.snapshot() {
		if _, ok := ev.(*ProtocolError); ok {
			n++
		}
	}
	if n != 1 {
		t.Errorf("*ProtocolError count = %d, want 1", n)
	}
}

// TestSubscribeAckDisarmsTimeout is the timeout's paired case: an ack that
// arrives disarms the ack timeout, so advancing past what would have been the
// deadline surfaces NO *ProtocolError.
func TestSubscribeAckDisarmsTimeout(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
	}})
	c, fc := subClient(t, f)
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForSubscriptionResult(t, ec) // the ack was processed → the timeout disarmed

	// Advancing past the deadline fires nothing (the timer was disarmed on the ack).
	fc.Advance(2 * DefaultRecoveryDeadline)
	time.Sleep(50 * time.Millisecond)
	for _, ev := range ec.snapshot() {
		if _, ok := ev.(*ProtocolError); ok {
			t.Error("a *ProtocolError surfaced after a successful ack (the timeout was not disarmed)")
		}
	}
}

// TestSubscriptionAckSubsetGuard pins the subset guard (ADR-0013): an ack whose
// `subscribed` contains a channel the outstanding request never asked for cannot
// belong to it (a late ack mis-landing after a timeout) and is dropped — it grants
// nothing and surfaces no *SubscriptionResult.
func TestSubscriptionAckSubsetGuard(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		// Grants a channel the request never asked for → not a valid answer to it.
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a","t.unrequested"],"count":1}`},
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
	waitForFrameCount(t, f, typeSubscribe, 1)
	time.Sleep(50 * time.Millisecond) // let the (rejected) ack be processed

	if got := c.Subscriptions(); len(got) != 0 {
		t.Errorf("Subscriptions() = %v, want none (an ack with an un-requested channel is dropped)", got)
	}
	for _, ev := range ec.snapshot() {
		if _, ok := ev.(*SubscriptionResult); ok {
			t.Error("a subset-violating ack surfaced a *SubscriptionResult")
		}
	}
}

// TestUnsubscriptionAckSubsetGuard pins the symmetric guard: a non-forced
// unsubscription_ack naming a channel the outstanding unsubscribe never sent is a
// late ack for a different request and must NOT release the slot.
func TestUnsubscriptionAckSubsetGuard(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe:   {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		typeUnsubscribe: {`{"type":"unsubscription_ack","unsubscribed":["t.wrong"],"count":0}`}, // not t.a
	}})
	c, _ := subClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForCond(t, "granted t.a", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })
	if err := c.Unsubscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	waitForFrameCount(t, f, typeUnsubscribe, 1) // wire unsubscribe sent; the ack names the wrong channel

	// The mis-matched ack was rejected → the unsubscribe slot stays held → a
	// follow-up subscribe does NOT reach the wire.
	if err := c.Subscribe(context.Background(), []string{"t.b"}); err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := len(f.framesOfType(typeSubscribe)); got != 1 {
		t.Errorf("subscribe frames = %d, want 1 (a mis-matched unsub-ack must not release the slot)", got)
	}
}

// waitForCond polls fn until true or the deadline; fails otherwise.
func waitForCond(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for !fn() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestUnsubscribeGrantedReleasesSlot pins the Unsubscribe happy path AND the
// slot-release fix: unsubscribing a granted channel sends a wire `unsubscribe`,
// prunes it from Subscriptions(), and — because the unsubscription_ack releases
// the serializer's slot — a follow-up Subscribe still reaches the wire (no wedge).
func TestUnsubscribeGrantedReleasesSlot(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe:   {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		typeUnsubscribe: {`{"type":"unsubscription_ack","unsubscribed":["t.a"],"count":0}`},
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
	waitForCond(t, "granted t.a", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })

	if err := c.Unsubscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	waitForFrameCount(t, f, typeUnsubscribe, 1) // a granted channel reaches the wire
	waitForCond(t, "t.a pruned", func() bool { return len(c.Subscriptions()) == 0 })

	var sawUnsub bool
	for _, ev := range ec.snapshot() {
		if u, ok := ev.(*Unsubscribed); ok && slices.Contains(u.Channels, "t.a") {
			sawUnsub = true
		}
	}
	if !sawUnsub {
		t.Error("no *Unsubscribed surfaced for t.a")
	}

	// The slot was released by the unsubscription_ack: a follow-up subscribe is not
	// wedged behind a never-released slot.
	if err := c.Subscribe(context.Background(), []string{"t.b"}); err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 2)
}

// TestUnsubscribeDeniedChannelNoWire pins both-set pruning: unsubscribing a
// channel that was requested but never granted prunes it from PendingSubscriptions
// with NO wire `unsubscribe` frame.
func TestUnsubscribeDeniedChannelNoWire(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe: {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
	}})
	c, _ := subClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a", "t.denied"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForCond(t, "t.denied pending", func() bool { return sortedEqual(c.PendingSubscriptions(), []string{"t.denied"}) })

	if err := c.Unsubscribe(context.Background(), []string{"t.denied"}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	waitForCond(t, "t.denied pruned", func() bool { return len(c.PendingSubscriptions()) == 0 })
	time.Sleep(50 * time.Millisecond) // let a would-be wire frame appear
	if got := len(f.framesOfType(typeUnsubscribe)); got != 0 {
		t.Errorf("unsubscribe frames = %d, want 0 (a never-granted channel is pruned locally)", got)
	}
}

// TestUnsubscribeErrorReleasesSlot pins the symmetric-error fix: an
// unsubscribe_error must release the serializer's slot (like subscribe_error), or
// the whole subscription surface wedges.
func TestUnsubscribeErrorReleasesSlot(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{
		typeSubscribe:   {`{"type":"subscription_ack","subscribed":["t.a"],"count":1}`},
		typeUnsubscribe: {`{"type":"unsubscribe_error","code":"invalid_request","message":"nope"}`},
	}})
	c, _ := subClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForCond(t, "granted t.a", func() bool { return sortedEqual(c.Subscriptions(), []string{"t.a"}) })
	if err := c.Unsubscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	waitForFrameCount(t, f, typeUnsubscribe, 1) // wire unsubscribe → answered with unsubscribe_error

	// The unsubscribe_error released the slot: a follow-up subscribe still reaches
	// the wire (not wedged).
	if err := c.Subscribe(context.Background(), []string{"t.b"}); err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	waitForFrameCount(t, f, typeSubscribe, 2)
}

// TestSubscribeQueueFull covers the bounded queue: once SubscribeQueueDepth
// requests are enqueued behind an unanswered in-flight one, the next returns
// ErrSubscribeQueueFull.
func TestSubscribeQueueFull(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // no subscription_ack → the first subscribe stays outstanding
	c, _ := subClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	// The first request goes in flight (no ack); fill the queue behind it.
	if err := c.Subscribe(context.Background(), []string{"t.first"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	var full error
	for range SubscribeQueueDepth + 8 {
		if err := c.Subscribe(context.Background(), []string{"t.x"}); err != nil {
			full = err
			break
		}
	}
	if !errors.Is(full, ErrSubscribeQueueFull) {
		t.Errorf("saturating the queue = %v, want ErrSubscribeQueueFull", full)
	}
}
