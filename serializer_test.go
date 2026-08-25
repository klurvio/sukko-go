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
