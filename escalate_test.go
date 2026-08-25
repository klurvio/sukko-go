package sukko

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ackAll scripts an epoch that answers every auth frame with an auth_ack{exp:0}.
func ackEpoch() epochScript {
	return epochScript{respond: map[string][]string{typeAuth: {`{"type":"auth_ack","data":{"exp":0}}`}}}
}

// waitForAuthenticated polls the collected events for the next *Authenticated.
func waitForAuthenticated(t *testing.T, ec *eventCollector) *Authenticated {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		for _, ev := range ec.snapshot() {
			if a, ok := ev.(*Authenticated); ok {
				return a
			}
		}
		select {
		case <-deadline:
			t.Fatal("no *Authenticated surfaced")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestEscalateSendsEscalationAuth covers the core: on a live socket, Escalate
// sends an `auth` frame carrying the caller-supplied JWT, and the auth_ack
// surfaces *Authenticated in escalation mode.
func TestEscalateSendsEscalationAuth(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	c, _ := authClient(t, f, WithAPIKey("key-1"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	if raw := f.framesOfType(typeAuth)[0].raw; !strings.Contains(raw, "jwt-esc") {
		t.Errorf("auth frame = %s, want the escalation JWT", raw)
	}
	if au := waitForAuthenticated(t, ec); au.Mode != AuthEscalation {
		t.Errorf("Authenticated.Mode = %v, want escalation", au.Mode)
	}
}

// TestEscalateWhileDisconnectedReturnsNotConnected pins the live-socket gate: in
// any non-connected state Escalate returns *NotConnectedError and sends nothing.
func TestEscalateWhileDisconnectedReturnsNotConnected(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c, _ := authClient(t, f, WithAPIKey("key")) // not connected yet
	err := c.Escalate(context.Background(), "jwt")
	var nce *NotConnectedError
	if !errors.As(err, &nce) {
		t.Fatalf("Escalate before connect = %v, want *NotConnectedError", err)
	}
	if nce.Op != "Escalate" {
		t.Errorf("NotConnectedError.Op = %q, want Escalate", nce.Op)
	}
}

// TestConnectReturnsConnected pins that Connect returning nil guarantees
// State()==Connected: an immediate Escalate (or RefreshToken) must not race the
// →connected transition into a spurious *NotConnectedError.
func TestConnectReturnsConnected(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	c, _ := authClient(t, f, WithToken("jwt"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if got := c.State(); got != StateConnected {
		t.Errorf("State() = %v immediately after Connect returned nil, want connected", got)
	}
	if err := c.Escalate(context.Background(), "jwt2"); err != nil {
		t.Errorf("Escalate immediately after Connect = %v, want nil", err)
	}
}

// TestEscalateEmptyToken rejects an empty JWT with ErrEmptyToken.
func TestEscalateEmptyToken(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c, _ := authClient(t, f, WithAPIKey("key"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)
	if err := c.Escalate(context.Background(), ""); !errors.Is(err, ErrEmptyToken) {
		t.Errorf("Escalate(\"\") = %v, want ErrEmptyToken", err)
	}
}

// TestEscalateAfterCloseReturnsClosed covers the closed state.
func TestEscalateAfterCloseReturnsClosed(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c, _ := authClient(t, f, WithAPIKey("key"))
	connectThenClose(t, c)
	if err := c.Escalate(context.Background(), "jwt"); !errors.Is(err, ErrClosed) {
		t.Errorf("Escalate after close = %v, want ErrClosed", err)
	}
}

// TestAuthenticatedModeReflectsMethod pins the mode handoff: a RefreshToken
// surfaces *Authenticated{AuthRefresh}, an Escalate surfaces
// *Authenticated{AuthEscalation} — the mode is the method the caller called, not
// inferred from the wire (which carries none).
func TestAuthenticatedModeReflectsMethod(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	c, fc := authClient(t, f, WithToken("jwt-1"), WithAPIKey("key"))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	// A refresh → AuthRefresh.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	// An escalation → AuthEscalation (floored to RefreshMinInterval since the refresh).
	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)

	var modes []AuthMode
	deadline := time.After(3 * time.Second)
	for len(modes) < 2 {
		modes = modes[:0]
		for _, ev := range ec.snapshot() {
			if a, ok := ev.(*Authenticated); ok {
				modes = append(modes, a.Mode)
			}
		}
		select {
		case <-deadline:
			t.Fatalf("*Authenticated modes = %v, want [refresh escalation]", modes)
		case <-time.After(2 * time.Millisecond):
		}
	}
	if modes[0] != AuthRefresh || modes[1] != AuthEscalation {
		t.Errorf("*Authenticated modes = %v, want [refresh escalation]", modes)
	}
}

// TestEscalateCommitsJWTOnAck covers the at-ack commit: the escalation JWT is
// written to the credential store only when the auth_ack arrives. A refresh right
// after (which reads the store) then carries the committed JWT.
func TestEscalateCommitsJWTOnAck(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	c, fc := authClient(t, f, WithToken("jwt-orig"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	// A refresh cannot send until the escalation's flight clears (single-flight),
	// and the flight clears in the same handler that commits the JWT — so this
	// refresh is guaranteed to read the committed store.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
	if raw := f.framesOfType(typeAuth)[1].raw; !strings.Contains(raw, "jwt-esc") {
		t.Errorf("post-escalation refresh = %s, want the committed escalation JWT", raw)
	}
}

// TestEscalateRejectedDoesNotCommit covers the at-ack safety: a rejected
// escalation (auth_error) must NOT poison the credential store — a later refresh
// still carries the original JWT, not the rejected escalation JWT.
func TestEscalateRejectedDoesNotCommit(t *testing.T) {
	f := newFakeWS(t)
	// Every auth is rejected — enough to prove the escalation JWT was not committed.
	f.script(epochScript{
		respond: map[string][]string{typeAuth: {`{"type":"auth_error","data":{"code":"invalid_token","message":"nope"}}`}},
	})
	c, fc := authClient(t, f, WithToken("jwt-orig"),
		WithRand(newFakeRand()), WithHeartbeatInterval(time.Hour))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	// The auth_error cleared the flight without committing. A refresh now reads the
	// store, which must still hold the original JWT.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
	raw := f.framesOfType(typeAuth)[1].raw
	if strings.Contains(raw, "jwt-esc") {
		t.Errorf("post-rejection refresh = %s, must NOT carry the rejected escalation JWT", raw)
	}
	if !strings.Contains(raw, "jwt-orig") {
		t.Errorf("post-rejection refresh = %s, want the retained original JWT", raw)
	}
}

// TestEscalateSurvivesEpochDrop pins the fix for the epoch-drop-mid-escalation
// bug: an escalation whose epoch dies before the ack is NOT silently lost. Unlike
// a refresh (whose credential the reconnect dial re-presents from the store), an
// escalation JWT is committed only at-ack and is not in the store — so it must be
// re-sent on the reconnect, or the caller's escalation vanishes.
func TestEscalateSurvivesEpochDrop(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{closeAfterFrames: 1, closeAfter: 1011},                                            // E1 closes after the escalation frame, no ack
		epochScript{respond: map[string][]string{typeAuth: {`{"type":"auth_ack","data":{"exp":0}}`}}}, // E2 acks
	)
	fc := newFakeClock()
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithAPIKey("key"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Close(ctx)
		ec.waitClosed(t)
	}()

	// Escalate on E1: the frame is sent, then E1 drops without answering.
	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)

	// Reconnect to E2; the escalation is retried (floored to RefreshMinInterval
	// since the E1 send).
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)

	got := f.framesOfType(typeAuth)[1]
	if got.epoch != 2 || !strings.Contains(got.raw, "jwt-esc") {
		t.Errorf("second auth frame: epoch %d raw %s, want epoch 2 carrying jwt-esc", got.epoch, got.raw)
	}
	// E2's ack surfaces *Authenticated in escalation mode and commits the JWT.
	var sawEsc bool
	for _, ev := range ec.snapshot() {
		if a, ok := ev.(*Authenticated); ok && a.Mode == AuthEscalation {
			sawEsc = true
		}
	}
	if !sawEsc {
		t.Error("no *Authenticated{escalation} after the reconnect re-escalation")
	}
}

// TestEscalateWaitsForInFlightRefresh covers single-flight: an escalation must not
// preempt an `auth` already in flight — it waits for the flight to clear.
func TestEscalateWaitsForInFlightRefresh(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // withholds the ack → the refresh stays in flight
	c, _ := authClient(t, f, WithToken("jwt-1"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let a would-be preempting send appear
	if got := len(f.framesOfType(typeAuth)); got != 1 {
		t.Errorf("auth frames = %d, want 1 (escalation must not preempt an in-flight auth)", got)
	}
}

// TestEscalateBoxLatestWins pins the latest-wins box: a second Escalate before the
// owner sends overwrites the JWT, so the freshest credential is escalated.
func TestEscalateBoxLatestWins(t *testing.T) {
	t.Parallel()
	var b escalateBox
	b.put("jwt-1", 1)
	b.put("jwt-2", 2)
	jwt, gen, ok := b.take()
	if !ok || jwt != "jwt-2" || gen != 2 {
		t.Errorf("take() = (%q, %d, %v), want (jwt-2, 2, true)", jwt, gen, ok)
	}
	if _, _, ok := b.take(); ok {
		t.Error("take() after a take still reports a pending JWT; box was not cleared")
	}
}

// TestUpdateTokenSendsNoAuthWhileConnected pins UpdateToken as store-only: it
// never sends an `auth` frame, even on a live socket (that is RefreshToken and
// Escalate).
func TestUpdateTokenSendsNoAuthWhileConnected(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c, _ := authClient(t, f, WithToken("jwt-1"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.UpdateToken("jwt-2"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := len(f.framesOfType(typeAuth)); got != 0 {
		t.Errorf("UpdateToken sent %d auth frames while connected, want 0 (store-only)", got)
	}
}
