package sukko

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestUpdateTokenWinsPreDialFetch pins the override precedence on the dial path: a
// credential set by UpdateToken before connecting wins the first dial over the
// pre-dial TokenSource fetch (B1) — the fetch is skipped and the dial carries the
// caller's credential.
func TestUpdateTokenWinsPreDialFetch(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	ts := func(context.Context) (Token, error) { return Token{Value: "ts-fetched"}, nil }
	c, _ := authClient(t, f, WithTokenSource(ts))
	if err := c.UpdateToken("caller-tok"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	connectThenClose(t, c)
	if got := f.authAt(1).authorization; got != "Bearer caller-tok" {
		t.Errorf("dial Authorization = %q, want the UpdateToken credential (pre-dial fetch skipped)", got)
	}
}

// TestUpdateTokenWinsNextRefreshThenTokenSourceResumes pins the connected-path
// override precedence: after UpdateToken, exactly the next auth uses the caller's
// credential (no fetch); the one after resumes the TokenSource.
func TestUpdateTokenWinsNextRefreshThenTokenSourceResumes(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	var calls atomic.Int64
	ts := func(context.Context) (Token, error) {
		return Token{Value: fmt.Sprintf("ts-%d", calls.Add(1))}, nil
	}
	c, fc := authClient(t, f, WithTokenSource(ts))
	if err := c.Connect(context.Background()); err != nil { // pre-dial fetch = ts-1
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.UpdateToken("caller-tok"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}

	// Refresh #1 uses the caller's credential (no fetch). It is not floored — no
	// owner auth has been sent yet — so it goes out immediately.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken #1: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	if raw := f.framesOfType(typeAuth)[0].raw; !strings.Contains(raw, "caller-tok") {
		t.Errorf("refresh #1 = %s, want the UpdateToken credential", raw)
	}

	// Refresh #2 resumes the TokenSource (fetches ts-2), floored since refresh #1.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken #2: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
	raw := f.framesOfType(typeAuth)[1].raw
	if strings.Contains(raw, "caller-tok") {
		t.Errorf("refresh #2 = %s, must resume the TokenSource, not reuse the override", raw)
	}
	if !strings.Contains(raw, "ts-") {
		t.Errorf("refresh #2 = %s, want a fetched TokenSource credential", raw)
	}
}

// TestCredentialStoreOverrideLifecycle pins the store's override lifecycle
// deterministically: a caller write sets override and no source write clobbers it;
// the override is consumed only by a gen-guarded clear (a fresher caller write
// survives a stale clear); a committed escalation always writes and clears.
func TestCredentialStoreOverrideLifecycle(t *testing.T) {
	t.Parallel()
	s := newCredentialStore("", "")

	// A caller write sets override; a racing source write must not clobber it.
	s.setToken("caller")
	_, override, gen := s.peekOverride()
	if !override {
		t.Fatal("setToken did not set override")
	}
	s.setTokenFromSource("source-raced")
	if tok, _, _ := s.peekOverride(); tok != "caller" {
		t.Errorf("token = %q after a raced source write, want caller (must not clobber a pending override)", tok)
	}

	// A stale clear (older generation) must NOT consume a fresher UpdateToken.
	s.setToken("caller2") // bumps the generation
	s.clearOverrideIfGen(gen)
	if _, ovr, _ := s.peekOverride(); !ovr {
		t.Error("a stale-generation clear consumed a fresher override")
	}

	// A gen-matched clear consumes it; a source write then applies.
	tok, _, gen2 := s.peekOverride()
	if tok != "caller2" {
		t.Errorf("token = %q, want caller2", tok)
	}
	s.clearOverrideIfGen(gen2)
	if _, ovr, _ := s.peekOverride(); ovr {
		t.Error("a gen-matched clear did not consume the override")
	}
	s.setTokenFromSource("source2")
	if got, _, _ := s.peekOverride(); got != "source2" {
		t.Errorf("token = %q after clear+source write, want source2", got)
	}

	// commitEscalationIfGen writes and clears when the generation matches (no newer
	// UpdateToken since the escalation was requested).
	s.setToken("caller3")
	_, _, genA := s.peekOverride()
	s.commitEscalationIfGen("escalated", genA)
	if got, ovr, _ := s.peekOverride(); got != "escalated" || ovr {
		t.Errorf("commitEscalationIfGen(match) = (%q, override=%v), want (escalated, false)", got, ovr)
	}

	// commitEscalationIfGen no-ops when a newer UpdateToken bumped the generation —
	// the later caller action (UpdateToken) wins.
	s.setToken("caller4")
	_, _, genB := s.peekOverride()
	s.setToken("caller5") // a newer UpdateToken intervenes before the escalation acks
	s.commitEscalationIfGen("escalated2", genB)
	if got, ovr, _ := s.peekOverride(); got != "caller5" || !ovr {
		t.Errorf("commitEscalationIfGen(stale) = (%q, override=%v), want (caller5, true) — a newer UpdateToken must win", got, ovr)
	}
}

// TestEscalateThenTokenSourceResumes pins that a committed escalation does not
// claim override: on a TokenSource client, the refresh after an escalation fetches
// a fresh TokenSource credential rather than reusing the escalated JWT.
func TestEscalateThenTokenSourceResumes(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	var calls atomic.Int64
	ts := func(context.Context) (Token, error) {
		return Token{Value: fmt.Sprintf("ts-%d", calls.Add(1))}, nil
	}
	c, fc := authClient(t, f, WithTokenSource(ts))
	if err := c.Connect(context.Background()); err != nil { // pre-dial fetch = ts-1
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1) // escalation frame carries jwt-esc; ack commits it

	// The next refresh resumes the TokenSource (fetches ts-2), not the escalated JWT.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
	raw := f.framesOfType(typeAuth)[1].raw
	if strings.Contains(raw, "jwt-esc") {
		t.Errorf("post-escalation refresh = %s, must resume TokenSource, not reuse the escalated JWT", raw)
	}
	if !strings.Contains(raw, "ts-") {
		t.Errorf("post-escalation refresh = %s, want a fetched TokenSource credential", raw)
	}
}

// TestStaticUpdateTokenThenEscalateCommits pins the F1 fix: on a static client
// (no TokenSource) an UpdateToken leaves an override that nothing consumes, but a
// later escalation still commits its acked JWT — it is not suppressed by the stale
// override. A refresh after carries the escalated JWT, not the UpdateToken'd one.
func TestStaticUpdateTokenThenEscalateCommits(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	c, fc := authClient(t, f, WithToken("jwt-orig"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.UpdateToken("jwt-update"); err != nil { // leaves an override on a static client
		t.Fatalf("UpdateToken: %v", err)
	}
	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	// A refresh after the escalation's ack reads the store: it must hold the
	// escalated JWT (committed), not the UpdateToken'd one (which the escalation,
	// a later caller action, superseded).
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
	if raw := f.framesOfType(typeAuth)[1].raw; !strings.Contains(raw, "jwt-esc") {
		t.Errorf("post-escalation refresh = %s, want the committed escalation JWT (not suppressed by the stale override)", raw)
	}
}

// TestOverrideSurvivesDialFailure pins the F2 fix on the dial path: the override
// is consumed only on handshake SUCCESS, not on a dial attempt — so a dial whose
// handshake fails does NOT lose the caller's credential to a resumed TokenSource
// fetch; the reconnect dial still presents it.
func TestOverrideSurvivesDialFailure(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch(), ackEpoch())
	f.rejectUpgrade(503, "unavailable") // the first handshake fails (reconnect-class)
	fc := newFakeClock()
	var calls atomic.Int64
	ts := func(context.Context) (Token, error) { return Token{Value: fmt.Sprintf("ts-%d", calls.Add(1))}, nil }
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithTokenSource(ts))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.UpdateToken("caller-tok"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	_ = c.Connect(context.Background()) // first dial presents caller-tok, but the handshake is rejected
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Close(ctx)
		ec.waitClosed(t)
	}()

	waitForState(t, c, StateReconnecting)
	f.rejectUpgrade(0, "") // let the reconnect succeed
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)
	// The failed dial did not consume the override — the reconnect dial still
	// presents caller-tok, not a resumed TokenSource fetch.
	if got := f.authAt(2).authorization; got != "Bearer caller-tok" {
		t.Errorf("reconnect dial Authorization = %q, want caller-tok (override must survive a dial failure)", got)
	}
}

// TestConnectedOverrideSurvivesUnansweredEpoch pins the residual fix: the
// connected path consumes the override at ANSWER time, not send time. A refresh
// that presents the override but whose epoch dies before any ack does NOT consume
// it — the override survives to the reconnect rather than being lost to a resumed
// TokenSource fetch.
func TestConnectedOverrideSurvivesUnansweredEpoch(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{closeAfterFrames: 1, closeAfter: 1011}, // E1 drops after the refresh frame, no ack
		epochScript{}, // E2 healthy
	)
	fc := newFakeClock()
	var calls atomic.Int64
	ts := func(context.Context) (Token, error) { return Token{Value: fmt.Sprintf("ts-%d", calls.Add(1))}, nil }
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithTokenSource(ts))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil { // pre-dial fetch ts-1 (no override yet)
		t.Fatalf("Connect: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Close(ctx)
		ec.waitClosed(t)
	}()

	if err := c.UpdateToken("caller-tok"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	// A refresh presents caller-tok (the send succeeds — the frame reaches the
	// server) but E1 drops before answering it.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	if raw := f.framesOfType(typeAuth)[0].raw; !strings.Contains(raw, "caller-tok") {
		t.Errorf("refresh = %s, want caller-tok", raw)
	}

	// Reconnect: the override was never answered, so it survived — the reconnect
	// dial presents caller-tok, not a resumed TokenSource fetch.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	if got := f.authAt(2).authorization; got != "Bearer caller-tok" {
		t.Errorf("reconnect dial = %q, want caller-tok (override survives an unanswered refresh)", got)
	}
}

// TestEscalateThenUpdateTokenLatestWins pins the F1 fix at the integration level:
// an Escalate whose ack lands AFTER a subsequent UpdateToken does not clobber the
// UpdateToken (the later caller action wins), because the commit is gen-guarded at
// the Escalate CALL. The escalation is floored behind a prior auth, giving a
// deterministic window to call UpdateToken before the escalation's ack.
func TestEscalateThenUpdateTokenLatestWins(t *testing.T) {
	f := newFakeWS(t)
	f.script(ackEpoch())
	var calls atomic.Int64
	ts := func(context.Context) (Token, error) { return Token{Value: fmt.Sprintf("ts-%d", calls.Add(1))}, nil }
	// A long heartbeat keeps the several RefreshMinInterval advances below from
	// tripping the heartbeat + pong timeout and dropping the epoch mid-test.
	c, fc := authClient(t, f, WithTokenSource(ts), WithHeartbeatInterval(time.Hour))
	if err := c.Connect(context.Background()); err != nil { // pre-dial ts-1
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	// Prime lastAuthSent so the escalation floors (an immediate escalation would send
	// before we could interleave UpdateToken). This first refresh is itself
	// fetch-paced behind the pre-dial fetch, so advance past that floor.
	if err := c.RefreshToken(context.Background()); err != nil { // fetch ts-2, sets lastAuthSent
		t.Fatalf("RefreshToken: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 1)

	// Escalate captures the current generation, then floors. UpdateToken bumps the
	// generation while the escalation waits — so its ack must NOT commit.
	if err := c.Escalate(context.Background(), "jwt-esc"); err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	if err := c.UpdateToken("jwt-update"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	// Release the floor: the escalation sends jwt-esc and is acked, but the commit
	// no-ops (a newer UpdateToken intervened).
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
	if raw := f.framesOfType(typeAuth)[1].raw; !strings.Contains(raw, "jwt-esc") {
		t.Errorf("escalation frame = %s, want jwt-esc", raw)
	}

	// The next refresh presents the UpdateToken credential (it won), not the escalated one.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken #2: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 3)
	raw := f.framesOfType(typeAuth)[2].raw
	if !strings.Contains(raw, "jwt-update") {
		t.Errorf("post-escalation refresh = %s, want jwt-update (the later UpdateToken wins)", raw)
	}
	if strings.Contains(raw, "jwt-esc") {
		t.Errorf("post-escalation refresh = %s, must not carry the escalation JWT (its commit was superseded)", raw)
	}
}

// TestOverrideConsumedAfterHandshake pins the other side: once a dial that
// presented the override completes its handshake, the override is consumed — a
// later reconnect resumes the TokenSource rather than reusing the caller's token.
func TestOverrideConsumedAfterHandshake(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{closeAfter: 1011}, // E1 connects (handshake OK → override consumed) then drops
		epochScript{},                 // E2 healthy
	)
	fc := newFakeClock()
	var calls atomic.Int64
	ts := func(context.Context) (Token, error) { return Token{Value: fmt.Sprintf("ts-%d", calls.Add(1))}, nil }
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithTokenSource(ts))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.UpdateToken("caller-tok"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	_ = c.Connect(context.Background()) // dial 1 presents caller-tok; handshake OK consumes the override
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Close(ctx)
		ec.waitClosed(t)
	}()

	if got := f.authAt(1).authorization; got != "Bearer caller-tok" {
		t.Errorf("first dial = %q, want caller-tok", got)
	}
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	// The override was consumed on E1's handshake — the reconnect resumes TokenSource.
	if got := f.authAt(2).authorization; !strings.HasPrefix(got, "Bearer ts-") {
		t.Errorf("reconnect dial = %q, want a resumed TokenSource fetch (override consumed on handshake)", got)
	}
}
