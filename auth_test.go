package sukko

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// authClient builds a fakeWS-pointed client with the given auth options (no
// WithNoAuth), so the dial carries a real credential. It returns the fake clock
// so a test can advance past the RefreshMinInterval floor between auth sends.
func authClient(t *testing.T, f *fakeWS, opts ...Option) (*Client, *fakeClock) {
	t.Helper()
	fc := newFakeClock()
	base := []Option{WithHTTPClient(f.client()), WithClock(fc)}
	c, err := NewClient(context.Background(), f.URL(), append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, fc
}

// connectThenClose connects, waits for the handshake, and closes cleanly — enough
// to drive exactly one dial for an auth assertion.
func connectThenClose(t *testing.T, c *Client) {
	t.Helper()
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestDialAppliesCredential covers the credential-at-dial matrix: a JWT rides the
// Authorization header, an API key the X-API-Key header, and WithQueryParamAuth
// moves both to query params — the sibling-SDK behavior the gateway expects.
func TestDialAppliesCredential(t *testing.T) {
	tests := []struct {
		name         string
		opts         []Option
		wantAuthz    string
		wantAPIKeyHd string
		wantTokenQ   string
		wantAPIKeyQ  string
	}{
		{name: "jwt header", opts: []Option{WithToken("jwt-abc")}, wantAuthz: "Bearer jwt-abc"},
		{name: "api key header", opts: []Option{WithAPIKey("key-xyz")}, wantAPIKeyHd: "key-xyz"},
		{name: "jwt + api key headers", opts: []Option{WithToken("jwt-abc"), WithAPIKey("key-xyz")}, wantAuthz: "Bearer jwt-abc", wantAPIKeyHd: "key-xyz"},
		{name: "jwt query", opts: []Option{WithToken("jwt-abc"), WithQueryParamAuth()}, wantTokenQ: "jwt-abc"},
		{name: "api key query", opts: []Option{WithAPIKey("key-xyz"), WithQueryParamAuth()}, wantAPIKeyQ: "key-xyz"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeWS(t)
			f.script(epochScript{})
			c, _ := authClient(t, f, tc.opts...)
			connectThenClose(t, c)

			got := f.authAt(1)
			if got.authorization != tc.wantAuthz {
				t.Errorf("Authorization = %q, want %q", got.authorization, tc.wantAuthz)
			}
			if got.apiKeyHeader != tc.wantAPIKeyHd {
				t.Errorf("X-API-Key = %q, want %q", got.apiKeyHeader, tc.wantAPIKeyHd)
			}
			if got.tokenQuery != tc.wantTokenQ {
				t.Errorf("?token = %q, want %q", got.tokenQuery, tc.wantTokenQ)
			}
			if got.apiKeyQuery != tc.wantAPIKeyQ {
				t.Errorf("?api_key = %q, want %q", got.apiKeyQuery, tc.wantAPIKeyQ)
			}
		})
	}
}

// TestRefreshTokenSingleFlight covers the single-flight guarantee: a refresh
// while one is already outstanding (no auth_ack yet) coalesces — the SDK sends
// exactly one `auth` frame, carrying the current token.
func TestRefreshTokenSingleFlight(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // no reply to `auth` → auth_ack withheld, the flight stays open
	c, _ := authClient(t, f, WithToken("jwt-1"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)

	// A second refresh while the first is still in flight must coalesce.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken #2: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let a would-be second send appear
	if got := len(f.framesOfType(typeAuth)); got != 1 {
		t.Errorf("auth frames = %d, want 1 (the 2nd refresh must coalesce)", got)
	}
	if raw := f.framesOfType(typeAuth)[0].raw; !strings.Contains(raw, "jwt-1") {
		t.Errorf("auth frame = %s, want it to carry the current token", raw)
	}
}

// TestRefreshTokenAckClearsInFlight covers the flight completing: an auth_ack
// clears the single-flight marker, so a subsequent refresh sends again.
func TestRefreshTokenAckClearsInFlight(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{typeAuth: {`{"type":"auth_ack","data":{"exp":0}}`}}})
	c, fc := authClient(t, f, WithToken("jwt-1"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)

	// A SINGLE second refresh must produce a second auth frame — deferred via the
	// pending flag until the auth_ack clears the flight, never dropped. A retry
	// loop here would mask the random-select coalescing bug (finding B). It is also
	// floored to RefreshMinInterval since the first auth, so advance past the floor.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken #2: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
}

// TestRefreshTokenSurvivesReconnect pins finding A: a refresh in flight when the
// connection drops must not wedge single-flight. Epoch 1 closes (reconnect-class)
// right after receiving the auth frame without answering it; after the reconnect,
// RefreshToken must send again rather than silently no-op forever.
func TestRefreshTokenSurvivesReconnect(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{closeAfterFrames: 1, closeAfter: 1011},
		epochScript{},
	)
	fc := newFakeClock()
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithToken("jwt-1"))
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

	// Refresh on epoch 1 → auth frame sent; epoch 1 then closes without answering.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)

	// Reconnect to epoch 2 and let it come fully up.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForState(t, c, StateConnected)

	// The wedge bug (inFlight stuck true) would make this a permanent no-op. The
	// send is floored to RefreshMinInterval since epoch 1's auth, so advance past
	// the floor to see it go out on epoch 2.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken after reconnect: %v", err)
	}
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(DefaultRefreshMinInterval)
	waitForFrameCount(t, f, typeAuth, 2)
	auths := f.framesOfType(typeAuth)
	if last := auths[len(auths)-1]; last.epoch != 2 {
		t.Errorf("second auth frame on epoch %d, want 2 (post-reconnect)", last.epoch)
	}
}

// TestRefreshArmsProactivelyFromAckExp covers proactive arming: an auth_ack's
// expiry schedules the next refresh at exp − RefreshLead, so the SDK rotates the
// credential automatically before it expires. It also pins the *Authenticated
// surfaced for a successful refresh.
func TestRefreshArmsProactivelyFromAckExp(t *testing.T) {
	fc := newFakeClock()
	now := fc.Now()
	exp := now.Add(time.Hour).Unix() // the credential expires in one hour
	ackFrame := fmt.Sprintf(`{"type":"auth_ack","data":{"exp":%d}}`, exp)

	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{typeAuth: {ackFrame}}})
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithToken("jwt-1"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	// One refresh gets an auth_ack{exp}; the owner arms the proactive timer.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)

	// Advance to exp − RefreshLead (floored). The proactive timer fires and the
	// SDK refreshes on its own — a second auth frame with no caller call.
	fc.BlockUntilTimer(purposeRefresh)
	refreshAt := time.Unix(exp, 0).Add(-DefaultRefreshLead)
	if floor := now.Add(DefaultRefreshMinInterval); refreshAt.Before(floor) {
		refreshAt = floor
	}
	fc.Advance(refreshAt.Sub(now))
	waitForFrameCount(t, f, typeAuth, 2)

	var authed int
	for _, ev := range ec.snapshot() {
		if a, ok := ev.(*Authenticated); ok && a.Mode == AuthRefresh {
			authed++
		}
	}
	if authed < 1 {
		t.Errorf("*Authenticated count = %d, want ≥1 for the successful refresh", authed)
	}
}

// TestRefreshArmsReactivelyFromAuthError covers the reactive path: an auth_error
// schedules a refresh (floored by RefreshMinInterval so an auth_error→refresh
// storm is impossible), and *AuthError is surfaced in-band.
func TestRefreshArmsReactivelyFromAuthError(t *testing.T) {
	fc := newFakeClock()
	f := newFakeWS(t)
	f.script(epochScript{onConnect: []string{`{"type":"auth_error","data":{"code":"token_expired","message":"expired"}}`}})
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithToken("jwt-1"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	// The auth_error arms a reactive refresh; fire it and confirm an auth frame.
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(time.Second)
	waitForFrameCount(t, f, typeAuth, 1)

	var sawErr bool
	for _, ev := range ec.snapshot() {
		if _, ok := ev.(*AuthError); ok {
			sawErr = true
		}
	}
	if !sawErr {
		t.Error("no *AuthError surfaced for the auth_error frame")
	}
}

// TestRefreshWantedWhileDisconnectedSendsOnReconnect pins finding 1's fix: a
// refresh wanted while there is no live socket (here a RefreshToken during
// backoff; in production a proactive timer firing in the reconnect gap) is not
// lost — the epoch-up signal retries it once the new connection is up.
func TestRefreshWantedWhileDisconnectedSendsOnReconnect(t *testing.T) {
	f := newFakeWS(t)
	f.script(
		epochScript{closeAfter: 1011}, // epoch 1 closes immediately (reconnect-class)
		epochScript{},                 // epoch 2 is healthy
	)
	fc := newFakeClock()
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithToken("jwt-1"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	_ = c.Connect(context.Background())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Close(ctx)
		ec.waitClosed(t)
	}()

	// Epoch 1 dropped; the client is reconnecting. A refresh now cannot send
	// (no live socket) and must be remembered, not dropped.
	waitForState(t, c, StateReconnecting)
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}

	// Reconnect: the epoch-up signal retries the wanted refresh on epoch 2.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	waitForFrameCount(t, f, typeAuth, 1)
	if got := f.framesOfType(typeAuth)[0].epoch; got != 2 {
		t.Errorf("auth frame on epoch %d, want 2 (retried on reconnect)", got)
	}
}

// TestTokenSourceFetchThenSend covers the connected-refresh fetch: a TokenSource
// client fetches a fresh credential and sends THAT (not the stored dial token) in
// the auth frame.
func TestTokenSourceFetchThenSend(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	ts := func(context.Context) (Token, error) { return Token{Value: "fetched-jwt"}, nil }
	c, _ := authClient(t, f, WithToken("static-jwt"), WithTokenSource(ts))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)
	if raw := f.framesOfType(typeAuth)[0].raw; !strings.Contains(raw, "fetched-jwt") {
		t.Errorf("auth frame = %s, want it to carry the fetched token", raw)
	}
	// The dial used the static token; the refresh used the fetched one.
	if got := f.authAt(1).authorization; got != "Bearer static-jwt" {
		t.Errorf("dial Authorization = %q, want the static token", got)
	}
}

// TestTokenSourceFailureSurfacesAndRetries covers a single fetch failure: no auth
// frame, an in-band *TokenSourceError{Attempt:1}, and a retry armed (not terminal).
func TestTokenSourceFailureSurfacesAndRetries(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	ts := func(context.Context) (Token, error) { return Token{}, errors.New("token endpoint down") }
	c, _ := authClient(t, f, WithToken("static-jwt"), WithTokenSource(ts), WithRand(newFakeRand()))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	tse := waitForTokenSourceError(t, ec)
	if tse.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", tse.Attempt)
	}
	if got := len(f.framesOfType(typeAuth)); got != 0 {
		t.Errorf("auth frames = %d, want 0 (the fetch failed)", got)
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil (one failure is non-terminal)", err)
	}
}

// TestTokenSourceExhaustionTerminates covers the connected-refresh 5-strike: after
// MaxTokenSourceAttempts consecutive fetch failures the client terminates with
// ErrTokenSourceFailed, having surfaced one *TokenSourceError per attempt.
func TestTokenSourceExhaustionTerminates(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	ts := func(context.Context) (Token, error) { return Token{}, errors.New("token endpoint down") }
	// Lengthen the heartbeat so the 30s retry-floor advances below don't trip the
	// 30s heartbeat + pong timeout and force a reconnect mid-exhaustion.
	c, fc := authClient(t, f, WithToken("static-jwt"), WithTokenSource(ts),
		WithRand(newFakeRand()), WithHeartbeatInterval(time.Hour))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// The RefreshToken fails once; drive the remaining retries to exhaustion.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	for range MaxTokenSourceAttempts - 1 {
		fc.BlockUntilTimer(purposeRefresh)
		fc.Advance(DefaultRefreshMinInterval)
	}

	ec.waitClosed(t)
	if err := c.Err(); !errors.Is(err, ErrTokenSourceFailed) {
		t.Errorf("Err() = %v, want ErrTokenSourceFailed", err)
	}
	if got := c.State(); got != StateError {
		t.Errorf("State() = %v, want error", got)
	}
	var term *Terminal
	var tseCount int
	for _, ev := range ec.snapshot() {
		switch e := ev.(type) {
		case *Terminal:
			term = e
		case *TokenSourceError:
			tseCount++
		}
	}
	if term == nil || !errors.Is(term.Err, ErrTokenSourceFailed) {
		t.Errorf("Terminal.Err = %v, want ErrTokenSourceFailed", term)
	}
	if tseCount != MaxTokenSourceAttempts {
		t.Errorf("*TokenSourceError count = %d, want %d", tseCount, MaxTokenSourceAttempts)
	}
}

// TestTokenSourceNotFetchedWhileDisconnected pins finding 1: with no live socket
// (during a reconnect backoff) the owner must NOT invoke TokenSource — a
// disconnected fetch failure would burn a connected-path exhaustion strike during
// a backoff FR-005 says is non-terminal.
func TestTokenSourceNotFetchedWhileDisconnected(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{closeAfter: 1011}, epochScript{}) // epoch 1 drops immediately
	fetched := make(chan struct{}, 4)
	ts := func(context.Context) (Token, error) {
		fetched <- struct{}{}
		return Token{Value: "fetched"}, nil
	}
	c, _ := authClient(t, f, WithToken("static"), WithTokenSource(ts), WithRand(newFakeRand()))
	ec := collectEvents(c)
	_ = c.Connect(context.Background())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Close(ctx)
		ec.waitClosed(t)
	}()

	waitForState(t, c, StateReconnecting)
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	select {
	case <-fetched:
		t.Fatal("TokenSource invoked while disconnected")
	case <-time.After(200 * time.Millisecond): // correct: no fetch without a live socket
	}
}

// TestTokenSourcePanicContained pins finding 2: a panicking TokenSource is
// contained (it does not crash the process) and counts as a failure — driven to
// exhaustion it terminates with ErrTokenSourceFailed rather than silently killing
// the refresh subsystem.
func TestTokenSourcePanicContained(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	ts := func(context.Context) (Token, error) { panic("token source boom") }
	c, fc := authClient(t, f, WithToken("static"), WithTokenSource(ts),
		WithRand(newFakeRand()), WithHeartbeatInterval(time.Hour))
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	for range MaxTokenSourceAttempts - 1 {
		fc.BlockUntilTimer(purposeRefresh)
		fc.Advance(DefaultRefreshMinInterval)
	}

	ec.waitClosed(t)
	if err := c.Err(); !errors.Is(err, ErrTokenSourceFailed) {
		t.Errorf("Err() = %v, want ErrTokenSourceFailed (panic contained + counted)", err)
	}
}

// TestTokenSourceExpiryArmsProactiveWithoutAck pins the Token.Expiry arming: a
// fetched Token{Expiry} schedules the next proactive refresh with no auth_ack from
// the server — the handshake sends none, so Token.Expiry is a TokenSource client's
// only way to refresh ahead of expiry.
func TestTokenSourceExpiryArmsProactiveWithoutAck(t *testing.T) {
	f := newFakeWS(t)
	// The server acks (clearing the flight) but with exp:0 — "never/unknown" — so
	// the proactive SCHEDULE can only come from Token.Expiry, not auth_ack.exp.
	f.script(epochScript{respond: map[string][]string{typeAuth: {`{"type":"auth_ack","data":{"exp":0}}`}}})
	// The fake clock's base is 2026-01-01 UTC; expire one hour out.
	expiry := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	ts := func(context.Context) (Token, error) { return Token{Value: "tok", Expiry: expiry}, nil }
	c, fc := authClient(t, f, WithToken("static"), WithTokenSource(ts),
		WithRand(newFakeRand()), WithHeartbeatInterval(time.Hour))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	// Bootstrap the first fetch: it stores Token.Expiry and arms the proactive timer.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 1)

	// Advancing to Token.Expiry − RefreshLead fires the proactive schedule and the
	// SDK refreshes on its own — with no auth_ack ever received.
	fc.BlockUntilTimer(purposeRefresh)
	fc.Advance(expiry.Add(-DefaultRefreshLead).Sub(fc.Now()))
	waitForFrameCount(t, f, typeAuth, 2)
}

// waitForTokenSourceError polls the collected events for a *TokenSourceError.
func waitForTokenSourceError(t *testing.T, ec *eventCollector) *TokenSourceError {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		for _, ev := range ec.snapshot() {
			if e, ok := ev.(*TokenSourceError); ok {
				return e
			}
		}
		select {
		case <-deadline:
			t.Fatal("no *TokenSourceError surfaced")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// waitForState polls until the client reaches the given connection state.
func waitForState(t *testing.T, c *Client, want ConnectionState) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for c.State() != want {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for state %v (have %v)", want, c.State())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// TestQueryParamDialErrorRedactsToken pins the §IX fix: a failed dial under
// WithQueryParamAuth returns Go's *url.Error, which embeds the dial URL with the
// token in it — and the SDK must redact that before the error reaches the caller.
func TestQueryParamDialErrorRedactsToken(t *testing.T) {
	const secret = "supersecretjwt1234567890"
	// A wss:// URL to a closed port: the dial fails fast with a *url.Error whose
	// message embeds the URL — and WithQueryParamAuth put the token in that URL.
	c, err := NewClient(context.Background(), "wss://127.0.0.1:1/ws",
		WithToken(secret), WithQueryParamAuth(), WithClock(newFakeClock()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connErr := c.Connect(ctx)
	if connErr == nil {
		t.Fatal("Connect to a closed port returned nil, want a dial error")
	}
	if strings.Contains(connErr.Error(), secret) {
		t.Errorf("dial error leaked the token: %q", connErr.Error())
	}
	if !strings.Contains(connErr.Error(), redactedMarker) {
		t.Errorf("dial error was not redacted (no %q marker): %q", redactedMarker, connErr.Error())
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer closeCancel()
	_ = c.Close(closeCtx)
}

// TestNoAuthSendsNoCredential confirms WithNoAuth() dials bare — no Authorization,
// no X-API-Key, no query credential.
func TestNoAuthSendsNoCredential(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c := newTestClient(t, f) // uses WithNoAuth()
	connectThenClose(t, c)

	got := f.authAt(1)
	if got != (capturedAuth{}) {
		t.Errorf("WithNoAuth dial carried a credential: %+v", got)
	}
}

// TestUpdateTokenRotatesCredential covers UpdateToken: it stores a JWT (returning
// an error on an empty one) that the NEXT dial picks up — the rotation path the
// dial reads per-connect.
func TestUpdateTokenRotatesCredential(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{closeAfter: 1011}) // epoch 1 closes → a reconnect dials again

	fc := newFakeClock()
	c, err := NewClient(context.Background(), f.URL(),
		WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()), WithToken("jwt-old"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ec := collectEvents(c)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := c.UpdateToken(""); err == nil {
		t.Error("UpdateToken(\"\") = nil, want an error on an empty token")
	}
	if err := c.UpdateToken("jwt-new"); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}

	// Force the reconnect and assert the second dial carries the rotated token.
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)

	if got := f.authAt(1).authorization; got != "Bearer jwt-old" {
		t.Errorf("first dial Authorization = %q, want Bearer jwt-old", got)
	}
	if got := f.authAt(2).authorization; got != "Bearer jwt-new" {
		t.Errorf("second dial Authorization = %q, want Bearer jwt-new (rotated)", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = c.Close(ctx)
	ec.waitClosed(t)
}
