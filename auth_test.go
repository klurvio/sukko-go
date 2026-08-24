package sukko

import (
	"context"
	"strings"
	"testing"
	"time"
)

// authClient builds a fakeWS-pointed client with the given auth options (no
// WithNoAuth), so the dial carries a real credential.
func authClient(t *testing.T, f *fakeWS, opts ...Option) *Client {
	t.Helper()
	base := []Option{WithHTTPClient(f.client()), WithClock(newFakeClock())}
	c, err := NewClient(context.Background(), f.URL(), append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
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
			c := authClient(t, f, tc.opts...)
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
	c := authClient(t, f, WithToken("jwt-1"))
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
	c := authClient(t, f, WithToken("jwt-1"))
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
	// loop here would mask the random-select coalescing bug (finding B).
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken #2: %v", err)
	}
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

	// The wedge bug (inFlight stuck true) would make this a permanent no-op.
	if err := c.RefreshToken(context.Background()); err != nil {
		t.Fatalf("RefreshToken after reconnect: %v", err)
	}
	waitForFrameCount(t, f, typeAuth, 2)
	auths := f.framesOfType(typeAuth)
	if last := auths[len(auths)-1]; last.epoch != 2 {
		t.Errorf("second auth frame on epoch %d, want 2 (post-reconnect)", last.epoch)
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
