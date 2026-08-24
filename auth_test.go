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
