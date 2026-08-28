package sukko

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestPublishSendsFrame is the WS publish happy path (T066): a connected client
// Publishes a JSON object and the `publish` frame reaches the socket carrying the
// channel and the caller's payload nested under the contract's inner `data`.
func TestPublishSendsFrame(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // publish is fire-and-forget; no response is scripted
	c, _ := subClient(t, f, WithClientID("cid"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeClient(t, c)

	if err := c.Publish(context.Background(), "acme.x", map[string]any{"k": "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	waitForFrameCount(t, f, typePublish, 1)
	raw := f.framesOfType(typePublish)[0].raw
	if !strings.Contains(raw, `"channel":"acme.x"`) {
		t.Errorf("publish frame = %s, want channel acme.x", raw)
	}
	if !strings.Contains(raw, `"k":"v"`) {
		t.Errorf("publish frame = %s, want inner data {k:v}", raw)
	}
}

// TestPublishRejectsBeforeSend covers the reject side of the payload pre-check
// (T066/T158 WS leg): a non-object payload and an oversize payload each return the
// typed error BEFORE any byte reaches the socket — the fake records zero publish frames.
func TestPublishRejectsBeforeSend(t *testing.T) {
	// map[string]any{"d": s} marshals to `{"d":"<s>"}` == 8 + len(s) bytes, so one byte
	// over the limit is len(s) == limit-7.
	oversize := strings.Repeat("a", DefaultMaxPublishSize-7)
	tests := []struct {
		name    string
		data    any
		wantErr error
	}{
		{"a slice is not an object", []int{1, 2}, ErrPayloadNotObject},
		{"a scalar is not an object", 42, ErrPayloadNotObject},
		{"nil is not an object", nil, ErrPayloadNotObject},
		{"one byte over the limit", map[string]any{"d": oversize}, ErrPayloadTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeWS(t)
			f.script(epochScript{})
			c, _ := subClient(t, f, WithClientID("cid"))
			if err := c.Connect(context.Background()); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer closeClient(t, c)

			if err := c.Publish(context.Background(), "acme.x", tc.data); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Publish err = %v, want %v", err, tc.wantErr)
			}
			if got := f.framesOfType(typePublish); len(got) != 0 {
				t.Errorf("a rejected publish sent %d frames, want 0", len(got))
			}
		})
	}
}

// TestPrepublishCheckSizeBoundary pins the exact size boundary two-sided on the shared
// pre-check directly (a ~64 KB wire frame would exceed the fake server's read limit, so
// the accept side is asserted on the verdict, not on frame receipt): a payload marshaling
// to exactly MaxPublishSize is accepted, one byte over is rejected.
func TestPrepublishCheckSizeBoundary(t *testing.T) {
	c, _ := subClient(t, newFakeWS(t), WithClientID("cid")) // no connection needed for the pre-check
	atLimit := strings.Repeat("a", DefaultMaxPublishSize-8) // {"d":"<s>"} == 8 + len(s) == limit
	oneOver := strings.Repeat("a", DefaultMaxPublishSize-7)

	if _, err := c.prepublishCheck(map[string]any{"d": atLimit}); err != nil {
		t.Errorf("prepublishCheck at the limit = %v, want nil", err)
	}
	if _, err := c.prepublishCheck(map[string]any{"d": oneOver}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("prepublishCheck one byte over = %v, want ErrPayloadTooLarge", err)
	}
}

// TestPublishRequiresJWT is the credential-class pre-check (T066/T106 WS leg): an
// API-key-only client cannot publish (publish requires a JWT) and no frame is sent,
// while a WithNoAuth client (auth-disabled deployment) publishes freely.
func TestPublishRequiresJWT(t *testing.T) {
	t.Run("api-key-only is rejected", func(t *testing.T) {
		f := newFakeWS(t)
		f.script(epochScript{})
		fc := newFakeClock()
		c, err := NewClient(context.Background(), f.URL(),
			WithHTTPClient(f.client()), WithClock(fc), WithRand(newFakeRand()),
			WithAPIKey("k-123"), WithClientID("cid"))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		if err := c.Connect(context.Background()); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer closeClient(t, c)

		if err := c.Publish(context.Background(), "acme.x", map[string]any{"k": "v"}); !errors.Is(err, ErrPublishRequiresJWT) {
			t.Fatalf("Publish err = %v, want ErrPublishRequiresJWT", err)
		}
		if got := f.framesOfType(typePublish); len(got) != 0 {
			t.Errorf("an api-key-only publish sent %d frames, want 0", len(got))
		}
	})

	t.Run("no-auth publishes freely", func(t *testing.T) {
		f := newFakeWS(t)
		f.script(epochScript{})
		c, _ := subClient(t, f, WithClientID("cid")) // subClient is WithNoAuth
		if err := c.Connect(context.Background()); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer closeClient(t, c)

		if err := c.Publish(context.Background(), "acme.x", map[string]any{"k": "v"}); err != nil {
			t.Fatalf("Publish err = %v, want nil (no-auth)", err)
		}
		waitForFrameCount(t, f, typePublish, 1)
	})
}

// TestPrepublishCheckCredentialClass pins the JWT-PASSES side of the credential gate —
// the `token != ""` rows that TestPublishRequiresJWT (NoAuth / API-key-only, both
// token=="") leaves unexercised. Without these, the `token == ""` conjunct is never
// load-bearing and a regression to a bare `!noAuth` (reject every authenticated client)
// would stay green. A JWT client passes; an API-key-only client flips from rejected to
// accepted once UpdateToken gives it a JWT.
func TestPrepublishCheckCredentialClass(t *testing.T) {
	f := newFakeWS(t)
	newC := func(t *testing.T, cred Option) *Client {
		t.Helper()
		c, err := NewClient(context.Background(), f.URL(),
			WithHTTPClient(f.client()), WithClock(newFakeClock()), WithRand(newFakeRand()),
			cred, WithClientID("cid"))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		return c
	}

	// A JWT-bearing client passes the credential gate (token != "").
	if _, err := newC(t, WithToken("jwt-abc")).prepublishCheck(map[string]any{"k": "v"}); err != nil {
		t.Errorf("prepublishCheck with a JWT = %v, want nil", err)
	}

	// An API-key-only client is rejected until UpdateToken flips its class to JWT.
	ac := newC(t, WithAPIKey("k-123"))
	if _, err := ac.prepublishCheck(map[string]any{"k": "v"}); !errors.Is(err, ErrPublishRequiresJWT) {
		t.Errorf("prepublishCheck api-key-only = %v, want ErrPublishRequiresJWT", err)
	}
	ac.UpdateToken("jwt-xyz")
	if _, err := ac.prepublishCheck(map[string]any{"k": "v"}); err != nil {
		t.Errorf("prepublishCheck after UpdateToken = %v, want nil (now has a JWT)", err)
	}
}

// TestPublishNotConnected covers the lifecycle guards: a not-yet-connected client
// returns *NotConnectedError, and a closed client returns ErrClosed — both before any
// frame is sent.
func TestPublishNotConnected(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c, _ := subClient(t, f, WithClientID("cid"))

	var nce *NotConnectedError
	if err := c.Publish(context.Background(), "acme.x", map[string]any{"k": "v"}); !errors.As(err, &nce) {
		t.Fatalf("Publish before Connect = %v, want *NotConnectedError", err)
	}
	if got := f.framesOfType(typePublish); len(got) != 0 {
		t.Errorf("a disconnected publish sent %d frames, want 0", len(got))
	}

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	closeClient(t, c)
	if err := c.Publish(context.Background(), "acme.x", map[string]any{"k": "v"}); !errors.Is(err, ErrClosed) {
		t.Errorf("Publish after Close = %v, want ErrClosed", err)
	}
}
