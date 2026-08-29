package sukko

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A Web Push registration reaches POST /api/v1/push/subscribe with the platform,
// endpoint, keys, and channels, and the 201 body's device_id comes back as int64.
func TestPushSubscribeWeb(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{status: 201, body: `{"device_id":12345}`})
	c := restClient(t, g, WithToken("jwt-abc"))

	id, err := c.PushSubscribe(context.Background(), PushSubscribeOptions{
		Platform:   PlatformWeb,
		Endpoint:   "https://fcm.googleapis.com/fcm/send/abc",
		P256dhKey:  "BNcR",
		AuthSecret: "tBH",
		Channels:   []string{"acme.general.messages"},
	})
	if err != nil {
		t.Fatalf("PushSubscribe: %v", err)
	}
	if id != 12345 {
		t.Errorf("device id = %d, want 12345", id)
	}
	reqs := g.requests()
	if len(reqs) != 1 || reqs[0].method != "POST" || reqs[0].path != pushSubscribePath {
		t.Fatalf("request = %+v, want POST %s", reqs, pushSubscribePath)
	}
	if reqs[0].auth != "Bearer jwt-abc" {
		t.Errorf("Authorization = %q, want Bearer jwt-abc", reqs[0].auth)
	}
	for _, want := range []string{`"platform":"web"`, `"endpoint":"https://fcm`, `"p256dh_key":"BNcR"`, `"auth_secret":"tBH"`, `"acme.general.messages"`} {
		if !strings.Contains(reqs[0].body, want) {
			t.Errorf("body missing %s: %s", want, reqs[0].body)
		}
	}
}

// An android registration carries the device token instead of the Web Push keys.
func TestPushSubscribeAndroid(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{status: 201, body: `{"device_id":7}`})
	c := restClient(t, g, WithToken("jwt"))

	id, err := c.PushSubscribe(context.Background(), PushSubscribeOptions{
		Platform: PlatformAndroid,
		Token:    "fcm-tok-123",
		Channels: []string{"acme.x"},
	})
	if err != nil {
		t.Fatalf("PushSubscribe: %v", err)
	}
	if id != 7 {
		t.Errorf("device id = %d, want 7", id)
	}
	body := g.requests()[0].body
	if !strings.Contains(body, `"platform":"android"`) || !strings.Contains(body, `"token":"fcm-tok-123"`) {
		t.Errorf("body = %s, want platform android + token", body)
	}
}

// device_id is an int64 on the wire and must decode exactly above 2^53 — the value
// that would be lossy if it were ever routed through a float64 (the injection the js
// SDK avoids with BigInt; Go decodes it natively).
func TestPushSubscribeDeviceIDInt64(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{status: 201, body: `{"device_id":9007199254740993}`})
	c := restClient(t, g, WithToken("jwt"))

	id, err := c.PushSubscribe(context.Background(), PushSubscribeOptions{
		Platform: PlatformAndroid, Token: "t", Channels: []string{"acme.x"},
	})
	if err != nil {
		t.Fatalf("PushSubscribe: %v", err)
	}
	if id != 9007199254740993 {
		t.Errorf("device id = %d, want 9007199254740993 (exact int64, not float-rounded)", id)
	}
}

// Client-side validation fires before any network I/O: a malformed option set yields
// ErrInvalidPushOptions and sends zero requests.
func TestPushSubscribeValidation(t *testing.T) {
	tests := []struct {
		name string
		opts PushSubscribeOptions
	}{
		{"empty platform", PushSubscribeOptions{Channels: []string{"acme.x"}}},
		{"bad platform", PushSubscribeOptions{Platform: "windows", Token: "t", Channels: []string{"acme.x"}}},
		{"no channels", PushSubscribeOptions{Platform: PlatformAndroid, Token: "t"}},
		{"web missing endpoint", PushSubscribeOptions{Platform: PlatformWeb, P256dhKey: "k", AuthSecret: "s", Channels: []string{"acme.x"}}},
		{"web missing p256dh", PushSubscribeOptions{Platform: PlatformWeb, Endpoint: "e", AuthSecret: "s", Channels: []string{"acme.x"}}},
		{"web missing auth_secret", PushSubscribeOptions{Platform: PlatformWeb, Endpoint: "e", P256dhKey: "k", Channels: []string{"acme.x"}}},
		{"android missing token", PushSubscribeOptions{Platform: PlatformAndroid, Channels: []string{"acme.x"}}},
		{"ios missing token", PushSubscribeOptions{Platform: PlatformIOS, Channels: []string{"acme.x"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGateway(t)
			g.route(pushSubscribePath, gatewayResponse{status: 201, body: `{"device_id":1}`})
			c := restClient(t, g, WithToken("jwt"))

			_, err := c.PushSubscribe(context.Background(), tc.opts)
			if !errors.Is(err, ErrInvalidPushOptions) {
				t.Errorf("err = %v, want ErrInvalidPushOptions", err)
			}
			if n := len(g.requests()); n != 0 {
				t.Errorf("sent %d requests, want 0 (pre-check must fire first)", n)
			}
		})
	}
}

// A 403 EDITION_LIMIT names the edition the failed operation actually required: Web Push
// needs Pro, mobile (android/ios) registration needs Enterprise.
func TestPushSubscribeEditionGate(t *testing.T) {
	tests := []struct {
		name string
		opts PushSubscribeOptions
		want Edition
	}{
		{"web ⇒ pro", PushSubscribeOptions{Platform: PlatformWeb, Endpoint: "e", P256dhKey: "k", AuthSecret: "s", Channels: []string{"acme.x"}}, EditionPro},
		{"android ⇒ enterprise", PushSubscribeOptions{Platform: PlatformAndroid, Token: "t", Channels: []string{"acme.x"}}, EditionEnterprise},
		{"ios ⇒ enterprise", PushSubscribeOptions{Platform: PlatformIOS, Token: "t", Channels: []string{"acme.x"}}, EditionEnterprise},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGateway(t)
			g.route(pushSubscribePath, gatewayResponse{status: 403, body: `{"code":"EDITION_LIMIT","message":"gated"}`})
			c := restClient(t, g, WithToken("jwt"))

			_, err := c.PushSubscribe(context.Background(), tc.opts)
			var ed *EditionRequiredError
			if !errors.As(err, &ed) {
				t.Fatalf("err = %v, want *EditionRequiredError", err)
			}
			if ed.RequiredEdition != tc.want {
				t.Errorf("RequiredEdition = %q, want %q", ed.RequiredEdition, tc.want)
			}
		})
	}
}

// A 404 — the gateway runs with GATEWAY_PUSH_ENABLED=false, so the push routes are not
// registered — surfaces ErrPushDisabled on every push operation. The gateway's 404 body
// is Go's mux plain text, not the JSON envelope, so the mapping must key on status before
// attempting to parse.
func TestPushDisabled404(t *testing.T) {
	run := func(t *testing.T, path string, call func(c *Client) error) {
		g := newFakeGateway(t)
		g.route(path, gatewayResponse{status: 404, body: "404 page not found\n"})
		c := restClient(t, g, WithToken("jwt"))
		if err := call(c); !errors.Is(err, ErrPushDisabled) {
			t.Errorf("err = %v, want ErrPushDisabled", err)
		}
	}
	run(t, pushSubscribePath, func(c *Client) error {
		_, err := c.PushSubscribe(context.Background(), PushSubscribeOptions{Platform: PlatformAndroid, Token: "t", Channels: []string{"acme.x"}})
		return err
	})
	run(t, pushSubscribePath, func(c *Client) error { return c.PushUnsubscribe(context.Background(), 1) })
	run(t, pushVAPIDPath, func(c *Client) error { _, err := c.PushVAPIDKey(context.Background()); return err })
}

// A 503 — push exists but is temporarily down — surfaces ErrPushUnavailable (distinct
// from the permanent ErrPushDisabled).
func TestPushUnavailable503(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{status: 503, body: `{"code":"SERVICE_UNAVAILABLE","message":"down"}`})
	c := restClient(t, g, WithToken("jwt"))
	err := c.PushUnsubscribe(context.Background(), 1)
	if !errors.Is(err, ErrPushUnavailable) {
		t.Errorf("err = %v, want ErrPushUnavailable", err)
	}
	if errors.Is(err, ErrPushDisabled) {
		t.Error("503 must not be ErrPushDisabled (that is the 404/permanent case)")
	}
}

// Unsubscribe DELETEs the subscribe path with the device_id body and reports success on 200.
func TestPushUnsubscribe(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{status: 200, body: `{"success":true}`})
	c := restClient(t, g, WithToken("jwt"))

	if err := c.PushUnsubscribe(context.Background(), 9007199254740993); err != nil {
		t.Fatalf("PushUnsubscribe: %v", err)
	}
	req := g.requests()[0]
	if req.method != "DELETE" || req.path != pushSubscribePath {
		t.Errorf("request = %s %s, want DELETE %s", req.method, req.path, pushSubscribePath)
	}
	if !strings.Contains(req.body, `"device_id":9007199254740993`) {
		t.Errorf("body = %s, want exact int64 device_id", req.body)
	}
}

// VAPIDKey GETs the vapid-key path and returns the public key.
func TestPushVAPIDKey(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushVAPIDPath, gatewayResponse{status: 200, body: `{"public_key":"BNcRdreA"}`})
	c := restClient(t, g, WithToken("jwt"))

	key, err := c.PushVAPIDKey(context.Background())
	if err != nil {
		t.Fatalf("PushVAPIDKey: %v", err)
	}
	if key != "BNcRdreA" {
		t.Errorf("key = %q, want BNcRdreA", key)
	}
	if g.requests()[0].method != "GET" {
		t.Errorf("method = %s, want GET", g.requests()[0].method)
	}
}

// Under WithQueryParamAuth the credential rides the query string, mirroring the WS dial.
func TestPushQueryAuth(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushVAPIDPath, gatewayResponse{status: 200, body: `{"public_key":"k"}`})
	c := restClient(t, g, WithToken("jwt-q"), WithQueryParamAuth())

	if _, err := c.PushVAPIDKey(context.Background()); err != nil {
		t.Fatalf("PushVAPIDKey: %v", err)
	}
	req := g.requests()[0]
	if req.auth != "" {
		t.Errorf("Authorization header = %q, want empty (query auth)", req.auth)
	}
	if got := req.query.Get("token"); got != "jwt-q" {
		t.Errorf("query token = %q, want jwt-q", got)
	}
}

// After Close, push operations refuse with ErrClosed.
func TestPushAfterClose(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushVAPIDPath, gatewayResponse{status: 200, body: `{"public_key":"k"}`})
	c := restClient(t, g, WithToken("jwt"))
	_ = c.Close(context.Background())

	if _, err := c.PushVAPIDKey(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("err = %v, want ErrClosed", err)
	}
}

// A coded error with no special mapping surfaces *PushError carrying the gateway's code.
func TestPushGenericError(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{status: 400, body: `{"code":"INVALID_REQUEST","message":"bad"}`})
	c := restClient(t, g, WithToken("jwt"))

	_, err := c.PushSubscribe(context.Background(), PushSubscribeOptions{Platform: PlatformAndroid, Token: "t", Channels: []string{"acme.x"}})
	var pe *PushError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *PushError", err)
	}
	if pe.Code != "INVALID_REQUEST" {
		t.Errorf("Code = %q, want INVALID_REQUEST", pe.Code)
	}
}

// Push permits API-key-only callers — unlike RESTPublish, which requires a JWT. This
// guards that design decision: a refactor that folded push into the publish credential
// gate (ErrPublishRequiresJWT) would fail here, not silently pass.
func TestPushSubscribeAPIKeyOnly(t *testing.T) {
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{status: 201, body: `{"device_id":5}`})
	c := restClient(t, g, WithAPIKey("ak-123"))

	id, err := c.PushSubscribe(context.Background(), PushSubscribeOptions{
		Platform: PlatformWeb, Endpoint: "e", P256dhKey: "k", AuthSecret: "s", Channels: []string{"acme.x"},
	})
	if err != nil {
		t.Fatalf("PushSubscribe with API key only must succeed: %v", err)
	}
	if id != 5 {
		t.Errorf("device id = %d, want 5", id)
	}
	req := g.requests()[0]
	if req.apiKey != "ak-123" {
		t.Errorf("X-API-Key = %q, want ak-123", req.apiKey)
	}
	if req.auth != "" {
		t.Errorf("Authorization = %q, want empty (no JWT)", req.auth)
	}
}

// §IX: an error body that echoes the request target (which under query auth carries the
// token) must be redacted before it reaches the returned *PushError — mirrors the publish
// redaction test.
func TestPushRedactsServerEchoedToken(t *testing.T) {
	const token = "supersecrettoken1234"
	g := newFakeGateway(t)
	g.route(pushSubscribePath, gatewayResponse{
		status: 400,
		body:   `{"code":"INVALID_REQUEST","message":"rejected /api/v1/push/subscribe?token=` + token + `"}`,
	})
	c := restClient(t, g, WithToken(token), WithQueryParamAuth())

	_, err := c.PushSubscribe(context.Background(), PushSubscribeOptions{
		Platform: PlatformAndroid, Token: "t", Channels: []string{"acme.x"},
	})
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("returned error leaked the token: %v", err)
	}
}
