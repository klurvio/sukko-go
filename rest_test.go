package sukko

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRESTPublishRoundTrip is the REST happy path (T159/SC-007): a client with no
// WebSocket publishes over HTTP and gets a *PublishResult back; the request reaches the
// gateway with the JWT in the Authorization header and the payload nested under the
// contract's inner `data`.
func TestRESTPublishRoundTrip(t *testing.T) {
	g := newFakeGateway(t)
	g.route(restPublishPath, gatewayResponse{status: 200, body: `{"status":"accepted","channel":"acme.x"}`})
	c := restClient(t, g, WithToken("jwt-abc"))

	res, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("RESTPublish: %v", err)
	}
	if res.Channel != "acme.x" {
		t.Errorf("Channel = %q, want acme.x", res.Channel)
	}
	if res.Mid != "" {
		t.Errorf("Mid = %q, want empty (server omitted it)", res.Mid)
	}

	reqs := g.requests()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	if reqs[0].method != "POST" || reqs[0].path != restPublishPath {
		t.Errorf("request = %s %s, want POST %s", reqs[0].method, reqs[0].path, restPublishPath)
	}
	if reqs[0].auth != "Bearer jwt-abc" {
		t.Errorf("Authorization = %q, want Bearer jwt-abc", reqs[0].auth)
	}
	if !strings.Contains(reqs[0].body, `"channel":"acme.x"`) || !strings.Contains(reqs[0].body, `"k":"v"`) {
		t.Errorf("body = %s, want channel acme.x + inner data {k:v}", reqs[0].body)
	}
}

// TestRESTPublishQueryAuth pins the WithQueryParamAuth path: the credential rides the
// query string, not the Authorization header (mirrors the WebSocket dial).
func TestRESTPublishQueryAuth(t *testing.T) {
	g := newFakeGateway(t)
	g.route(restPublishPath, gatewayResponse{status: 200, body: `{"status":"accepted","channel":"acme.x"}`})
	c := restClient(t, g, WithToken("jwt-q"), WithQueryParamAuth())

	if _, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"}); err != nil {
		t.Fatalf("RESTPublish: %v", err)
	}
	reqs := g.requests()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	if reqs[0].auth != "" {
		t.Errorf("Authorization header set under query auth: %q", reqs[0].auth)
	}
	if got := reqs[0].query.Get("token"); got != "jwt-q" {
		t.Errorf("token query = %q, want jwt-q", got)
	}
}

// TestRESTPublishErrorTable is the FR-010 status→typed-error matrix (T157): every coded
// response maps to its typed error, with the two 403s discriminated by struct type.
func TestRESTPublishErrorTable(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		check  func(t *testing.T, err error)
	}{
		{"400 invalid request", 400, `{"code":"INVALID_REQUEST","message":"bad body"}`, wantPublishError("INVALID_REQUEST")},
		{"400 invalid channel", 400, `{"code":"INVALID_CHANNEL","message":"bad channel"}`, wantPublishError("INVALID_CHANNEL")},
		{"403 forbidden", 403, `{"code":"FORBIDDEN","message":"denied"}`, wantPublishError("FORBIDDEN")},
		{"403 edition limit", 403, `{"code":"EDITION_LIMIT","message":"needs pro"}`, wantEdition(EditionPro)},
		{"409 not routable", 409, `{"code":"PUBLISH_NOT_ROUTABLE","message":"no route"}`, wantPublishError("PUBLISH_NOT_ROUTABLE")},
		{"413 body too large", 413, `{"code":"BODY_TOO_LARGE","message":"too big"}`, wantSentinel(ErrPayloadTooLarge)},
		{"429 rate limited", 429, `{"code":"RATE_LIMITED","message":"slow down"}`, wantRateLimit},
		{"503 unavailable", 503, `{"code":"SERVICE_UNAVAILABLE","message":"down"}`, wantPublishError("SERVICE_UNAVAILABLE")},
		// A rate-limiting proxy/CDN in front of the gateway can answer with a non-JSON
		// body; the status leg still maps it to *RateLimitError.
		{"429 non-JSON body", 429, `<html>too many requests</html>`, wantRateLimit},
		// A bodyless/garbled status with no recognizable code falls to the HTTP_<status>
		// synthetic code rather than an empty *PublishError.
		{"502 HTML body", 502, `<html>bad gateway</html>`, wantPublishError("HTTP_502")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGateway(t)
			g.route(restPublishPath, gatewayResponse{status: tc.status, body: tc.body})
			c := restClient(t, g, WithToken("jwt"))
			_, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"})
			tc.check(t, err)
		})
	}
}

// TestRESTPublishRetryAfter pins Retry-After on a 429 three ways (NFR-005(ss)):
// present-non-zero ⇒ that duration, present-zero ⇒ a non-nil zero (retry now), absent ⇒
// nil — the pointer distinguishing "said nothing" from "retry immediately".
func TestRESTPublishRetryAfter(t *testing.T) {
	// The client's fake clock is pinned at 2026-01-01 00:00:00 UTC, so an HTTP-date 3s
	// ahead parses to 3s — the only form that exercises the clock seam (a value-typed
	// delta ignores now).
	httpDate := time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC).Format(http.TimeFormat)
	tests := []struct {
		name    string
		headers map[string]string
		wantNil bool
		wantDur time.Duration
	}{
		{"delta-seconds", map[string]string{"Retry-After": "3"}, false, 3 * time.Second},
		{"http-date", map[string]string{"Retry-After": httpDate}, false, 3 * time.Second},
		{"retry-after zero", map[string]string{"Retry-After": "0"}, false, 0},
		{"absent", nil, true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGateway(t)
			g.route(restPublishPath, gatewayResponse{status: 429, body: `{"code":"RATE_LIMITED","message":"slow"}`, headers: tc.headers})
			c := restClient(t, g, WithToken("jwt"))
			_, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"})

			var rle *RateLimitError
			if !errors.As(err, &rle) {
				t.Fatalf("err = %v, want *RateLimitError", err)
			}
			if tc.wantNil {
				if rle.RetryAfter != nil {
					t.Errorf("RetryAfter = %v, want nil (header absent)", *rle.RetryAfter)
				}
				return
			}
			if rle.RetryAfter == nil {
				t.Fatalf("RetryAfter = nil, want %v", tc.wantDur)
			}
			if *rle.RetryAfter != tc.wantDur {
				t.Errorf("RetryAfter = %v, want %v", *rle.RetryAfter, tc.wantDur)
			}
		})
	}
}

// TestRESTPublishRejectsBeforeRequest is the REST leg of the payload pre-check
// (T158): a non-object and an oversize payload each return the typed error with ZERO
// requests recorded — the shared pre-check fires before any network I/O.
func TestRESTPublishRejectsBeforeRequest(t *testing.T) {
	oversize := strings.Repeat("a", DefaultMaxPublishSize-7)
	tests := []struct {
		name    string
		data    any
		wantErr error
	}{
		{"non-object", []int{1, 2}, ErrPayloadNotObject},
		{"oversize", map[string]any{"d": oversize}, ErrPayloadTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGateway(t)
			g.route(restPublishPath, gatewayResponse{status: 200, body: `{"status":"accepted","channel":"acme.x"}`})
			c := restClient(t, g, WithToken("jwt"))

			if _, err := c.RESTPublish(context.Background(), "acme.x", tc.data); !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if reqs := g.requests(); len(reqs) != 0 {
				t.Errorf("a rejected publish made %d requests, want 0", len(reqs))
			}
		})
	}
}

// TestRESTPublishAPIKeyOnlyRejected is the credential-class pre-check on the REST leg
// (T106): an API-key-only client is refused before any request.
func TestRESTPublishAPIKeyOnlyRejected(t *testing.T) {
	g := newFakeGateway(t)
	g.route(restPublishPath, gatewayResponse{status: 200, body: `{"status":"accepted","channel":"acme.x"}`})
	c := restClient(t, g, WithAPIKey("k-123"))

	if _, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"}); !errors.Is(err, ErrPublishRequiresJWT) {
		t.Fatalf("err = %v, want ErrPublishRequiresJWT", err)
	}
	if reqs := g.requests(); len(reqs) != 0 {
		t.Errorf("an api-key-only publish made %d requests, want 0", len(reqs))
	}
}

// TestRESTPublishRedactsToken pins §IX/§V on the REST boundary: a transport failure
// under WithQueryParamAuth embeds the token-bearing URL in Go's *url.Error, and the
// returned error string must not leak the token. The token is ≥ minMaskableSecretLength
// so value-masking participates, not only the query-name pattern.
func TestRESTPublishRedactsToken(t *testing.T) {
	const token = "supersecrettoken1234"
	// An unreachable host forces http.Client.Do to fail with a *url.Error carrying the
	// full ?token= URL; the endpoint is never reached, so no scripted gateway is needed.
	c, err := NewClient(context.Background(), "wss://127.0.0.1:1/ws",
		WithClock(newFakeClock()), WithRand(newFakeRand()),
		WithToken(token), WithQueryParamAuth(), WithClientID("cid"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"})
	if err == nil {
		t.Fatal("RESTPublish to an unreachable host returned nil error")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("error string leaks the token: %v", err)
	}
}

// TestRESTPublishRedactsServerEchoedToken pins the body-echo leg of §IX/§V: a JSON
// error body that echoes the request target (which under query auth carries the token)
// must be redacted before it lands in the returned *PublishError.
func TestRESTPublishRedactsServerEchoedToken(t *testing.T) {
	const token = "supersecrettoken1234"
	g := newFakeGateway(t)
	g.route(restPublishPath, gatewayResponse{
		status: 400,
		body:   `{"code":"INVALID_REQUEST","message":"rejected /api/v1/publish?token=` + token + `"}`,
	})
	c := restClient(t, g, WithToken(token), WithQueryParamAuth())

	_, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"})
	if err == nil {
		t.Fatal("RESTPublish with a 400 returned nil error")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("the returned error leaks the server-echoed token: %v", err)
	}
}

// TestRESTPublishClosedClient covers the lifecycle guard: a closed client returns
// ErrClosed with no HTTP request. A NoAuth client is used so the shared credential
// pre-check passes and the closed check is what fires.
func TestRESTPublishClosedClient(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{})
	c, _ := subClient(t, f, WithClientID("cid"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	closeClient(t, c)

	if _, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"}); !errors.Is(err, ErrClosed) {
		t.Errorf("RESTPublish on a closed client = %v, want ErrClosed", err)
	}
}

// TestRESTPublishMalformed200 covers a 200 whose body does not decode: the call errors
// rather than returning a zero PublishResult.
func TestRESTPublishMalformed200(t *testing.T) {
	g := newFakeGateway(t)
	g.route(restPublishPath, gatewayResponse{status: 200, body: `{not valid json`})
	c := restClient(t, g, WithToken("jwt"))

	if _, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"}); err == nil {
		t.Error("RESTPublish with a malformed 200 body returned nil error")
	}
}

func wantPublishError(code string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		var pe *PublishError
		if !errors.As(err, &pe) {
			t.Fatalf("err = %v, want *PublishError{Code:%q}", err, code)
		}
		if pe.Code != code {
			t.Errorf("Code = %q, want %q", pe.Code, code)
		}
	}
}

func wantEdition(ed Edition) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		var ere *EditionRequiredError
		if !errors.As(err, &ere) {
			t.Fatalf("err = %v, want *EditionRequiredError", err)
		}
		if ere.RequiredEdition != ed {
			t.Errorf("RequiredEdition = %q, want %q", ere.RequiredEdition, ed)
		}
		// EDITION_LIMIT must be discriminated from FORBIDDEN — not also a *PublishError.
		var pe *PublishError
		if errors.As(err, &pe) {
			t.Errorf("EDITION_LIMIT also matched *PublishError %+v — the two 403s are not discriminated", pe)
		}
	}
}

func wantSentinel(s error) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, s) {
			t.Fatalf("err = %v, want %v", err, s)
		}
	}
}

func wantRateLimit(t *testing.T, err error) {
	t.Helper()
	var rle *RateLimitError
	if !errors.As(err, &rle) {
		t.Fatalf("err = %v, want *RateLimitError", err)
	}
}

// The 200 body's mid — the stable message identity of the published message —
// surfaces on PublishResult; absent for a multi-topic fan-out publish (or an old
// gateway), which the server signals by omitting it.
func TestRESTPublishMid(t *testing.T) {
	g := newFakeGateway(t)
	g.route(restPublishPath, gatewayResponse{status: 200, body: `{"status":"accepted","channel":"acme.x","mid":"beef-1-7"}`})
	c := restClient(t, g, WithToken("jwt-abc"))

	res, err := c.RESTPublish(context.Background(), "acme.x", map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("RESTPublish: %v", err)
	}
	if res.Mid != "beef-1-7" {
		t.Errorf("Mid = %q, want beef-1-7", res.Mid)
	}
}
