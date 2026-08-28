package sukko

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// gatewayResponse scripts one route's reply: an HTTP status, a JSON body, and optional
// headers (Retry-After in particular).
type gatewayResponse struct {
	status  int
	body    string
	headers map[string]string
}

// recordedRequest is one request the fake gateway received. The auth fields let a test
// assert the credential arrived the right way (header vs query), and the whole record
// existing lets "zero requests" be asserted for a local pre-check that must fire first.
type recordedRequest struct {
	method string
	path   string
	auth   string // Authorization header
	apiKey string // X-API-Key header
	query  url.Values
	body   string
}

// fakeGateway is a TLS httptest server standing in for the Sukko gateway's REST
// surface. Routes are scripted by path; every request is recorded. It is the REST
// counterpart to fakeWS.
type fakeGateway struct {
	t        *testing.T
	server   *httptest.Server
	mu       sync.Mutex
	routes   map[string]gatewayResponse
	received []recordedRequest
}

func newFakeGateway(t *testing.T) *fakeGateway {
	g := &fakeGateway{t: t, routes: map[string]gatewayResponse{}}
	g.server = httptest.NewTLSServer(http.HandlerFunc(g.handle))
	t.Cleanup(g.server.Close)
	return g
}

// wsURL returns the ws-scheme form of the server's origin — what NewClient is given, so
// the client's REST base derives straight back to this server.
func (g *fakeGateway) wsURL() string { return "wss" + strings.TrimPrefix(g.server.URL, "https") }

// client returns an http.Client trusting the fake's TLS certificate.
func (g *fakeGateway) client() *http.Client { return g.server.Client() }

// route scripts the reply for a path (e.g. "/api/v1/publish").
func (g *fakeGateway) route(path string, resp gatewayResponse) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.routes[path] = resp
}

func (g *fakeGateway) requests() []recordedRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]recordedRequest(nil), g.received...)
}

func (g *fakeGateway) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	g.mu.Lock()
	g.received = append(g.received, recordedRequest{
		method: r.Method,
		path:   r.URL.Path,
		auth:   r.Header.Get("Authorization"),
		apiKey: r.Header.Get("X-API-Key"),
		query:  r.URL.Query(),
		body:   string(body),
	})
	resp, ok := g.routes[r.URL.Path]
	g.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"code":"NOT_FOUND","message":"no scripted route"}`)
		return
	}
	for k, v := range resp.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	status := resp.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, resp.body)
}

// restClient builds a client pointed at the fake gateway. RESTPublish needs no
// WebSocket, so the client is never connected — and NewClient starts no goroutines, so
// there is nothing to close.
func restClient(t *testing.T, g *fakeGateway, opts ...Option) *Client {
	t.Helper()
	base := []Option{
		WithHTTPClient(g.client()), WithClock(newFakeClock()), WithRand(newFakeRand()),
		WithClientID("cid"),
	}
	c, err := NewClient(context.Background(), g.wsURL(), append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}
