package sukko

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// The transport tests exercise the WebSocket transport directly against fakeWS,
// with no client above it — dial, read, send, close-direction, handshake
// capture, and the read limit. Everything the supervisor will later rely on is
// pinned here first.

// wsTransportFor builds a transport pointed at a fakeWS, dialing through its
// TLS client. A generous MaxPublishSize keeps the read limit out of the way
// except in the test that targets it.
func wsTransportFor(t *testing.T, f *fakeWS) *wsTransport {
	t.Helper()
	cfg := defaultConfig()
	cfg.httpClient = f.client()
	return newWSTransport(f.URL(), cfg, nil)
}

func TestWSTransportDialsAndReads(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{onConnect: []string{`{"type":"pong","ts":1}`}})

	tr := wsTransportFor(t, f)
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "done") }()

	got, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !strings.Contains(string(got), `"type":"pong"`) {
		t.Errorf("read %q, want the scripted pong frame", got)
	}
}

func TestWSTransportSendIsRecorded(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // no onConnect; the server just records what we send

	tr := wsTransportFor(t, f)
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "done") }()

	frame := []byte(`{"type":"subscribe","data":{"channels":["acme.x"]}}`)
	if err := conn.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// The server records frames asynchronously; give the fixture a moment by
	// reading back through its typed accessor with a short bounded wait.
	if !waitForFrame(t, f, "subscribe") {
		t.Errorf("server did not record the subscribe frame; got %d frames", len(f.frames()))
	}
}

func TestWSTransportRemoteCloseCarriesCodeAndDirection(t *testing.T) {
	f := newFakeWS(t)
	// The server accepts, then closes with 1008 (policy violation) — the
	// slow-client verdict the SDK must classify as a remote close.
	f.script(epochScript{closeAfter: websocket.StatusPolicyViolation, closeReason: "too slow"})

	tr := wsTransportFor(t, f)
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "done") }()

	_, err = conn.Read(context.Background())
	var ce *CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("Read error = %v (%T), want *CloseError", err, err)
	}
	if ce.Code != int(websocket.StatusPolicyViolation) {
		t.Errorf("code = %d, want 1008", ce.Code)
	}
	if ce.Direction != directionRemote {
		t.Errorf("direction = %v, want remote", ce.Direction)
	}
	if ce.Reason != "too slow" {
		t.Errorf("reason = %q, want %q — the close frame's reason must survive", ce.Reason, "too slow")
	}
}

// TestWSTransportAbnormalDropIsLocal1006 covers the third Read outcome: the TCP
// connection vanishes with no close frame while the read context is live. That
// is a library-synthesized abnormal closure, which the SDK classifies as a local
// 1006 (its own view that the connection disappeared), not a remote close.
func TestWSTransportAbnormalDropIsLocal1006(t *testing.T) {
	f := newFakeWS(t)
	// The server tears the socket down abruptly, with no close frame.
	f.script(epochScript{dropRaw: true})

	tr := wsTransportFor(t, f)
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "done") }()

	_, err = conn.Read(context.Background())
	var ce *CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("Read error = %v (%T), want *CloseError for an abnormal drop", err, err)
	}
	if ce.Code != int(websocket.StatusAbnormalClosure) {
		t.Errorf("code = %d, want 1006 (abnormal closure)", ce.Code)
	}
	if ce.Direction != directionLocal {
		t.Errorf("direction = %v, want local — a synthesized 1006 is the client's own view", ce.Direction)
	}
}

func TestWSTransportContextCancelIsLocal(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{}) // accept and idle — no frames, no close

	tr := wsTransportFor(t, f)
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "done") }()

	// A Read whose context is canceled returns the wrapped ctx error, not a
	// CloseError: the caller initiated it and already knows why.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = conn.Read(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Read error = %v, want a wrapped context.Canceled", err)
	}
	var ce *CloseError
	if errors.As(err, &ce) {
		t.Error("a canceled read surfaced a *CloseError; it should be the ctx error")
	}
}

func TestWSTransportHandshakeErrorCapturesStatusCodeAndRetryAfter(t *testing.T) {
	f := newFakeWS(t)
	f.setUpgradeHeader("Retry-After", "3")
	f.rejectUpgrade(429, `{"code":"TENANT_LIMIT_EXCEEDED","message":"too many connections"}`)

	tr := wsTransportFor(t, f)
	_, err := tr.Open(context.Background())

	var he *HandshakeError
	if !errors.As(err, &he) {
		t.Fatalf("Open error = %v (%T), want *HandshakeError", err, err)
	}
	if he.Status != 429 {
		t.Errorf("status = %d, want 429", he.Status)
	}
	if he.Code != "TENANT_LIMIT_EXCEEDED" {
		t.Errorf("code = %q, want TENANT_LIMIT_EXCEEDED", he.Code)
	}
	if he.RetryAfter == nil || *he.RetryAfter != 3*time.Second {
		t.Errorf("retryAfter = %v, want 3s", he.RetryAfter)
	}
}

func TestWSTransportHandshakeErrorAbsentRetryAfterIsNil(t *testing.T) {
	f := newFakeWS(t)
	f.rejectUpgrade(403, `{"code":"FORBIDDEN","message":"nope"}`)

	tr := wsTransportFor(t, f)
	_, err := tr.Open(context.Background())

	var he *HandshakeError
	if !errors.As(err, &he) {
		t.Fatalf("want *HandshakeError, got %v", err)
	}
	if he.RetryAfter != nil {
		t.Errorf("retryAfter = %v, want nil (header absent)", he.RetryAfter)
	}
}

// TestWSTransportReadLimitDerivedFromMaxPublishSize is T087: a message at the
// configured payload size arrives intact rather than tripping the read limit and
// closing with 1009. Proven both ways — one under the derived limit succeeds,
// one over it fails.
func TestWSTransportReadLimitDerivedFromMaxPublishSize(t *testing.T) {
	const payloadSize = 64 * 1024 // a full-size payload

	// A message frame carrying a payloadSize-byte data blob.
	bigFrame := `{"type":"message","channel":"acme.x","seq":1,"ts":1,"data":"` +
		strings.Repeat("x", payloadSize) + `"}`

	t.Run("at the limit is delivered intact", func(t *testing.T) {
		f := newFakeWS(t)
		f.script(epochScript{onConnect: []string{bigFrame}})

		cfg := defaultConfig()
		cfg.httpClient = f.client()
		cfg.maxPublishSize = payloadSize // read limit = this + envelope overhead
		tr := newWSTransport(f.URL(), cfg, nil)

		conn, err := tr.Open(context.Background())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "done") }()

		got, err := conn.Read(context.Background())
		if err != nil {
			t.Fatalf("a message at the payload size was not delivered: %v", err)
		}
		if len(got) < payloadSize {
			t.Errorf("read %d bytes, want the full %d-plus-envelope frame", len(got), payloadSize)
		}
	})

	t.Run("beyond the limit is rejected", func(t *testing.T) {
		f := newFakeWS(t)
		f.script(epochScript{onConnect: []string{bigFrame}})

		cfg := defaultConfig()
		cfg.httpClient = f.client()
		// A tiny payload limit means the big frame exceeds the read limit.
		cfg.maxPublishSize = 1024
		tr := newWSTransport(f.URL(), cfg, nil)

		conn, err := tr.Open(context.Background())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "done") }()

		if _, err := conn.Read(context.Background()); err == nil {
			t.Error("an oversize frame was accepted; the read limit did not apply")
		}
	})
}

func TestWSTransportCapabilities(t *testing.T) {
	f := newFakeWS(t)
	tr := wsTransportFor(t, f)
	got := tr.Capabilities()
	want := Capabilities{CanPublish: true, CanSubscribeLive: true, CanRefreshInPlace: true}
	if got != want {
		t.Errorf("WS capabilities = %+v, want %+v", got, want)
	}
}

func TestSSECapabilitiesAreReceiveOnly(t *testing.T) {
	// The SSE transport lands in Phase 11, but its capability answer is pinned
	// now: receive-only, so the client can pre-check a publish locally.
	got := capabilitiesFor(TransportSSE)
	if got != (Capabilities{}) {
		t.Errorf("SSE capabilities = %+v, want all-false (receive-only)", got)
	}
}

// waitForFrame polls the fixture's typed frame accessor for a bounded time. The
// server records asynchronously, so a brief wait is the honest way to observe a
// send landed — no fake clock is involved on the transport's own I/O path.
func waitForFrame(t *testing.T, f *fakeWS, typ string) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(f.framesOfType(typ)) > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
