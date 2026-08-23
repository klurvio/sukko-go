package sukko

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeWS is the in-process server every WebSocket-facing test runs against.
//
// It is deliberately not a mock of the SDK's own transport: it is an HTTP
// server speaking the wire contract, so a test exercises the real dial, the
// real handshake, and the real frame codec. What it adds over a live gateway is
// determinism — a test scripts exactly which frames arrive, in what order, and
// what goes wrong.
//
// Two capabilities shape the design. First, scripting is **per epoch, looked up
// by dial count**: reconnect behavior is most of what this SDK does, and
// asserting it needs connect #1 and connect #2 to differ deterministically.
// Second, every frame the client sends is **recorded**, because most recovery
// assertions are about what the client did — "exactly one reconnect frame", "no
// replay was sent" — and an absence is only meaningful against a recorded total.
//
// It is served over TLS (httptest.NewTLSServer) so tests exercise the wss://
// path the SDK requires by default, rather than the insecure opt-in.
type fakeWS struct {
	t      *testing.T
	server *httptest.Server

	mu sync.Mutex
	// dials counts upgrade attempts, and indexes the per-epoch script.
	dials int
	// scripts holds per-epoch behavior; scripts[0] drives the first dial.
	// The final entry repeats for every later epoch, so a test scripting one
	// epoch does not have to enumerate reconnects it does not care about.
	scripts []epochScript
	// received holds every frame the client sent, across all epochs.
	received []recordedFrame
	// upgradeStatus, when non-zero, rejects the upgrade with this status
	// instead of accepting it.
	upgradeStatus int
	// upgradeBody is the JSON body returned with a rejected upgrade.
	upgradeBody string
	// upgradeHeaders are set on the upgrade response, rejected or not — the
	// hook for Retry-After on a 429.
	upgradeHeaders map[string]string
	// stallUpgrade, when non-nil, holds the handshake until it is closed. It is
	// what makes a mid-dial cancellation or a dial timeout testable.
	stallUpgrade chan struct{}
}

// epochScript describes one connection's behavior.
type epochScript struct {
	// onConnect is sent, in order, as soon as the connection is established.
	onConnect []string
	// respond maps an inbound message type to the frames sent in reply. A type
	// absent from the map gets no reply, which is how a withheld ack is
	// scripted.
	respond map[string][]string
	// closeAfter, when non-zero, closes the connection with this code once
	// onConnect and any triggered responses have been written.
	closeAfter websocket.StatusCode
	// closeReason accompanies closeAfter.
	closeReason string
	// closeAfterFrames, when non-zero, closes the connection once this many
	// client frames have been received — the mid-stream close.
	//
	// It is distinct from closeAfter, which fires at the first opportunity.
	// Cutting a connection *during* an exchange is a different fault from
	// refusing it: a replay interrupted halfway leaves the client having
	// received some records and no terminator, which is the case that separates
	// "recovery was interrupted" from "recovery never started".
	closeAfterFrames int
	// dropRaw, when true, tears the TCP connection down abruptly after onConnect
	// with no WebSocket close frame (coder's CloseNow) — the abnormal-closure
	// fault, distinct from closeAfter's clean close frame. The client sees a
	// synthesized 1006 rather than a coded remote close.
	dropRaw bool
}

// recordedFrame is one frame the client sent.
type recordedFrame struct {
	// epoch is the 1-based dial this frame arrived on.
	epoch int
	// typ is the message's "type" field, or "" if it had none.
	typ string
	// raw is the frame verbatim, for assertions on fields beyond the type.
	raw string
}

// newFakeWS starts a TLS-hosted fake and registers cleanup. The returned fake
// accepts connections but sends nothing until a script is installed.
func newFakeWS(t *testing.T) *fakeWS {
	t.Helper()

	f := &fakeWS{t: t, upgradeHeaders: map[string]string{}}
	f.server = httptest.NewTLSServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

// URL returns the wss:// endpoint to dial.
func (f *fakeWS) URL() string {
	return "wss" + f.server.URL[len("https"):]
}

// client returns an http.Client trusting the fake's TLS certificate. The SDK
// takes it through WithHTTPClient, which is the same seam a caller uses for a
// private CA — so the tests exercise the real configuration path.
func (f *fakeWS) client() *http.Client { return f.server.Client() }

// script installs per-epoch behavior. The last entry repeats for every epoch
// beyond those given.
func (f *fakeWS) script(scripts ...epochScript) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scripts = scripts
}

// rejectUpgrade makes the next handshake fail with an HTTP status and body,
// which is how the handshake-policy rows are exercised.
func (f *fakeWS) rejectUpgrade(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upgradeStatus = status
	f.upgradeBody = body
}

// setUpgradeHeader sets a header on the upgrade response — Retry-After on a
// 429, in practice.
func (f *fakeWS) setUpgradeHeader(key, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upgradeHeaders[key] = value
}

// stall holds every handshake until the returned release func is called, so a
// test can cancel a dial mid-flight or let a dial timeout expire.
func (f *fakeWS) stall() (release func()) {
	f.mu.Lock()
	ch := make(chan struct{})
	f.stallUpgrade = ch
	f.mu.Unlock()

	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

// dialCount reports how many upgrades have been attempted. Reconnect
// assertions are counts: "exactly one dial" is the positive form of "it did not
// retry".
func (f *fakeWS) dialCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials
}

// frames returns every frame the client has sent, in order.
func (f *fakeWS) frames() []recordedFrame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedFrame(nil), f.received...)
}

// framesOfType returns the frames whose "type" matches, which is how a test
// asserts "exactly one reconnect" or "no replay at all".
func (f *fakeWS) framesOfType(typ string) []recordedFrame {
	var out []recordedFrame
	for _, fr := range f.frames() {
		if fr.typ == typ {
			out = append(out, fr)
		}
	}
	return out
}

// handle serves one upgrade attempt.
func (f *fakeWS) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.dials++
	epoch := f.dials
	status, body := f.upgradeStatus, f.upgradeBody
	headers := make(map[string]string, len(f.upgradeHeaders))
	maps.Copy(headers, f.upgradeHeaders)
	stall := f.stallUpgrade
	script := f.scriptForEpoch(epoch)
	f.mu.Unlock()

	for k, v := range headers {
		w.Header().Set(k, v)
	}

	// Hold the handshake open if the test asked for it, but abandon the wait if
	// the client goes away first — otherwise a canceled dial would leave this
	// handler parked and the leak checker would report it.
	if stall != nil {
		select {
		case <-stall:
		case <-r.Context().Done():
			return
		}
	}

	if status != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()

	f.serve(r.Context(), conn, epoch, script)
}

// scriptForEpoch returns the script for a 1-based epoch, repeating the last
// entry. Callers hold f.mu.
func (f *fakeWS) scriptForEpoch(epoch int) epochScript {
	if len(f.scripts) == 0 {
		return epochScript{}
	}
	if epoch > len(f.scripts) {
		return f.scripts[len(f.scripts)-1]
	}
	return f.scripts[epoch-1]
}

// serve runs one connection to completion.
func (f *fakeWS) serve(ctx context.Context, conn *websocket.Conn, epoch int, script epochScript) {
	for _, msg := range script.onConnect {
		if err := conn.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
			return
		}
	}

	// An abrupt drop: tear the socket down with no close frame.
	if script.dropRaw {
		_ = conn.CloseNow()
		return
	}

	// An immediate close: nothing to wait for, so drop the connection now.
	if script.closeAfter != 0 && len(script.respond) == 0 && script.closeAfterFrames == 0 {
		_ = conn.Close(script.closeAfter, script.closeReason)
		return
	}

	var frames int
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return // client closed, or the context ended
		}
		frames++

		typ := messageType(data)
		f.mu.Lock()
		f.received = append(f.received, recordedFrame{epoch: epoch, typ: typ, raw: string(data)})
		f.mu.Unlock()

		// A type absent from respond gets no reply — which is precisely how a
		// withheld ack or a dropped terminator is scripted.
		for _, reply := range script.respond[typ] {
			if err := conn.Write(ctx, websocket.MessageText, []byte(reply)); err != nil {
				return
			}
		}

		// Mid-stream close: the replies for this frame have been written, so the
		// client has received a partial exchange and then loses the connection.
		if script.closeAfterFrames != 0 && frames >= script.closeAfterFrames {
			code := script.closeAfter
			if code == 0 {
				code = websocket.StatusAbnormalClosure
			}
			_ = conn.Close(code, script.closeReason)
			return
		}

		if script.closeAfterFrames == 0 && script.closeAfter != 0 {
			_ = conn.Close(script.closeAfter, script.closeReason)
			return
		}
	}
}

// messageType extracts the "type" field, returning "" when the frame has none.
// The fake stays deliberately lenient: a test that sends a malformed frame on
// purpose should reach the SDK's decoder, not be rejected by the fixture.
func messageType(data []byte) string {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return ""
	}
	return envelope.Type
}

// closeUpgradeBody releases the HTTP response that accompanied a WebSocket
// upgrade. A successful upgrade hands back a response whose body is already
// consumed, and a rejected one hands back a body the caller must drain — either
// way, leaving it open holds a connection in the transport's pool, which the
// leak checker then reports against whichever test runs next.
func closeUpgradeBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

// ─── harness self-tests ───

func TestFakeWSAcceptsAndRecords(t *testing.T) {
	t.Parallel()

	f := newFakeWS(t)
	f.script(epochScript{
		respond: map[string][]string{
			"subscribe": {`{"type":"subscription_ack","subscribed":["acme.trades"]}`},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, f.URL(), &websocket.DialOptions{HTTPClient: f.client()})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeUpgradeBody(resp)
	defer conn.CloseNow()

	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"subscribe","channels":["acme.trades"]}`)); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := messageType(data); got != "subscription_ack" {
		t.Errorf("reply type = %q, want subscription_ack", got)
	}

	if got := f.framesOfType("subscribe"); len(got) != 1 {
		t.Errorf("recorded %d subscribe frames, want 1", len(got))
	}
	if got := f.dialCount(); got != 1 {
		t.Errorf("dialCount = %d, want 1", got)
	}
}

// Per-epoch scripting is what makes reconnect behavior testable: connect #1 and
// connect #2 must be able to differ, and the last script must repeat so a test
// need not enumerate epochs it does not care about.
func TestFakeWSScriptsPerEpoch(t *testing.T) {
	t.Parallel()

	f := newFakeWS(t)
	f.script(
		epochScript{onConnect: []string{`{"type":"epoch_one"}`}},
		epochScript{onConnect: []string{`{"type":"epoch_two"}`}},
	)

	for i, want := range []string{"epoch_one", "epoch_two", "epoch_two"} { // third repeats
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		conn, resp, err := websocket.Dial(ctx, f.URL(), &websocket.DialOptions{HTTPClient: f.client()})
		if err != nil {
			cancel()
			t.Fatalf("dial %d: %v", i+1, err)
		}
		closeUpgradeBody(resp)
		_, data, err := conn.Read(ctx)
		if err != nil {
			_ = conn.CloseNow()
			cancel()
			t.Fatalf("read %d: %v", i+1, err)
		}
		if got := messageType(data); got != want {
			t.Errorf("dial %d delivered %q, want %q", i+1, got, want)
		}
		_ = conn.CloseNow()
		cancel()
	}

	if got := f.dialCount(); got != 3 {
		t.Errorf("dialCount = %d, want 3", got)
	}
}

func TestFakeWSRejectsUpgrade(t *testing.T) {
	t.Parallel()

	f := newFakeWS(t)
	f.rejectUpgrade(429, `{"code":"TENANT_LIMIT_EXCEEDED","message":"too many connections"}`)
	f.setUpgradeHeader("Retry-After", "3")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, f.URL(), &websocket.DialOptions{HTTPClient: f.client()})
	if err == nil {
		t.Fatal("dial succeeded against a rejected upgrade")
	}
	if resp == nil {
		t.Fatal("no HTTP response accompanied the rejected upgrade")
	}
	defer closeUpgradeBody(resp)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After = %q, want %q", got, "3")
	}
}

// A withheld reply is scripted by omission — the mechanism behind every
// dropped-terminator and missing-ack fault.
func TestFakeWSWithholdsUnscriptedReplies(t *testing.T) {
	t.Parallel()

	f := newFakeWS(t)
	f.script(epochScript{respond: map[string][]string{}}) // nothing answers

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, f.URL(), &websocket.DialOptions{HTTPClient: f.client()})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeUpgradeBody(resp)
	defer conn.CloseNow()

	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"history"}`)); err != nil {
		t.Fatalf("write: %v", err)
	}

	readCtx, readCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer readCancel()
	if _, _, err := conn.Read(readCtx); err == nil {
		t.Error("a reply arrived for an unscripted message type")
	}

	if got := f.framesOfType("history"); len(got) != 1 {
		t.Errorf("recorded %d history frames, want 1 — the frame must still be recorded", len(got))
	}
}

func TestFakeWSClosesWithScriptedCode(t *testing.T) {
	t.Parallel()

	f := newFakeWS(t)
	f.script(epochScript{closeAfter: websocket.StatusPolicyViolation, closeReason: "slow client"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, f.URL(), &websocket.DialOptions{HTTPClient: f.client()})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeUpgradeBody(resp)
	defer conn.CloseNow()

	_, _, err = conn.Read(ctx)
	if err == nil {
		t.Fatal("read succeeded against a closed connection")
	}
	if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Errorf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
	}
}

// A mid-stream close cuts the connection after a scripted number of client
// frames, once its replies have been written. It is a different fault from
// refusing the connection: the client has received part of an exchange and then
// loses the link, which is what separates "recovery was interrupted" from
// "recovery never started".
func TestFakeWSClosesMidStream(t *testing.T) {
	t.Parallel()

	f := newFakeWS(t)
	f.script(epochScript{
		respond: map[string][]string{
			// A replay that delivers one record and never terminates.
			"replay": {frameMessage("acme.trades", 1, "kafka:7:41", `{}`)},
		},
		closeAfterFrames: 1,
		closeAfter:       websocket.StatusPolicyViolation,
		closeReason:      "cut mid-replay",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, f.URL(), &websocket.DialOptions{HTTPClient: f.client()})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeUpgradeBody(resp)
	defer conn.CloseNow()

	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"replay","from_pos":"kafka:7:40"}`)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The scripted record arrives...
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("reading the replayed record: %v", err)
	}
	if got := messageType(data); got != "message" {
		t.Errorf("first frame = %q, want message", got)
	}

	// ...and then the connection is cut, with no terminator.
	_, _, err = conn.Read(ctx)
	if err == nil {
		t.Fatal("the connection stayed open past the scripted mid-stream close")
	}
	if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Errorf("close status = %v, want %v", got, websocket.StatusPolicyViolation)
	}
}

// A stalled handshake is how a mid-dial cancellation and a dial timeout are
// tested. The handler must also abandon its wait when the client goes away, or
// it would park a goroutine and the leak checker would report it.
func TestFakeWSStallsHandshake(t *testing.T) {
	t.Parallel()

	f := newFakeWS(t)
	release := f.stall()
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, f.URL(), &websocket.DialOptions{HTTPClient: f.client()})
	closeUpgradeBody(resp)
	if err == nil {
		t.Fatal("dial completed against a stalled handshake")
	}
}
