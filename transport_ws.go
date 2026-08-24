package sukko

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// The WebSocket transport, built on coder/websocket — the SDK's one runtime
// dependency. It dials through the caller's *http.Client (the single TLS/CA/proxy
// seam), and translates the library's read errors into the SDK's own
// direction-tracked *CloseError so the close policy has something to classify.

// wsEnvelopeOverhead is the headroom above MaxPublishSize the read limit allows
// for a message frame's JSON envelope (type, channel, seq, ts, pos around the
// payload). Generous on purpose: a frame at the payload limit must arrive intact
// rather than tripping the read limit and closing the connection with 1009.
const wsEnvelopeOverhead = 4 * 1024

// wsTransport dials WebSocket connections.
type wsTransport struct {
	url        string
	httpClient *http.Client
	readLimit  int64
	clock      Clock // for Retry-After HTTP-date parsing; §VII forbids time.Now
}

// newWSTransport builds a WebSocket transport for a URL. The read limit is
// derived from MaxPublishSize plus envelope overhead, never left at
// coder/websocket's 32 KiB default — otherwise a 64 KiB inbound message would
// close the connection instead of being delivered.
func newWSTransport(url string, cfg *config) *wsTransport {
	return &wsTransport{
		url:        url,
		httpClient: cfg.httpClient,
		readLimit:  int64(cfg.maxPublishSize) + wsEnvelopeOverhead,
		clock:      cfg.clock,
	}
}

func (t *wsTransport) Capabilities() Capabilities {
	return capabilitiesFor(TransportWebSocket)
}

// Open dials and completes the WebSocket handshake.
func (t *wsTransport) Open(ctx context.Context) (Conn, error) {
	conn, resp, err := websocket.Dial(ctx, t.url, &websocket.DialOptions{HTTPClient: t.httpClient})
	// The upgrade response body is closed here — on success it is empty, on
	// failure it is read by handshakeErrorFromResponse before this defer runs.
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	if err != nil {
		// A non-101 response is a handshake failure the SDK surfaces as a typed
		// *HandshakeError with the status, body code, and Retry-After; a dial
		// error with no response (DNS, TCP, TLS) is wrapped as-is.
		if resp != nil {
			return nil, handshakeErrorFromResponse(resp, t.clock.Now())
		}
		return nil, fmt.Errorf("sukko: websocket dial: %w", err)
	}
	conn.SetReadLimit(t.readLimit)
	return &wsConn{conn: conn}, nil
}

// wsConn is one live WebSocket connection.
type wsConn struct {
	conn *websocket.Conn
	// closeOnce guards Close: the heartbeat-timeout path and terminalSequence can
	// both close the same conn, and coder/websocket's Close is not idempotent
	// (§VII: exactly-once close). The first call wins; later calls are no-ops.
	closeOnce sync.Once
}

// Read returns the next frame, or translates a close/failure into a typed error.
//
// Three outcomes distinguish who ended the connection, which is what the close
// policy keys on:
//   - a remote close frame → *CloseError with the server's code, remote direction
//   - the caller's own context canceled → the wrapped ctx error (the supervisor
//     initiated it and already knows why)
//   - anything else with the context still live → an abnormal drop, surfaced as a
//     library-synthesized local 1006
func (c *wsConn) Read(ctx context.Context) ([]byte, error) {
	_, data, err := c.conn.Read(ctx)
	if err == nil {
		return data, nil
	}

	if code := websocket.CloseStatus(err); code != -1 {
		var ce websocket.CloseError
		reason := ""
		if errors.As(err, &ce) {
			reason = ce.Reason
		}
		// A coded close frame surfaced through Read is attributed to the server.
		// This holds for every close the SDK observes today because the SDK only
		// closes in terminalSequence (a clean local 1000, never read back as an
		// epoch termination). When the heartbeat slice adds a client-initiated
		// CloseCodeHeartbeatTimeout (4999), that close MUST NOT be classified from
		// this remote-labeled path — the {4999, local} reconnect row would be
		// missed and it would read as an unenumerated remote close (terminal). The
		// heartbeat path classifies its own local close directly.
		return nil, &CloseError{Code: int(code), Direction: directionRemote, Reason: reason}
	}

	if ctx.Err() != nil {
		return nil, fmt.Errorf("sukko: websocket read canceled: %w", ctx.Err())
	}

	// No close frame and the context is live: the connection dropped abnormally.
	return nil, &CloseError{
		Code:      int(websocket.StatusAbnormalClosure),
		Direction: directionLocal,
		Reason:    err.Error(),
	}
}

// Send writes one frame as a text message — the SDK's frames are JSON.
func (c *wsConn) Send(ctx context.Context, frame []byte) error {
	if err := c.conn.Write(ctx, websocket.MessageText, frame); err != nil {
		return fmt.Errorf("sukko: websocket write: %w", err)
	}
	return nil
}

// Close initiates a local close with the given code and reason. It is idempotent
// via closeOnce: the heartbeat-timeout close and terminalSequence's close both
// target the same conn.
func (c *wsConn) Close(code int, reason string) error {
	var err error
	c.closeOnce.Do(func() {
		if e := c.conn.Close(websocket.StatusCode(code), reason); e != nil {
			err = fmt.Errorf("sukko: websocket close: %w", e)
		}
	})
	return err
}

// handshakeBody is the gateway's error response body on a rejected upgrade.
type handshakeBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// handshakeErrorFromResponse builds a *HandshakeError from a non-101 response.
func handshakeErrorFromResponse(resp *http.Response, now time.Time) *HandshakeError {
	he := &HandshakeError{
		Status:     resp.StatusCode,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), now),
	}

	// The body carries a machine code and message when present; a body that is
	// missing or unparseable simply leaves those empty rather than failing. The
	// caller (Open) owns closing resp.Body.
	if resp.Body != nil {
		if raw, err := io.ReadAll(io.LimitReader(resp.Body, wsEnvelopeOverhead)); err == nil {
			var body handshakeBody
			if json.Unmarshal(raw, &body) == nil {
				he.Code = body.Code
				he.Message = body.Message
			}
		}
	}
	return he
}
