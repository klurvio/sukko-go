package sukko

import "context"

// Transport dials connections to the server. It is a factory: each Open yields
// one Conn for one epoch, so no read or close state can leak across reconnects —
// the read side lives on the connection, not on the transport.
type Transport interface {
	// Open dials and completes the handshake, returning a live connection. A
	// handshake that is not a successful upgrade returns a *HandshakeError.
	Open(ctx context.Context) (Conn, error)
	// Capabilities reports what this transport can do. It is static per kind.
	Capabilities() Capabilities
}

// Conn is one live connection — a single epoch's worth of it.
//
// Read is the only reader of the socket, deliberately: the back-pressure design
// relies on the decode goroutine being able to stop reading (by parking on
// delivery) so the TCP window closes. A connection that kept reading in the
// background would defeat that. Read blocks until a frame arrives, an error
// occurs, or the passed context is canceled — cancellation closes the
// connection locally.
type Conn interface {
	Read(ctx context.Context) ([]byte, error)
	Send(ctx context.Context, frame []byte) error
	// Close initiates a local close with a WebSocket code and reason.
	Close(code int, reason string) error
}

// Capabilities reports what a transport can do. It is transport-dependent and
// static per kind: a WebSocket is bidirectional, an SSE stream is receive-only,
// so the SDK can answer "can I publish on this connection?" locally rather than
// letting the server reject the attempt.
type Capabilities struct {
	CanPublish        bool
	CanSubscribeLive  bool
	CanRefreshInPlace bool
}

// capabilitiesFor returns the static capabilities of a transport kind.
func capabilitiesFor(kind TransportKind) Capabilities {
	switch kind {
	case TransportWebSocket:
		return Capabilities{CanPublish: true, CanSubscribeLive: true, CanRefreshInPlace: true}
	case TransportSSE:
		// Receive-only: no publish, no live subscribe, no in-place refresh.
		return Capabilities{}
	default:
		return Capabilities{}
	}
}
