package sukko

import (
	"context"
	"encoding/json"
	"fmt"
)

// Publish sends data to a channel over the WebSocket. It is fire-and-forget: the call
// returns after a local pre-check and the frame write, and the server's outcome
// arrives in-band on Messages() as *PublishAccepted or *PublishError — the wire
// protocol has no per-call id, so a synchronous signature would be a false
// request/response correlation (FR-001b). For a genuine synchronous result, use
// RESTPublish.
//
// Publish REQUIRES a live socket — a publish is an action with no meaning offline — so
// it returns *NotConnectedError in any non-connected state and ErrClosed once the
// client is closed. The local pre-check (shared with RESTPublish) rejects, before any
// byte reaches the socket, a payload that does not marshal to a JSON object
// (ErrPayloadNotObject), one larger than MaxPublishSize (ErrPayloadTooLarge), and a
// publish from an API-key-only client (ErrPublishRequiresJWT — the gateway refuses
// API-key publish; a JWT or a WithNoAuth deployment is required).
func (c *Client) Publish(ctx context.Context, channel string, data any) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sukko: publish: %w", err)
	}
	payload, err := c.prepublishCheck(data)
	if err != nil {
		return err
	}
	c.mu.Lock()
	state := c.state
	closed := c.closed
	c.mu.Unlock()
	switch {
	case closed || state == StateClosed || state == StateError:
		return ErrClosed
	case state != StateConnected:
		return &NotConnectedError{Op: "Publish"}
	}
	// Send on the CURRENT epoch's bound conn (not currentConn()), so the frame rides the
	// epoch observed here; if that epoch died between the read and the send, the send
	// fails on its own dead conn and the caller sees *NotConnectedError.
	e := c.currentEpochRef()
	if e == nil {
		return &NotConnectedError{Op: "Publish"}
	}
	if !c.sendPublishFrame(e.conn, channel, payload) {
		return &NotConnectedError{Op: "Publish"}
	}
	return nil
}

// prepublishCheck is the shared local publish pre-check for both the WebSocket Publish
// and the REST RESTPublish surfaces. It marshals the caller's payload and enforces, in
// order, that it is a JSON object (ErrPayloadNotObject), within MaxPublishSize
// (ErrPayloadTooLarge), and that the credential class may publish — an API-key-only
// client is refused (ErrPublishRequiresJWT); a JWT-bearing or WithNoAuth client passes.
// It returns the marshaled payload so the caller writes the exact bytes it validated.
func (c *Client) prepublishCheck(data any) (json.RawMessage, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPayloadNotObject, err)
	}
	// A JSON object is the only accepted top-level form; a slice, scalar, or nil is not.
	if len(raw) == 0 || raw[0] != '{' {
		return nil, ErrPayloadNotObject
	}
	if len(raw) > c.cfg.maxPublishSize {
		return nil, ErrPayloadTooLarge
	}
	// Publish requires a JWT: an API-key-only client (a credential but no token) is
	// refused; a WithNoAuth deployment carries no credential and is allowed.
	if token, _ := c.creds.snapshot(); token == "" && !c.cfg.noAuth {
		return nil, ErrPublishRequiresJWT
	}
	return raw, nil
}

// sendPublishFrame writes publish{channel, data} on the given conn, reporting whether
// it was sent. A send error means the epoch's socket is going away.
func (c *Client) sendPublishFrame(conn Conn, channel string, data json.RawMessage) bool {
	frame, err := json.Marshal(wirePublish{Type: typePublish, Data: publishPayload{Channel: channel, Data: data}})
	if err != nil {
		return false // unreachable: a channel string and raw JSON always marshal
	}
	return conn.Send(c.rootCtx, frame) == nil
}
