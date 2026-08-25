package sukko

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
)

// The subscribe serializer is a supervisor-lifetime single-owner goroutine — the
// third such goroutine alongside the auth-owner. It exists because the wire's
// subscription_ack carries NO correlation id (research D10): with two subscribes
// in flight an arriving ack is unattributable, and the grant diff feeds both the
// not-granted event and the escalation delta, so mis-attribution would fabricate
// or discard denials. The serializer therefore keeps at most ONE subscribe/
// unsubscribe outstanding at a time; callers still call concurrently and the SDK
// does the serializing (FR-001a).
//
// The serializer owns the queue (subReqCh), the desired set, the wire sends, and
// the one-outstanding slot (subFlight). The DECODE loop — not the serializer —
// reconciles an arriving ack and emits *SubscriptionResult, so the grant event is
// in receive order relative to data (the same discipline auth uses for
// *Authenticated); it reads the serializer's requested set from subFlight and then
// pokes the serializer to release the slot. This keeps the control-plane rule: the
// decode loop hands off (a poke), never enqueues.

// subReqKind distinguishes a subscribe request from an unsubscribe.
type subReqKind int

const (
	reqSubscribe subReqKind = iota
	reqUnsubscribe
)

// subReq is a caller Subscribe/Unsubscribe handed to the serializer.
type subReq struct {
	kind     subReqKind
	channels []string
}

// subFlight is the one outstanding subscribe/unsubscribe, shared between the
// serializer and the decode loop. The serializer sets it BEFORE the wire send (so
// a fast ack cannot arrive before the decode loop can read the requested set) and
// clears it when the answering poke releases the slot; the decode loop reads it to
// reconcile an ack in receive order. Guarded by its own mutex — a leaf lock, never
// held across a send.
type subFlight struct {
	mu       sync.Mutex
	active   bool
	kind     subReqKind
	channels []string
}

func (s *subFlight) set(kind subReqKind, channels []string) {
	s.mu.Lock()
	s.active, s.kind, s.channels = true, kind, channels
	s.mu.Unlock()
}

func (s *subFlight) clear() {
	s.mu.Lock()
	s.active, s.channels = false, nil
	s.mu.Unlock()
}

func (s *subFlight) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

// snapshot returns whether a request is outstanding, its kind, and its requested
// channels — read by the decode loop to reconcile an ack.
func (s *subFlight) snapshot() (active bool, kind subReqKind, channels []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, s.kind, s.channels
}

// subState holds the subscription sets behind a mutex so the caller-facing
// accessors (Subscriptions/PendingSubscriptions) read them while the serializer
// (desired) and the decode loop (granted) mutate them. desired is the caller's
// wanted set (changed only by Subscribe/Unsubscribe); granted is the
// currently-subscribed subset (cleared at every epoch boundary, repopulated by
// acks). PendingSubscriptions = desired − granted (FR-001a).
type subState struct {
	mu      sync.Mutex
	desired map[string]struct{}
	granted map[string]struct{}
}

func newSubState() *subState {
	return &subState{desired: map[string]struct{}{}, granted: map[string]struct{}{}}
}

// addDesired records channels the caller wants (a Subscribe). Serializer only.
func (s *subState) addDesired(channels []string) {
	s.mu.Lock()
	for _, ch := range channels {
		s.desired[ch] = struct{}{}
	}
	s.mu.Unlock()
}

// grant marks channels as granted (they are already desired). Decode loop only.
func (s *subState) grant(channels []string) {
	s.mu.Lock()
	for _, ch := range channels {
		s.granted[ch] = struct{}{}
	}
	s.mu.Unlock()
}

// remove prunes channels from BOTH the desired and granted sets (Unsubscribe's
// both-set pruning, FR-001a) and returns the subset that was granted — the only
// channels a wire `unsubscribe` frame need cover. Serializer only.
func (s *subState) remove(channels []string) (wasGranted []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range channels {
		if _, ok := s.granted[ch]; ok {
			wasGranted = append(wasGranted, ch)
			delete(s.granted, ch)
		}
		delete(s.desired, ch)
	}
	slices.Sort(wasGranted)
	return wasGranted
}

// grantedSnapshot returns the granted set, sorted (Subscriptions()).
func (s *subState) grantedSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.granted))
	for ch := range s.granted {
		out = append(out, ch)
	}
	slices.Sort(out)
	return out
}

// pendingSnapshot returns desired − granted, sorted (PendingSubscriptions()).
func (s *subState) pendingSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for ch := range s.desired {
		if _, ok := s.granted[ch]; !ok {
			out = append(out, ch)
		}
	}
	slices.Sort(out)
	return out
}

// runSubscribeSerializer is the single-owner subscription goroutine. It never
// reconciles or emits — that is the decode loop's job (receive order) — it only
// sends, tracks the one-outstanding slot, and releases it on a poke.
func (c *Client) runSubscribeSerializer(ownerCtx context.Context) {
	defer c.recoverSubscribeSerializer()

	// process sends a dequeued request's wire frame when connected and records it
	// as the outstanding flight. A Subscribe records its channels as desired first
	// (so they are pending even before the ack, and survive to a reconnect resume).
	// send sets the flight BEFORE the wire send (so a fast ack cannot beat the
	// decode loop's read of the requested set) and clears it on a failed send.
	send := func(conn Conn, kind subReqKind, channels []string) {
		c.subFlight.set(kind, channels)
		if !c.sendSubscribeFrame(ownerCtx, conn, subReq{kind: kind, channels: channels}) {
			c.subFlight.clear()
		}
	}
	process := func(req subReq) {
		switch req.kind {
		case reqSubscribe:
			c.subs.addDesired(req.channels)
			conn := c.currentConn()
			if conn == nil {
				// Disconnected: the desired set is updated; the reconnect resume flushes
				// it (a later slice). No frame, no slot consumed.
				return
			}
			send(conn, reqSubscribe, req.channels)
		case reqUnsubscribe:
			// Both-set prune immediately (reflected in the accessors). A wire
			// `unsubscribe` covers only the channels that were granted; a never-granted
			// channel is pruned locally with no wire traffic and no slot.
			wasGranted := c.subs.remove(req.channels)
			if len(wasGranted) == 0 {
				return
			}
			conn := c.currentConn()
			if conn == nil {
				return
			}
			send(conn, reqUnsubscribe, wasGranted)
		}
	}

	for {
		// Accept a new request only when idle — leave requests buffered in subReqCh
		// while one is outstanding (single-outstanding, FR-001a).
		var reqCh <-chan subReq
		if !c.subFlight.isActive() {
			reqCh = c.subReqCh
		}
		select {
		case <-ownerCtx.Done():
			return
		case req := <-reqCh:
			process(req)
		case <-c.subPoke:
			// The outstanding request was answered (the decode loop reconciled and
			// emitted in receive order). Release the slot; the next loop iteration
			// re-enables reqCh and any buffered request is processed.
			c.subFlight.clear()
		}
	}
}

// reconcileSubscriptionAck runs on the DECODE goroutine when a subscription_ack
// arrives. It reconciles the grant against the serializer's outstanding requested
// set and emits *SubscriptionResult IN RECEIVE ORDER (via the epoch context). It
// reports whether it matched an outstanding subscribe flight — the caller pokes to
// release the slot ONLY when it did, so a spurious/stale ack (no outstanding
// subscribe) neither reconciles NOR releases the next flight.
func (c *Client) reconcileSubscriptionAck(epochCtx context.Context, subscribed []string, count int) (matched bool) {
	active, kind, requested := c.subFlight.snapshot()
	if !active || kind != reqSubscribe {
		return false // no outstanding subscribe this ack could answer
	}
	c.subs.grant(subscribed)
	c.forward(epochCtx, &SubscriptionResult{
		Requested:  slices.Clone(requested),
		Granted:    slices.Clone(subscribed),
		NotGranted: difference(requested, subscribed),
		Count:      count,
	})
	return true
}

// outstandingIs reports whether a request of the given kind is currently
// outstanding — used by the decode loop to release the slot ONLY for an answer
// that matches the in-flight request (so a stale/duplicate answer cannot release
// the NEXT flight and roll mis-attribution forward).
func (c *Client) outstandingIs(kind subReqKind) bool {
	active, k, _ := c.subFlight.snapshot()
	return active && k == kind
}

// sendSubscribeFrame marshals and sends the wire frame for a request, reporting
// whether it was sent. A send error means the socket is going away.
func (c *Client) sendSubscribeFrame(ownerCtx context.Context, conn Conn, req subReq) bool {
	var frame []byte
	var err error
	switch req.kind {
	case reqSubscribe:
		frame, err = json.Marshal(wireSubscribe{Type: typeSubscribe, Data: subscribePayload{Channels: req.channels}})
	case reqUnsubscribe:
		frame, err = json.Marshal(wireUnsubscribe{Type: typeUnsubscribe, Data: unsubscribePayload{Channels: req.channels}})
	}
	if err != nil {
		return false // unreachable: a struct of strings always marshals
	}
	return conn.Send(ownerCtx, frame) == nil
}

// pokeSubscribeSerializer signals the serializer to release its outstanding slot.
// Non-blocking (buffered 1).
func (c *Client) pokeSubscribeSerializer() {
	select {
	case c.subPoke <- struct{}{}:
	default:
	}
}

// recoverSubscribeSerializer routes a serializer panic to a terminal, mirroring
// the auth-owner (ADR-0010): record the cause, then cancel root so the supervisor
// tears down with the cause set (the serializer never calls terminalSequence).
func (c *Client) recoverSubscribeSerializer() {
	r := recover()
	if r == nil {
		return
	}
	ie := &InternalError{Op: "subscribe-serializer", Value: fmt.Sprint(r), Stack: string(debug.Stack())}
	c.cfg.logger.Error("sukko: subscribe-serializer panic", "value", fmt.Sprint(r))
	c.setErrIfNil(ie)
	c.setTerminalCauseIfNil(ie)
	c.rootCancel()
}

// difference returns a − b as a sorted slice.
func difference(a, b []string) []string {
	inB := make(map[string]struct{}, len(b))
	for _, s := range b {
		inB[s] = struct{}{}
	}
	var out []string
	for _, s := range a {
		if _, ok := inB[s]; !ok {
			out = append(out, s)
		}
	}
	slices.Sort(out)
	return out
}
