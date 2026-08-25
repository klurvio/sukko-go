package sukko

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"
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
	// gen bumps on every set, so a flight is uniquely identifiable. The decode loop
	// stamps a poke with the gen it validated; the serializer releases only if that
	// gen still matches the current flight — so a stale poke (its flight cleared by
	// a timeout and a new one sent) is a no-op, not a release of the NEXT flight.
	gen uint64
}

func (s *subFlight) set(kind subReqKind, channels []string) {
	s.mu.Lock()
	s.active, s.kind, s.channels = true, kind, channels
	s.gen++
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

// snapshot returns whether a request is outstanding, its kind, its requested
// channels, and its generation — read by the decode loop to reconcile an ack and
// stamp the release poke.
func (s *subFlight) snapshot() (active bool, kind subReqKind, channels []string, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, s.kind, s.channels, s.gen
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

	// ackTimer bounds how long the one outstanding request waits for its ack, so a
	// withheld ack never strands the queue. AckTimeout tracks the CONFIGURED
	// RecoveryDeadline (read at arm time), not the constant default (FR-001a).
	var ackTimer Timer
	var ackC <-chan time.Time
	disarmAck := func() {
		if ackTimer != nil {
			ackTimer.Stop()
			ackTimer, ackC = nil, nil
		}
	}
	armAck := func() {
		disarmAck()
		ackTimer = c.clock.NewTimer(c.cfg.recoveryDeadline, purposeAckTimeout)
		ackC = ackTimer.C()
	}

	// send records the flight BEFORE the wire send (so a fast ack cannot beat the
	// decode loop's read of the requested set), arms the ack timeout on a
	// successful send, and clears the flight on a failed send. process (below)
	// records a Subscribe's channels as desired first, so they are pending even
	// before the ack and survive to a reconnect resume.
	send := func(conn Conn, kind subReqKind, channels []string) {
		c.subFlight.set(kind, channels)
		if c.sendSubscribeFrame(ownerCtx, conn, subReq{kind: kind, channels: channels}) {
			armAck()
		} else {
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
			disarmAck()
			return
		case req := <-reqCh:
			process(req)
		case g := <-c.subPoke:
			// The outstanding request was answered (the decode loop reconciled and
			// emitted in receive order). Release the slot and disarm the timeout — but
			// ONLY if the poke's generation still matches the current flight. A stale
			// poke (its flight cleared by a timeout, a new one now outstanding) is a
			// FULL no-op: disarming or clearing on it would strand the new flight.
			if active, _, _, cur := c.subFlight.snapshot(); active && g == cur {
				disarmAck()
				c.subFlight.clear()
			}
		case <-ackC:
			// The outstanding request's ack never arrived within AckTimeout. Surface a
			// *ProtocolError (via ownerSurface — parks on rootCtx, discards on owner
			// teardown) and release the slot so the queue is not stranded.
			ackTimer, ackC = nil, nil
			_, kind, _, _ := c.subFlight.snapshot()
			answer := typeSubscriptionAck
			if kind == reqUnsubscribe {
				answer = typeUnsubscriptionAck
			}
			c.ownerSurface(ownerCtx, &ProtocolError{Type: answer, Message: "timed out waiting for the acknowledgement"})
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
func (c *Client) reconcileSubscriptionAck(epochCtx context.Context, subscribed []string, count int) (gen uint64, matched bool) {
	active, kind, requested, g := c.subFlight.snapshot()
	if !active || kind != reqSubscribe {
		return 0, false // no outstanding subscribe this ack could answer
	}
	// Subset guard (§II, ADR-0013): the contract guarantees a subscription_ack's
	// `subscribed` is a subset of THIS request's channels. An ack whose subscribed
	// contains a channel the outstanding request never asked for cannot belong to it
	// — a late ack for a timed-out flight mis-landing on a same-kind successor — so
	// drop it rather than fabricate a grant/denial. (It cannot catch a full denial,
	// empty `subscribed`; that residual ambiguity is the ADR's accepted trade-off.)
	if !subset(subscribed, requested) {
		return 0, false
	}
	c.subs.grant(subscribed)
	c.forward(epochCtx, &SubscriptionResult{
		Requested:  slices.Clone(requested),
		Granted:    slices.Clone(subscribed),
		NotGranted: difference(requested, subscribed),
		Count:      count,
	})
	return g, true
}

// outstandingGen returns the current flight's generation if a request of the given
// kind is outstanding — used by the decode loop to stamp a release poke, so the
// serializer releases ONLY the flight the answer actually belongs to (a stale or
// mis-kinded answer cannot release the next flight).
func (c *Client) outstandingGen(kind subReqKind) (gen uint64, ok bool) {
	active, k, _, g := c.subFlight.snapshot()
	if active && k == kind {
		return g, true
	}
	return 0, false
}

// matchUnsubscriptionAck returns the outstanding unsubscribe's generation if this
// (non-forced) ack belongs to it — the symmetric subset guard to the subscribe side
// (ADR-0013): a legit unsubscription_ack's `unsubscribed` is a subset of the
// channels the flight sent (its granted subset), so an ack naming a channel the
// flight never unsubscribed is a late ack for a different (timed-out) unsubscribe
// and must not release this flight.
func (c *Client) matchUnsubscriptionAck(unsubscribed []string) (gen uint64, ok bool) {
	active, kind, channels, g := c.subFlight.snapshot()
	if !active || kind != reqUnsubscribe {
		return 0, false
	}
	if !subset(unsubscribed, channels) {
		return 0, false
	}
	return g, true
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

// pokeSubscribeSerializer signals the serializer to release the flight identified
// by gen. Drain a stale poke (left by a flight the timeout already cleared) before
// sending, so a legit poke is never dropped behind a stale one — dispatch is the
// SOLE poke producer (every call site is on the decode goroutine), so there is no
// producer race, and the serializer (sole consumer) always sees the latest.
func (c *Client) pokeSubscribeSerializer(gen uint64) {
	select {
	case <-c.subPoke:
	default:
	}
	select {
	case c.subPoke <- gen:
	default:
	}
}

// subset reports whether every element of a is in b.
func subset(a, b []string) bool {
	inB := make(map[string]struct{}, len(b))
	for _, s := range b {
		inB[s] = struct{}{}
	}
	for _, s := range a {
		if _, ok := inB[s]; !ok {
			return false
		}
	}
	return true
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
