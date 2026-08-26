package sukko

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
)

// This file holds the recovery foundation (Slice 1): the resume identity, the
// per-channel `pos` cursor, and the reconnect-replay window. The cursor and window
// are the state the reconnect-replay round-trip needs; gap recovery, the replay
// floor, the recovery deadline, and the *PossibleGap snapshot land in later slices.

// generateClientID mints a fresh per-process resume identity (32 hex chars from 16
// crypto-random bytes) when the caller supplied none via WithClientID. It is called
// once at NewClient, off the hot path, so crypto/rand's error path is acceptable
// here (unlike backoff jitter, which uses the injectable Rand). A fresh id forfeits
// cross-restart replay — that trade-off is the caller's to avoid via WithClientID.
func generateClientID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("sukko: generate client id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// posCursor holds the per-channel opaque `pos` cursor that anchors reconnect-replay
// (reconnect{last_pos}) and live replay. The DECODE goroutine is its SOLE writer —
// both advance-on-live and seed-on-history run there — so the seed-if-absent
// check-then-set needs no generation guard, just the leaf mutex that lets the
// reconnect path (supervisor) and Stats read the map concurrently. It is never held
// across a send. The `pos` value is OPAQUE: stored and echoed verbatim, never parsed
// and never compared (comparing two pos values would void the opacity the wire
// contract depends on), which is why the cursor advances by receive order, never by
// magnitude (FR-006).
type posCursor struct {
	mu  sync.Mutex
	pos map[string]string // tenant-prefixed channel → opaque pos
}

func newPosCursor() *posCursor {
	return &posCursor{pos: map[string]string{}}
}

// advance records a live record's pos, overwriting any prior value: a live message
// is always the newest anchor for its channel. Called for SourceLive only — never
// SourceReplay (a replayed record's older pos would regress the cursor) and the
// history seed is separate (seedIfAbsent). Decode goroutine only.
func (p *posCursor) advance(channel, pos string) {
	if pos == "" {
		return // a message without a pos (Direct backend) anchors nothing
	}
	p.mu.Lock()
	p.pos[channel] = pos
	p.mu.Unlock()
}

// seedIfAbsent records a history record's pos ONLY when the channel has no cursor
// yet — the one narrow admission of a history pos (FR-006). It never overwrites a
// live cursor (which is newer), so monotonicity holds without comparing pos values:
// a seed cannot regress what does not exist. Called for SourceHistory only. Decode
// goroutine only.
func (p *posCursor) seedIfAbsent(channel, pos string) {
	if pos == "" {
		return
	}
	p.mu.Lock()
	if _, ok := p.pos[channel]; !ok {
		p.pos[channel] = pos
	}
	p.mu.Unlock()
}

// snapshot returns a copy of the cursor map for the reconnect{last_pos} frame. It is
// always non-nil (empty, not nil) so the frame marshals `{}` rather than `null` — a
// null last_pos is a malformed resume request (wire.go). Read by the supervisor at
// reconnect time.
func (p *posCursor) snapshot() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]string, len(p.pos))
	maps.Copy(out, p.pos)
	return out
}

// replayWindow marks the interval on a reconnected epoch between sending
// reconnect{last_pos} and receiving its reconnect_ack/reconnect_error, during which
// arriving live-shaped `message` frames are the server's replay of missed records
// and are normalized to SourceReplay. It is an atomic flag, ASSIGNED (not toggled)
// on every epoch-up — open iff a reconnect frame was sent — so a mid-window epoch
// death cannot leak an open window into the next epoch, which may send no reconnect.
// The supervisor opens it (before the decode loop starts, so the open happens-before
// the first read); the decode loop reads it per message and closes it on the ack.
type replayWindow struct {
	open atomic.Bool
}

func (w *replayWindow) set(open bool) { w.open.Store(open) }
func (w *replayWindow) close()        { w.open.Store(false) }
func (w *replayWindow) isOpen() bool  { return w.open.Load() }

// sendReconnect writes reconnect{client_id, last_pos} on the reconnected epoch,
// reporting whether it was sent. last_pos is the cursor snapshot — always an empty
// (never nil) map when cursorless, so the frame marshals `{}` not `null`. A send
// error means the socket is already going away; runEpoch will detect it and
// reconnect. Called by the supervisor at epoch-up on a reconnect.
func (c *Client) sendReconnect(conn Conn) bool {
	frame, err := json.Marshal(wireReconnect{
		Type: typeReconnect,
		Data: reconnectPayload{ClientID: c.cfg.clientID, LastPos: c.cursor.snapshot()},
	})
	if err != nil {
		return false // unreachable: strings + a string map always marshal
	}
	return conn.Send(c.rootCtx, frame) == nil
}

// applyRecovery resolves a message-class event's final Source and updates the pos
// cursor, on the decode goroutine (the cursor's sole writer). Order is load-bearing
// (FR-006): the reconnect-replay window override is applied to Source FIRST, then
// the cursor is keyed on the FINAL Source — so a live-shaped record replayed inside
// the window is retagged SourceReplay and therefore does NOT advance the cursor
// (its older pos would regress it). A live record advances; a history record seeds
// only if the channel is cursorless; a replayed record touches nothing.
func (c *Client) applyRecovery(m *Message) {
	if m.Source == SourceLive && c.replayWin.isOpen() {
		m.Source = SourceReplay
	}
	switch m.Source {
	case SourceLive:
		c.cursor.advance(m.Channel, m.Pos)
	case SourceHistory:
		c.cursor.seedIfAbsent(m.Channel, m.Pos)
	case SourceReplay:
		// A replayed record (window override or replay_message) anchors nothing.
	}
}
