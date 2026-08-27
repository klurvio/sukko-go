package sukko

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"
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

// ─── gap → replay recovery FSM (Slice 2) ───

// The recovery FSM is a pure per-channel action machine — the sukko-py
// RecoveryEngine shape (§XVIII): each handle* method mutates the channel state
// and returns the replay sends the owner must perform, with no I/O of its own.
// runRecoveryOwner is the thin single-owner I/O shell around it (recovery_owner.go).
//
// Two contract facts drive the design:
//
//   - `pos` is opaque and MUST NEVER be compared. Gaps for a channel arrive in
//     connection order, so the FIRST un-recovered gap's last_pos is the anchor for
//     the whole replay cycle; later gaps within the cycle mean "more loss", covered
//     by replaying from the earlier anchor (the contract's conservative-anchor
//     guarantee: may re-deliver, never misses). Coalescing is therefore by ARRIVAL
//     ORDER, never magnitude (FR-006, T127/T141).
//
//   - The server rejects a replay for a channel it does not have subscribed
//     (handler_replay.go: "subscribe before replaying"). So a gap whose epoch died
//     must NOT be replayed on the next epoch until that channel is re-granted there.
//     Epoch-boundary re-drive is GRANT-triggered (handleGrant), never epoch-up-
//     triggered (T130). Every send is epoch-gated: the FSM emits a replay only for
//     the CURRENT epoch, and the owner sends it on that epoch's own bound conn.

// recoveryPhase is a channel's position in the gap→replay cycle.
type recoveryPhase uint8

const (
	// recIdle: no un-recovered gap outstanding.
	recIdle recoveryPhase = iota
	// recFloorWait: a gap is pending but the per-channel replay floor (the server's
	// connection-scoped replay rate limit) has not elapsed; due() fires it.
	recFloorWait
	// recReplaying: a replay is in flight on `epoch`, awaiting replay_complete.
	recReplaying
	// recAwaitingGrant: the epoch carrying this channel's recovery died mid-cycle;
	// the anchor is retained and the replay is re-driven when the channel is
	// re-granted on the new epoch (handleGrant). The one state with no live epoch.
	recAwaitingGrant
)

// recoveryChannel is one channel's FSM state, owned solely by the recovery owner
// goroutine (no mutex).
type recoveryChannel struct {
	phase recoveryPhase
	// anchor is the from_pos of the pending/active replay — the first un-recovered
	// gap's last_pos. DIVERGENCE from sukko-py (which clears it at begin_replay): the
	// anchor LIVES until its replay_complete, so a mid-REPLAYING epoch death can
	// re-drive from it (T130/T142). A re-drive's from_pos is this anchor, NEVER the
	// pos cursor (T142) — the cursor may have advanced past the gapped window on live
	// traffic, which would silently skip the lost records.
	anchor string
	// followup is the first gap seen while REPLAYING — the next cycle's anchor, begun
	// after replay_complete. On an epoch death it collapses into the head anchor (one
	// re-driven frame per channel): replaying from the earlier anchor covers it.
	followup    string
	hasFollowup bool
	// lastReplayAt is the wall time of this channel's last replay send, for the
	// floor. Zero = never. RESET (zeroed) with the epoch (T128): the server's replay
	// limiter is per-connection, so a fresh epoch opens the floor.
	lastReplayAt time.Time
	// floorWake is the absolute time a FLOOR_WAIT may fire.
	floorWake time.Time
	// epoch is the connection this channel's FLOOR_WAIT/REPLAYING belongs to — the
	// send-gate identity. Nil in recIdle and recAwaitingGrant (no live epoch).
	epoch *epoch
}

// replayAction is a send the owner must perform: replay{channel, from_pos} on
// `epoch`'s bound conn. The FSM only ever emits actions for the current epoch, so
// `epoch` is always non-nil and always the socket the frame may legally go out on.
type replayAction struct {
	channel string
	fromPos string
	epoch   *epoch
}

// recoveryFSM holds the per-channel FSM behind the single-owner goroutine.
type recoveryFSM struct {
	floor    time.Duration
	channels map[string]*recoveryChannel
}

func newRecoveryFSM(floor time.Duration) *recoveryFSM {
	return &recoveryFSM{floor: floor, channels: map[string]*recoveryChannel{}}
}

func (f *recoveryFSM) channelFor(channel string) *recoveryChannel {
	rec := f.channels[channel]
	if rec == nil {
		rec = &recoveryChannel{}
		f.channels[channel] = rec
	}
	return rec
}

// beginReplay transitions rec into REPLAYING on `epoch` and returns the send. The
// caller has already confirmed the floor is open and the epoch is current.
func (f *recoveryFSM) beginReplay(rec *recoveryChannel, channel, fromPos string, now time.Time, epoch *epoch) []replayAction {
	rec.phase = recReplaying
	rec.anchor = fromPos // kept until replay_complete (divergence from sukko-py)
	rec.lastReplayAt = now
	rec.floorWake = time.Time{}
	rec.epoch = epoch
	return []replayAction{{channel: channel, fromPos: fromPos, epoch: epoch}}
}

// handleGap coalesces a gap per the arrival-order/conservative-anchor rule and, if
// the floor is open and the epoch is current, emits a replay; otherwise it waits
// out the floor (due fires it) or, when the gap's epoch is no longer current,
// parks the anchor for a grant-triggered re-drive. lastPos is the anchor and is
// never compared. Precondition: lastPos != "" (an empty last_pos is un-anchorable
// and is not admitted — the *PossibleGap half lands in Slice 4).
func (f *recoveryFSM) handleGap(channel, lastPos string, now time.Time, current, evEpoch *epoch) []replayAction {
	rec := f.channelFor(channel)

	// The gap's epoch is no longer live (its decode raced ahead of a reconnect):
	// retain an anchor and re-drive when the channel is re-granted. An in-flight
	// cycle keeps its earlier anchor (covers this gap too).
	if evEpoch != current {
		if rec.phase == recIdle {
			rec.anchor = lastPos
			rec.phase = recAwaitingGrant
			rec.epoch = nil
		}
		return nil
	}

	switch rec.phase {
	case recIdle:
		if floorWake := rec.lastReplayAt.Add(f.floor); floorWake.After(now) {
			rec.phase = recFloorWait
			rec.anchor = lastPos
			rec.floorWake = floorWake
			rec.epoch = current
			return nil
		}
		return f.beginReplay(rec, channel, lastPos, now, current)
	case recFloorWait:
		// Coalesce: keep the earlier anchor (replaying from it covers this gap).
	case recReplaying:
		if !rec.hasFollowup {
			rec.followup = lastPos
			rec.hasFollowup = true
		}
	case recAwaitingGrant:
		// A gap on the current epoch for a channel awaiting a re-grant proves the
		// channel IS subscribed here (the server only gaps subscribed channels), so
		// the re-grant this channel was waiting for has effectively arrived — re-drive
		// the RETAINED anchor now (conservatively covering this gap too) rather than
		// stall for a subscription_ack grant that a dropped/withheld resume ack might
		// never deliver. The floor was reset with the epoch, so it is open.
		return f.beginReplay(rec, channel, rec.anchor, now, current)
	}
	return nil
}

// handleReplayComplete ends the active replay. With a gap retained mid-replay it
// begins the follow-up cycle (floor-gated, same epoch); otherwise the channel
// returns to idle. A complete for a channel not REPLAYING is a stale/duplicate
// terminator and is ignored.
func (f *recoveryFSM) handleReplayComplete(channel string, now time.Time, current, evEpoch *epoch) []replayAction {
	rec := f.channels[channel]
	if rec == nil || rec.phase != recReplaying {
		return nil
	}
	if !rec.hasFollowup {
		rec.phase = recIdle
		rec.anchor = ""
		rec.epoch = nil
		return nil
	}
	followup := rec.followup
	rec.followup, rec.hasFollowup = "", false

	// The completing epoch is gone: re-drive the follow-up from its anchor on the
	// next grant rather than send on a dead socket.
	if evEpoch != current {
		rec.phase = recAwaitingGrant
		rec.anchor = followup
		rec.epoch = nil
		return nil
	}
	if floorWake := rec.lastReplayAt.Add(f.floor); floorWake.After(now) {
		rec.phase = recFloorWait
		rec.anchor = followup
		rec.floorWake = floorWake
		rec.epoch = current
		return nil
	}
	return f.beginReplay(rec, channel, followup, now, current)
}

// handleGrant re-drives the retained replay for any of the granted channels that
// were left awaiting a re-grant by an epoch death (T130). It fires only when the
// grant belongs to the current epoch — a stale grant is dropped, because the live
// epoch's own resume-subscribe grant is guaranteed to re-trigger the re-drive.
func (f *recoveryFSM) handleGrant(channels []string, now time.Time, current, evEpoch *epoch) []replayAction {
	if evEpoch != current {
		return nil
	}
	var acts []replayAction
	for _, channel := range channels {
		rec := f.channels[channel]
		if rec == nil || rec.phase != recAwaitingGrant {
			continue
		}
		// The floor was reset with the epoch (handleReset), so it is open; re-drive
		// from the retained anchor immediately.
		acts = append(acts, f.beginReplay(rec, channel, rec.anchor, now, current)...)
	}
	return acts
}

// handleReset applies an epoch boundary: every channel mid-cycle (FLOOR_WAIT or
// REPLAYING) retains its anchor but moves to awaiting-grant so the replay is
// re-driven on the next epoch, and EVERY channel's floor is reset (T128) — the
// server's replay limiter is per-connection. A retained follow-up collapses into
// the head anchor (one re-driven frame per channel). Enqueued through the same
// inbox as the decode events (the decode loop and this reset are both the
// supervisor goroutine), so the event stream is totally ordered and a reset
// applies to exactly the epoch that just died.
func (f *recoveryFSM) handleReset() {
	for _, rec := range f.channels {
		if rec.phase == recFloorWait || rec.phase == recReplaying {
			rec.phase = recAwaitingGrant
			rec.followup, rec.hasFollowup = "", false
			rec.epoch = nil
		}
		rec.lastReplayAt = time.Time{}
		rec.floorWake = time.Time{}
	}
}

// due fires any FLOOR_WAIT whose floor has elapsed. A channel whose epoch is no
// longer current (its epoch died between arming the floor and the timer firing)
// is parked for a grant-triggered re-drive rather than sent on a dead socket.
func (f *recoveryFSM) due(now time.Time, current *epoch) []replayAction {
	var acts []replayAction
	for channel, rec := range f.channels {
		if rec.phase != recFloorWait || rec.floorWake.After(now) {
			continue
		}
		if rec.epoch != current {
			rec.phase = recAwaitingGrant
			rec.epoch = nil
			rec.floorWake = time.Time{}
			continue
		}
		acts = append(acts, f.beginReplay(rec, channel, rec.anchor, now, current)...)
	}
	return acts
}

// nextFloorWake returns the earliest absolute time due() needs to run, or false if
// no channel is floor-waiting — the single-timer arming input (the next_deadline
// pattern; the Slice-3 recovery deadline folds into the same minimum).
func (f *recoveryFSM) nextFloorWake() (time.Time, bool) {
	var earliest time.Time
	found := false
	for _, rec := range f.channels {
		if rec.phase != recFloorWait {
			continue
		}
		if !found || rec.floorWake.Before(earliest) {
			earliest = rec.floorWake
			found = true
		}
	}
	return earliest, found
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
