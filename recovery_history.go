package sukko

import (
	"encoding/json"
	"sync"
	"time"
)

// The History single-flight (Slice 3b). History() requests up to `limit` historical
// records for a channel; they arrive as SourceHistory *Message events terminated by
// history_complete (or history_error). Only ONE history may be outstanding per
// client (FR-001b): N concurrent histories would put N×HistoryLimit records on the
// bounded delivery channel. That single-flight is enforced by historyFlight, a
// leaf-lock slot shared by three parties — the CALLER (claim, and identity-matched
// releaseIf on a send failure), the DECODE loop (channel-matched release on a
// terminator), and the recovery OWNER (deadline + epoch-death interrupt).
//
// History is NOT in the recovery FSM (which is per-channel replay): it is a
// client-level, single-flight, one-shot request that does NOT re-drive across a
// reconnect — an epoch death mid-history is a truncated recovery, interrupted and
// abandoned (the caller re-requests), the deliberate asymmetry with replay.
//
// LOCK DISCIPLINE (§VII): every decision — the owner's deadline check and its
// epoch-death check — reads, decides, and mutates `active` under ONE lock hold and
// returns the channel to interrupt; the owner surfaces the *RecoveryInterruptedError
// AFTER unlocking, because ownerSurface can park on back-pressure and a lock held
// across that park would block every History() caller and every decode release for
// the whole stall. Keeping each decision a single critical section is also what
// makes a generation stamp unnecessary. The lock is never held across a send.
type historyFlight struct {
	mu      sync.Mutex
	active  bool
	channel string
	// epoch is the connection the history was requested on (the arm identity — stamped
	// once at claim, never re-stamped). If it is no longer current the epoch died and
	// the history is interrupted; history never suspends on an epoch change (unlike a
	// replay, which re-drives), so it needs no armEpoch.
	epoch *epoch
	// deadline is when a missing history_complete is declared interrupted; re-armed on
	// a consumer stall (park-suspension), never on each history record (§VII).
	deadline time.Time
	// armEpisodes is delivery.parkEpisodes() at the last arm — the stall-suspension
	// baseline, exactly as the replay deadline uses (recovery.go).
	armEpisodes int64
}

// claim reserves the slot for a new history on `channel`/`epoch`, reporting whether
// it succeeded — false means one is already in flight (the caller returns
// ErrHistoryInProgress). Caller goroutine.
func (h *historyFlight) claim(channel string, epoch *epoch, deadline time.Time, episodes int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active {
		return false
	}
	h.active, h.channel, h.epoch, h.deadline, h.armEpisodes = true, channel, epoch, deadline, episodes
	return true
}

// releaseIfChannel ends the flight ONLY when it is the flight for `channel` (H1) —
// so a stale/duplicate terminator for another channel cannot free an unrelated
// history and let a second one run. Reports whether it released. Decode goroutine.
// The same-channel-duplicate residual (a late terminator from a just-completed
// History(A) landing on a fresh History(A)) is the ADR-0013 accepted-ambiguity class
// and is deliberately not gen-stamped.
func (h *historyFlight) releaseIfChannel(channel string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active && h.channel == channel {
		h.active = false
		return true
	}
	return false
}

// releaseIf ends the flight ONLY when it is still the exact flight the caller
// claimed — same channel AND epoch — the send-failure path. conn.Send is a blockable
// await (a wedged socket parks it up to the heartbeat timeout), during which the
// owner may have interrupted this flight (its deadline or an epoch death) and a fresh
// History may have re-claimed the slot on another epoch. An UNCONDITIONAL release
// would then free that unrelated flight, breaking single-flight (up to 2×HistoryLimit
// on the delivery channel). The identity match frees only a still-owned or defunct
// flight — a same-(channel, epoch) successor can only exist with its own send still
// parked or on a dead conn, so freeing it is the ADR-0013 accepted-ambiguity class.
//
// That "successor is always defunct" half leans on two Conn properties: writes on one
// conn serialize (a successor's Write cannot start until this straggler's returned),
// and a failed Write means the conn is dying, never transient. Both hold for
// coder/websocket; an alternate transport with concurrent or non-poisoning writes
// would need re-auditing here (the epoch match still excludes the dangerous
// cross-epoch live successor regardless).
func (h *historyFlight) releaseIf(channel string, epoch *epoch) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active && h.channel == channel && h.epoch == epoch {
		h.active = false
		return true
	}
	return false
}

// nextDeadline returns the pending history deadline for the owner's single timer, or
// false when no history is in flight.
func (h *historyFlight) nextDeadline() (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.active {
		return time.Time{}, false
	}
	return h.deadline, true
}

// due decides the in-flight history's fate at a timer fire and returns the channel
// to interrupt (having released the slot), or "" if nothing is due or it was
// suspended. Read-decide-mutate under one lock; the owner surfaces after unlocking.
func (h *historyFlight) due(t tick, dur time.Duration) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.active || h.deadline.After(t.now) {
		return ""
	}
	// Epoch death → interrupt: a history cannot resume across a reconnect (the
	// deliberate asymmetry with a replay, which suspends on an epoch change and
	// re-drives). current==nil (disconnected) or a changed epoch both mean the
	// connection that was serving this history is gone.
	if t.current == nil || h.epoch != t.current {
		h.active = false
		return h.channel
	}
	// Consumer stall → suspend (measure SERVER silence, not consumer speed): a
	// back-pressure episode during the window re-arms rather than fires, mirroring
	// the replay deadline and the heartbeat's pong suspension (§VII).
	if t.parked || t.episodes != h.armEpisodes {
		h.deadline = t.now.Add(dur)
		h.armEpisodes = t.episodes
		return ""
	}
	h.active = false
	return h.channel
}

// interruptIfEpochDead ends an in-flight history whose epoch is no longer current —
// the prompt epoch-death path, run by the owner at every epoch reset so a truncated
// history is interrupted immediately rather than waiting for its deadline. Returns
// the channel to interrupt, or "".
func (h *historyFlight) interruptIfEpochDead(current *epoch) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active && h.epoch != current {
		h.active = false
		return h.channel
	}
	return ""
}

// sendHistoryFrame writes history{channel, limit} on the claimed epoch's bound conn,
// reporting whether it was sent. A send error means that epoch's socket is going
// away — the caller releases the slot and returns *NotConnectedError.
func (c *Client) sendHistoryFrame(conn Conn, channel string, limit int) bool {
	frame, err := json.Marshal(wireHistory{Type: typeHistory, Data: historyPayload{Channel: channel, Limit: limit}})
	if err != nil {
		return false // unreachable: a struct of a string and an int always marshals
	}
	return conn.Send(c.rootCtx, frame) == nil
}

// releaseHistory ends the single-flight for a channel on a terminator
// (history_complete/history_error) and wakes the owner to drop the history deadline
// from its timer. Channel-matched (H1). Decode goroutine.
func (c *Client) releaseHistory(channel string) {
	if c.historyFlight.releaseIfChannel(channel) {
		c.pokeRecoveryOwner()
	}
}
