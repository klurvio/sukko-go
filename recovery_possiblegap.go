package sukko

import (
	"slices"
	"sync"
)

// The *PossibleGap snapshot lifecycle (Slice 4). A *PossibleGap tells the caller
// "records may have been lost on these channels and the SDK cannot replay them" — the
// data-loss signal for the Direct backend, a Kafka channel that saw no live message
// before a drop, and every un-anchorable gap.
//
// The reconnect-path signal is snapshot-driven: at each epoch death the channels that
// were GRANTED in that epoch but hold NO pos cursor (nothing for reconnect{last_pos}
// to anchor a replay on) union into a pending set — never the DESIRED set, so a
// permanently-denied channel yields no false signal. The pending set is drained and
// emitted, coalesced into ONE *PossibleGap{Channels}, when a reconnect completes
// (reconnect_ack, or reconnect_error{not_available}); a full-cursor Kafka reconnect
// leaves it empty and emits nothing. A reconnect that dies before its ack never drains
// — so the pending set unions across failed reconnects (union-on-retake) and is
// emitted before the final *Terminal if the client dies still holding one.
//
// It is a leaf-mutex type (the posCursor shape): touched at epoch-down and
// reconnect-ack on the supervisor goroutine, and at terminalSequence, which can run on
// a different goroutine (a never-connected Close). The lock is uncontended in practice
// and never held across an I/O park.
type possibleGaps struct {
	mu      sync.Mutex
	pending map[string]struct{}
}

func newPossibleGaps() *possibleGaps {
	return &possibleGaps{pending: map[string]struct{}{}}
}

// add unions the cursorless subset of `granted` into the pending set: the channels
// that were granted in the ended epoch and hold no cursor. The cursor snapshot is read
// BEFORE the lock so the leaf ordering is cursor.mu → (released) → possibleGaps.mu,
// never nested.
func (p *possibleGaps) add(granted []string, cursor *posCursor) {
	cur := cursor.snapshot()
	p.mu.Lock()
	for _, ch := range granted {
		if _, held := cur[ch]; !held {
			p.pending[ch] = struct{}{}
		}
	}
	p.mu.Unlock()
}

// drainIf offers the sorted pending set to emit and clears it ONLY if emit reports the
// event landed — so a *PossibleGap that could not be admitted is retained for a later
// drain rather than silently lost (the whole point of this slice). It reports whether
// it drained, and is a no-op returning false when nothing is pending. emit runs under
// the lock but performs only a non-blocking reserve send (delivery.trySend), never a
// parking send, so the leaf lock is not held across an I/O wait (§VII).
func (p *possibleGaps) drainIf(emit func(channels []string) bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pending) == 0 {
		return false
	}
	channels := make([]string, 0, len(p.pending))
	for ch := range p.pending {
		channels = append(channels, ch)
	}
	slices.Sort(channels)
	if emit(channels) {
		clear(p.pending)
		return true
	}
	return false
}

// emitPossibleGap drains the pending snapshot into one coalesced *PossibleGap via the
// non-blocking reserve send, clearing the snapshot only on a successful admission and
// counting one emission (never one per channel). Called on a completed reconnect
// (reconnect_ack / reconnect_error{not_available}) and at teardown.
func (c *Client) emitPossibleGap() {
	c.possibleGaps.drainIf(func(channels []string) bool {
		if c.delivery.trySend(&PossibleGap{Channels: channels}) {
			c.counters.possibleGaps.Add(1)
			return true
		}
		return false
	})
}
