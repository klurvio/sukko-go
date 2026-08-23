package sukko

import (
	"sync/atomic"
	"time"
)

// Stats is an eventually-consistent snapshot of the client's counters.
//
// It is a plain value struct, deliberately: the internal counters are
// atomic.Int64 (lock-free, off the hot path's critical section), but
// sync/atomic.Int64 embeds a noCopy guard, so a struct of them cannot be
// returned by value without a go vet copylocks failure — which FR-011's own CI
// gate would catch. Stats and the internal counters are therefore distinct
// types, and Stats() copies field by field.
//
// Each field is individually atomic; the set is not one atomic observation, so
// the snapshot is eventually consistent — counters are mutually consistent only
// to the precision that six independent loads allow. There is no drops counter:
// drops are structurally impossible in this delivery design, so a zero would be
// the only value it could ever hold.
type Stats struct {
	// MessagesReceived counts *Message events delivered.
	MessagesReceived int64
	// Gaps counts gap frames received from the server.
	Gaps int64
	// PossibleGaps counts synthetic PossibleGap signals the SDK emitted.
	PossibleGaps int64
	// Replays counts replay requests the SDK issued.
	Replays int64
	// Reconnects counts successful reconnections.
	Reconnects int64
	// BackpressureBlocks counts blocked-on-full-channel episodes — the 0→1
	// transition of a delivery send parking, not the time spent parked.
	BackpressureBlocks int64
	// BackpressureReconnects counts reconnects induced by consumer back-pressure.
	BackpressureReconnects int64
	// Blocked is the total wall time delivery sends spent parked.
	Blocked time.Duration
	// UnknownEvents counts UnknownEvent values delivered.
	UnknownEvents int64
}

// counters is the internal, mutable counterpart to Stats.
//
// The fields are atomic.Int64 so hot-path producers increment them without a
// lock; Blocked is nanoseconds-as-int64 for the same reason (a time.Duration is
// an int64, so it accumulates atomically and converts back on snapshot). It is
// never copied — always held by pointer — which is what keeps the copylocks
// guarantee on Stats meaningful: the un-copyable type stays internal.
type counters struct {
	messagesReceived       atomic.Int64
	gaps                   atomic.Int64
	possibleGaps           atomic.Int64
	replays                atomic.Int64
	reconnects             atomic.Int64
	backpressureBlocks     atomic.Int64
	backpressureReconnects atomic.Int64
	blockedNanos           atomic.Int64
	unknownEvents          atomic.Int64
}

// snapshot copies the counters into a Stats value.
//
// The six-plus loads are individually atomic but not collectively so; the
// returned Stats is documented as eventually consistent for exactly that reason.
func (c *counters) snapshot() Stats {
	return Stats{
		MessagesReceived:       c.messagesReceived.Load(),
		Gaps:                   c.gaps.Load(),
		PossibleGaps:           c.possibleGaps.Load(),
		Replays:                c.replays.Load(),
		Reconnects:             c.reconnects.Load(),
		BackpressureBlocks:     c.backpressureBlocks.Load(),
		BackpressureReconnects: c.backpressureReconnects.Load(),
		Blocked:                time.Duration(c.blockedNanos.Load()),
		UnknownEvents:          c.unknownEvents.Load(),
	}
}

// addBlocked accumulates a parked interval. It takes a duration rather than two
// timestamps so the caller reads the clock (Clock.Now, never time.Now) and this
// stays a pure accumulator.
func (c *counters) addBlocked(d time.Duration) {
	c.blockedNanos.Add(int64(d))
}
