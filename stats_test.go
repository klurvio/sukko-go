package sukko

import (
	"testing"
	"time"
)

// TestStatsIsCopyableByValue is the copylocks guard, expressed as code rather
// than left to the linter alone: if Stats ever gained an atomic or a mutex, this
// would stop compiling (and go vet would flag the assignment). The whole reason
// Stats and counters are separate types is to keep this true.
func TestStatsIsCopyableByValue(t *testing.T) {
	var c counters
	c.messagesReceived.Store(3)

	s := c.snapshot() // returned by value
	s2 := s           // copied by value — a copylocks failure if Stats held a lock
	if s2.MessagesReceived != 3 {
		t.Fatalf("copied Stats lost its value: %d", s2.MessagesReceived)
	}
}

// TestSnapshotReflectsCounters checks each counter reaches its Stats field —
// the mapping is hand-written, so a mis-wired field (Gaps reading replays) is a
// real risk this pins.
func TestSnapshotReflectsCounters(t *testing.T) {
	var c counters
	c.messagesReceived.Store(1)
	c.gaps.Store(2)
	c.possibleGaps.Store(3)
	c.replays.Store(4)
	c.reconnects.Store(5)
	c.backpressureBlocks.Store(6)
	c.backpressureReconnects.Store(7)
	c.unknownEvents.Store(9)
	c.addBlocked(8 * time.Second)

	got := c.snapshot()
	want := Stats{
		MessagesReceived:       1,
		Gaps:                   2,
		PossibleGaps:           3,
		Replays:                4,
		Reconnects:             5,
		BackpressureBlocks:     6,
		BackpressureReconnects: 7,
		Blocked:                8 * time.Second,
		UnknownEvents:          9,
	}
	if got != want {
		t.Errorf("snapshot() = %+v, want %+v", got, want)
	}
}

// TestBackpressureBlocksIsAnEpisodeCounter documents the semantic the spec pins:
// BackpressureBlocks counts parking *episodes* (the 0→1 transition), and Blocked
// accumulates the *duration*. They are distinct measures of the same event, and
// conflating them is the mistake this test exists to prevent — a producer that
// bumped BackpressureBlocks per nanosecond blocked would pass a naive test but
// violate the contract.
func TestBackpressureBlocksIsAnEpisodeCounter(t *testing.T) {
	var c counters

	// One episode lasting 5s: one increment of the episode counter, 5s of
	// blocked time — not five increments, not 5 in the block counter.
	c.backpressureBlocks.Add(1)
	c.addBlocked(5 * time.Second)

	got := c.snapshot()
	if got.BackpressureBlocks != 1 {
		t.Errorf("BackpressureBlocks = %d, want 1 (one episode)", got.BackpressureBlocks)
	}
	if got.Blocked != 5*time.Second {
		t.Errorf("Blocked = %s, want 5s", got.Blocked)
	}
}

// TestBlockedAccumulates confirms addBlocked sums rather than replaces — two
// episodes' durations add.
func TestBlockedAccumulates(t *testing.T) {
	var c counters
	c.addBlocked(2 * time.Second)
	c.addBlocked(3 * time.Second)

	if got := c.snapshot().Blocked; got != 5*time.Second {
		t.Errorf("Blocked = %s, want 5s (2s + 3s)", got)
	}
}
