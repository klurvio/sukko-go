package sukko

import (
	"testing"
	"time"
)

// TestDefaultsByValue asserts every exported default from the FR-001c options
// table by value, per NFR-005(dd) ("every default in the table is asserted by
// value"). These are public API frozen from v0.1.0 and several must track a
// server-side setting; the comment on each names what it tracks, so a drifting
// server default shows up here as a failing assertion rather than a support
// ticket.
func TestDefaultsByValue(t *testing.T) {
	t.Parallel()

	t.Run("capacity", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name string
			got  int
			want int
		}{
			// Matches the server's WS_CLIENT_SEND_BUFFER_SIZE, and clears the
			// construction floor below with room to spare.
			{"DefaultQueueSize", DefaultQueueSize, 512},
			// The server's WS_HISTORY_MAX_LIMIT *default*.
			{"DefaultHistoryLimit", DefaultHistoryLimit, 100},
			// The server's validated ceiling for that env var — a default is not
			// a maximum, so validation uses this and not DefaultHistoryLimit.
			{"MaxHistoryLimit", MaxHistoryLimit, 1000},
			// Contract bound (WS_MAX_REPLAY_MESSAGES); the server enforces it, so
			// it is not a client knob.
			{"MaxReplayMessages", MaxReplayMessages, 100},
			// 1 reserved *Terminal slot + 15 for the bounded safety subset.
			{"AdvisoryHeadroom", AdvisoryHeadroom, 16},
			// Must match the server's GATEWAY_MAX_PUBLISH_SIZE.
			{"DefaultMaxPublishSize", DefaultMaxPublishSize, 64 * 1024},
			{"DefaultMaxBackpressureReconnects", DefaultMaxBackpressureReconnects, 5},
			// "{tenant}.{suffix}" — at least two dot-separated parts.
			{"MinChannelParts", MinChannelParts, 2},
		} {
			if tc.got != tc.want {
				t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
			}
		}
	})

	t.Run("durations", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name string
			got  time.Duration
			want time.Duration
		}{
			{"DefaultDialTimeout", DefaultDialTimeout, 10 * time.Second},
			// Comfortably under the gateway's 60s GATEWAY_IDLE_TIMEOUT.
			{"DefaultHeartbeatInterval", DefaultHeartbeatInterval, 30 * time.Second},
			{"DefaultPongTimeout", DefaultPongTimeout, 10 * time.Second},
			// GATEWAY_AUTH_REFRESH_RATE_INTERVAL.
			{"DefaultRefreshMinInterval", DefaultRefreshMinInterval, 30 * time.Second},
			{"DefaultRefreshLead", DefaultRefreshLead, 60 * time.Second},
			// WS_REPLAY_RATE_LIMIT_INTERVAL.
			{"DefaultReplayFloor", DefaultReplayFloor, 10 * time.Second},
			// 2× the server's WS_REPLAY_TIMEOUT of 5s.
			{"DefaultRecoveryDeadline", DefaultRecoveryDeadline, 10 * time.Second},
			// 2× the gateway's 45s SSE_KEEPALIVE_INTERVAL.
			{"DefaultSSEIdleTimeout", DefaultSSEIdleTimeout, 90 * time.Second},
			{"DefaultTokenSourceTimeout", DefaultTokenSourceTimeout, 10 * time.Second},
		} {
			if tc.got != tc.want {
				t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
			}
		}
	})

	t.Run("backoff", func(t *testing.T) {
		t.Parallel()
		want := BackoffConfig{
			Initial:    500 * time.Millisecond,
			Max:        30 * time.Second,
			Multiplier: 2.0,
			Jitter:     0.2,
		}
		if DefaultBackoff != want {
			t.Errorf("DefaultBackoff = %+v, want %+v", DefaultBackoff, want)
		}
	})

	t.Run("close codes", func(t *testing.T) {
		t.Parallel()
		// 4000, direction-disambiguated — the contract's value and both sibling
		// SDKs' (@sukko/sdk, sukko-py) value for a client heartbeat-timeout close.
		if CloseCodeHeartbeatTimeout != 4000 {
			t.Errorf("CloseCodeHeartbeatTimeout = %d, want 4000 (contract + sibling parity)", CloseCodeHeartbeatTimeout)
		}
	})
}

// TestDefaultQueueSizeClearsTheFloor is the load-bearing relationship between
// four of the constants above, and the reason SC-001's zero-config quickstart
// constructs at all: NewClient rejects a QueueSize below
// HistoryLimit + MaxReplayMessages + AdvisoryHeadroom, so the default must
// clear that floor by construction. Asserting the arithmetic here means a
// future edit to any one constant fails loudly instead of turning the
// zero-config path into a constructor error.
func TestDefaultQueueSizeClearsTheFloor(t *testing.T) {
	t.Parallel()

	floor := DefaultHistoryLimit + MaxReplayMessages + AdvisoryHeadroom
	if floor != 216 {
		t.Errorf("capacity floor = %d, want 216", floor)
	}
	if DefaultQueueSize < floor {
		t.Fatalf("DefaultQueueSize %d is below the floor %d — the zero-config path would not construct",
			DefaultQueueSize, floor)
	}
}

// TestDefaultHeartbeatOutlivesPongTimeout pins the other cross-constant
// invariant the options table states as a validation rule
// (PongTimeout < HeartbeatInterval). If the defaults themselves violated it,
// a zero-config client would fail its own validation.
func TestDefaultHeartbeatOutlivesPongTimeout(t *testing.T) {
	t.Parallel()

	if DefaultPongTimeout >= DefaultHeartbeatInterval {
		t.Errorf("DefaultPongTimeout (%v) must be < DefaultHeartbeatInterval (%v)",
			DefaultPongTimeout, DefaultHeartbeatInterval)
	}
}

// TestDefaultBackoffSatisfiesItsOwnValidation pins the backoff validation rules
// from the options table against the shipped default, for the same reason: the
// default must not be a value the constructor would reject.
func TestDefaultBackoffSatisfiesItsOwnValidation(t *testing.T) {
	t.Parallel()

	b := DefaultBackoff
	if b.Initial <= 0 || b.Initial > b.Max {
		t.Errorf("DefaultBackoff violates 0 < Initial <= Max: %+v", b)
	}
	if b.Multiplier <= 1 {
		t.Errorf("DefaultBackoff violates Multiplier > 1: %v", b.Multiplier)
	}
	if b.Jitter < 0 || b.Jitter > 1 {
		t.Errorf("DefaultBackoff violates 0 <= Jitter <= 1: %v", b.Jitter)
	}
}
