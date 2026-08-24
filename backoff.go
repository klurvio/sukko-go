package sukko

import (
	"math"
	"time"
)

// computeBackoffDelay implements the pinned reconnect-backoff formula (FR-006):
//
//	base  = min(Initial × Multiplier^attempt, Max)
//	delay = base × (1 + Jitter × (2r − 1))    clamped to [0, Max]
//
// where attempt is 0 for the first reconnect after an epoch ends, and r is
// exactly one Rand.Float64() draw taken AFTER base is computed — one draw per
// call, and none at all when Jitter is zero. The formula is pinned this
// precisely so a seeded Rand yields an assertable delay sequence.
func computeBackoffDelay(b BackoffConfig, attempt int, r Rand) time.Duration {
	// base = Initial × Multiplier^attempt, saturated at Max. The growth is
	// computed in float seconds so a large attempt cannot overflow the
	// exponential before the Max clamp applies.
	base := float64(b.Initial) * math.Pow(b.Multiplier, float64(attempt))
	maxNs := float64(b.Max)
	if base > maxNs {
		base = maxNs
	}

	if b.Jitter == 0 {
		// No spread, no draw — the "none when Jitter is zero" rule.
		return time.Duration(base)
	}

	// One draw, after base. spread ∈ [−Jitter, +Jitter].
	spread := b.Jitter * (2*r.Float64() - 1)
	delay := base * (1 + spread)

	// Clamp to [0, Max]: jitter can push below zero (large Jitter, low draw) or
	// above Max (high draw on a near-saturated base).
	switch {
	case delay < 0:
		delay = 0
	case delay > maxNs:
		delay = maxNs
	}
	return time.Duration(delay)
}
