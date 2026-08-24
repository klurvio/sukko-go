package sukko

import (
	"testing"
	"time"
)

// TestComputeBackoffDelayNoJitter pins the exponential base sequence with jitter
// disabled: attempt n waits Initial × Multiplier^n, saturating at Max, and takes
// zero Rand draws (the "none when Jitter is zero" rule).
func TestComputeBackoffDelayNoJitter(t *testing.T) {
	b := BackoffConfig{Initial: 500 * time.Millisecond, Max: 30 * time.Second, Multiplier: 2.0, Jitter: 0}
	r := newFakeRand()

	want := []time.Duration{
		500 * time.Millisecond, // n=0
		1 * time.Second,        // n=1
		2 * time.Second,        // n=2
		4 * time.Second,        // n=3
		8 * time.Second,        // n=4
		16 * time.Second,       // n=5
		30 * time.Second,       // n=6 — 32s base capped to Max
		30 * time.Second,       // n=7 — still capped
	}
	for n, wantDelay := range want {
		if got := computeBackoffDelay(b, n, r); got != wantDelay {
			t.Errorf("attempt %d: delay = %v, want %v", n, got, wantDelay)
		}
	}
	if r.draws() != 0 {
		t.Errorf("draws = %d, want 0 — jitter disabled must draw no randomness", r.draws())
	}
}

// TestComputeBackoffDelayJitterMidpoint confirms one draw per attempt and that
// the midpoint draw (0.5) leaves the base delay unchanged: 2r−1 = 0.
func TestComputeBackoffDelayJitterMidpoint(t *testing.T) {
	b := BackoffConfig{Initial: 1 * time.Second, Max: 30 * time.Second, Multiplier: 2.0, Jitter: 0.2}
	r := newFakeRand() // 0.5 → jitter term vanishes

	for n, wantDelay := range []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second} {
		if got := computeBackoffDelay(b, n, r); got != wantDelay {
			t.Errorf("attempt %d: delay = %v, want %v (midpoint draw leaves base)", n, got, wantDelay)
		}
	}
	if r.draws() != 3 {
		t.Errorf("draws = %d, want 3 — exactly one draw per attempt", r.draws())
	}
}

// TestComputeBackoffDelayJitterBounds checks the jitter endpoints: r=0 spreads
// the delay down by the full Jitter fraction, and a draw that would push the
// result above Max is clamped.
func TestComputeBackoffDelayJitterBounds(t *testing.T) {
	b := BackoffConfig{Initial: 1 * time.Second, Max: 30 * time.Second, Multiplier: 2.0, Jitter: 0.5}

	// r=0 → 2r−1 = −1 → delay = base × (1 − 0.5) = base/2.
	if got := computeBackoffDelay(b, 0, newFakeRand(0)); got != 500*time.Millisecond {
		t.Errorf("r=0: delay = %v, want 500ms (base × 0.5)", got)
	}
	// A jitter=1 draw at r=0 zeroes the delay (clamped at the [0,Max] floor).
	full := BackoffConfig{Initial: 1 * time.Second, Max: 30 * time.Second, Multiplier: 2.0, Jitter: 1.0}
	if got := computeBackoffDelay(full, 0, newFakeRand(0)); got != 0 {
		t.Errorf("jitter=1, r=0: delay = %v, want 0 (clamped floor)", got)
	}
	// r near 1 at the Max-saturated base stays clamped to Max, never above.
	if got := computeBackoffDelay(b, 10, newFakeRand(0.999)); got > b.Max {
		t.Errorf("saturated base with high jitter: delay = %v, want ≤ Max %v", got, b.Max)
	}
}
