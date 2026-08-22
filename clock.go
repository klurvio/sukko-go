package sukko

import (
	"math/rand/v2"
	"time"
)

// This file holds the determinism seams: every wall-clock read and every random
// draw in the SDK goes through them.
//
// Go's race detector proves the SDK is free of data races, but it says nothing
// about whether a backoff schedule is correct. That needs a fake clock — and a
// fake clock only helps if the test can rendezvous with the timer it is about to
// fire. Hence the purpose name on every timer: a test waits for "backoff"
// specifically, rather than for "some number of timers", which would couple it
// to however many unrelated timers the client happened to have armed.
//
// The production implementations are the obvious ones. BlockUntilTimer is a
// no-op on them: it is a test rendezvous, and a live client must never block on
// it.

// timerPurpose names a timing path. The set is closed — an unlisted path is a
// real sleep waiting to happen, which is exactly what these seams exist to
// prevent.
type timerPurpose string

const (
	// purposeBackoff is the delay between reconnect attempts.
	purposeBackoff timerPurpose = "backoff"
	// purposeHeartbeat is the app-level heartbeat send interval.
	purposeHeartbeat timerPurpose = "heartbeat"
	// purposePong is the deadline for a pong reply.
	purposePong timerPurpose = "pong"
	// purposeRefresh is the proactive credential-refresh schedule.
	purposeRefresh timerPurpose = "refresh"
	// purposeReplayFloor is the per-channel floor between replay requests.
	purposeReplayFloor timerPurpose = "replay_floor"
	// purposeRecoveryDeadline is the missing-terminator detection deadline.
	purposeRecoveryDeadline timerPurpose = "recovery_deadline"
	// purposeAckTimeout is the subscribe serializer's ack deadline.
	purposeAckTimeout timerPurpose = "ack_timeout"
	// purposeBlockedWarn is the delay before warning that delivery has stalled.
	purposeBlockedWarn timerPurpose = "blocked_warn"
	// purposeTokenSource bounds a single TokenSource call.
	purposeTokenSource timerPurpose = "token_source"
	// purposeSSEIdle is the no-bytes deadline on an SSE stream.
	purposeSSEIdle timerPurpose = "sse_idle"
	// purposeDial bounds a reconnect dial.
	purposeDial timerPurpose = "dial"
)

// allTimerPurposes returns the closed set, for exhaustiveness checks.
func allTimerPurposes() []timerPurpose {
	return []timerPurpose{
		purposeBackoff, purposeHeartbeat, purposePong, purposeRefresh,
		purposeReplayFloor, purposeRecoveryDeadline, purposeAckTimeout,
		purposeBlockedWarn, purposeTokenSource, purposeSSEIdle, purposeDial,
	}
}

// Timer is a one-shot timer. It mirrors time.Timer closely enough to be
// substitutable, but exposes its channel through a method so a fake can supply
// one it controls.
type Timer interface {
	// C returns the channel the firing time is delivered on.
	C() <-chan time.Time
	// Stop prevents the timer from firing, reporting whether it did so before
	// the timer fired.
	Stop() bool
}

// Ticker is a repeating timer.
type Ticker interface {
	// C returns the channel tick times are delivered on.
	C() <-chan time.Time
	// Stop stops the ticker. It does not close the channel.
	Stop()
}

// Clock is the SDK's only source of time. Nothing in the package calls
// time.Now, time.After, time.NewTimer or time.NewTicker directly — a mixed
// clock, where timers are fake but Now is real, is the classic way a
// "deterministic" suite drifts on a loaded CI machine.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer arms a one-shot timer under a purpose name.
	NewTimer(d time.Duration, purpose timerPurpose) Timer
	// NewTicker arms a repeating timer under a purpose name.
	NewTicker(d time.Duration, purpose timerPurpose) Ticker
	// BlockUntilTimer blocks until a timer with the given purpose is armed.
	//
	// It is a test rendezvous: a test calls it before advancing the clock, so
	// the advance cannot land before the goroutine under test has armed its
	// timer — the lost-wakeup that makes fake-clock suites flaky. On the
	// production clock it returns immediately.
	BlockUntilTimer(purpose timerPurpose)
}

// Rand supplies the randomness for backoff jitter. It is a seam so a seeded
// implementation yields an exactly assertable delay sequence; "exponential with
// jitter" is otherwise untestable beyond "some delay happened".
type Rand interface {
	// Float64 returns a pseudo-random number in [0,1).
	Float64() float64
}

// realClock is the production Clock, backed by the time package.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration, _ timerPurpose) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

func (realClock) NewTicker(d time.Duration, _ timerPurpose) Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

// BlockUntilTimer is a no-op on the production clock: there is nothing to
// rendezvous with when time advances on its own.
func (realClock) BlockUntilTimer(timerPurpose) {}

// realTimer adapts time.Timer to the Timer interface.
type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }

// realTicker adapts time.Ticker to the Ticker interface.
type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

// realRand is the production Rand, backed by math/rand/v2. The global source is
// already safe for concurrent use, so this needs no locking of its own.
type realRand struct{}

// Float64 returns a pseudo-random number in [0,1).
//
// This value is used for one thing: spreading reconnect backoff so a fleet of
// clients does not re-dial in lockstep after a server restart. It is not a
// secret, is never used as one, and is not an input to any authentication,
// signing, or identifier the SDK produces — client IDs come from the caller.
// A predictable jitter draw lets an observer guess when a client will retry,
// which is information the retry itself reveals a moment later anyway.
//
// crypto/rand would be the wrong instrument: it can fail, forcing an error path
// onto the reconnect timer for no security gain.
//
//nolint:gosec // G404: non-cryptographic by design — backoff jitter only, never a secret or an identifier.
func (realRand) Float64() float64 { return rand.Float64() }
