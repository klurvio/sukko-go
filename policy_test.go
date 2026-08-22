package sukko

import "testing"

// The four policy tables in policy.go are the single source that NFR-005(kk)
// (termination classification) and NFR-005(ggg) (the *StateChange matrix) are
// generated from. These tests pin the tables themselves: every row the spec
// enumerates resolves to the class the spec assigns, lookups are total, and the
// enumerations stay closed. If a table and its matrix could drift, the matrix
// would be asserting whatever the table happened to say.

func TestClosePolicyClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		key       closeKey
		want      terminationClass
		wantEvent bool // whether the row surfaces a *CloseError
	}{
		// A local 1000 is the caller's own Close: a clean stop that surfaces
		// *Terminal{Err: nil} rather than a *CloseError.
		{"local normal close", closeKey{1000, directionLocal, true}, classCleanStop, false},
		// A remote 1000 is the server closing normally — still worth reconnecting.
		{"remote normal close", closeKey{1000, directionRemote, true}, classReconnect, true},
		{"server going away", closeKey{1001, directionRemote, true}, classReconnect, true},
		// 1006 is synthesized by the library when no close frame arrived.
		{"abnormal closure", closeKey{1006, directionLocal, true}, classReconnect, true},
		// The slow-client path: reconnect and recover, and count the cycle.
		{"slow client", closeKey{1008, directionRemote, true}, classReconnect, true},
		{"server internal error", closeKey{1011, directionRemote, true}, classReconnect, true},
		// Operator force-disconnect. Re-establishing immediately would defeat
		// the operator, so this is the one remote code that is terminal.
		{"force disconnect", closeKey{4000, directionRemote, true}, classTerminal, true},
		// The SDK's own heartbeat-timeout close.
		{"heartbeat timeout", closeKey{CloseCodeHeartbeatTimeout, directionLocal, true}, classReconnect, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := lookupClosePolicy(tc.key)
			if !ok {
				t.Fatalf("closePolicy has no row for %+v", tc.key)
			}
			if got.class != tc.want {
				t.Errorf("class = %v, want %v", got.class, tc.want)
			}
			if got.surfacesCloseError != tc.wantEvent {
				t.Errorf("surfacesCloseError = %v, want %v", got.surfacesCloseError, tc.wantEvent)
			}
		})
	}
}

// TestClosePolicyReconnectDisabled pins the WithReconnect(false) rule: every
// reconnect-class outcome becomes a clean-stop, so the client always lands in a
// resting state with a *Terminal delivered and Messages() closed. Terminal-class
// rows are unaffected — they already stop.
func TestClosePolicyReconnectDisabled(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		code int
		dir  closeDirection
		want terminationClass
	}{
		{"reconnect-class becomes clean-stop", 1008, directionRemote, classCleanStop},
		{"another reconnect-class row", 1011, directionRemote, classCleanStop},
		{"terminal stays terminal", 4000, directionRemote, classTerminal},
		{"clean-stop stays clean-stop", 1000, directionLocal, classCleanStop},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := lookupClosePolicy(closeKey{tc.code, tc.dir, false})
			if !ok {
				t.Fatalf("closePolicy has no row for code %d dir %v reconnect=false", tc.code, tc.dir)
			}
			if got.class != tc.want {
				t.Errorf("class = %v, want %v", got.class, tc.want)
			}
		})
	}
}

// A local 4000 is never emitted: the SDK allocates from the top of the
// application range, so any observed 4000 is unambiguously the server's
// force-disconnect. The absence is deliberate and worth pinning — if a local
// 4000 ever resolved, the direction-based disambiguation would be broken.
func TestClosePolicyHasNoLocalForceDisconnect(t *testing.T) {
	t.Parallel()

	if _, ok := lookupClosePolicy(closeKey{4000, directionLocal, true}); ok {
		t.Error("closePolicy resolves a local 4000; the SDK must never emit that code")
	}
}

func TestHandshakePolicyClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		status   int
		bodyCode string
		want     terminationClass
	}{
		// 401 IS "auth-terminal": the contract defines no auth close code, so
		// auth failure at the upgrade is an HTTP status.
		{"unauthorized", 401, "", classTerminal},
		{"unauthorized with body code", 401, "INVALID_TOKEN", classTerminal},
		// A permanent authorization refusal; retrying cannot change it.
		{"forbidden", 403, "FORBIDDEN", classTerminal},
		{"forbidden any body code", 403, "", classTerminal},
		// Tenant connection cap: back off, never hammer.
		{"tenant limit", 429, "TENANT_LIMIT_EXCEEDED", classReconnect},
		{"server error", 503, "", classReconnect},
		// Fallback rows keep the enumeration total.
		{"unlisted 4xx falls back to terminal", 418, "", classTerminal},
		{"unlisted 5xx falls back to reconnect", 599, "", classReconnect},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := lookupHandshakePolicy(tc.status, tc.bodyCode, true)
			if got.class != tc.want {
				t.Errorf("status %d code %q: class = %v, want %v", tc.status, tc.bodyCode, got.class, tc.want)
			}
		})
	}
}

// The 429 row is the only handshake outcome whose Retry-After overrides the
// computed backoff delay, so the flag that carries that behaviour is pinned
// separately from the class.
func TestHandshakePolicyRetryAfterOverride(t *testing.T) {
	t.Parallel()

	if got := lookupHandshakePolicy(429, "TENANT_LIMIT_EXCEEDED", true); !got.retryAfterOverrides {
		t.Error("a handshake 429 must let a non-nil Retry-After override the backoff delay")
	}
	if got := lookupHandshakePolicy(503, "", true); got.retryAfterOverrides {
		t.Error("only the 429 row honours Retry-After as a backoff override")
	}
}

// There is no WS-handshake EDITION_LIMIT row, because the platform cannot
// produce one: the edition gate is middleware applied to four routes, and /ws is
// registered ungated. A row here would surface an *EditionRequiredError whose
// RequiredEdition no call site could populate.
func TestHandshakePolicyHasNoEditionLimitRow(t *testing.T) {
	t.Parallel()

	got := lookupHandshakePolicy(403, "EDITION_LIMIT", true)
	if got.class != classTerminal {
		t.Errorf("a 403 EDITION_LIMIT must classify as an ordinary terminal handshake failure, got %v", got.class)
	}
	if got.surfacesEditionError {
		t.Error("the WS handshake must not surface an *EditionRequiredError; /ws is ungated")
	}
}

func TestHandshakePolicyReconnectDisabled(t *testing.T) {
	t.Parallel()

	if got := lookupHandshakePolicy(429, "TENANT_LIMIT_EXCEEDED", false); got.class != classCleanStop {
		t.Errorf("with reconnect disabled a 429 is a clean-stop, got %v", got.class)
	}
	if got := lookupHandshakePolicy(401, "", false); got.class != classTerminal {
		t.Errorf("terminal-class handshake rows ignore the reconnect flag, got %v", got.class)
	}
}

// The two code-less rows: internal epoch-failure causes that terminate the
// client without a wire close. They are enumerated so the termination tables
// stay exhaustive.
func TestInternalCausePolicy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		cause internalCause
		want  terminationClass
	}{
		// A recovered panic is treated as a disconnect: tear the epoch down and
		// reconnect through the normal backoff path.
		{"recovered panic", causeRecoveredPanic, classReconnect},
		// The credential source gave up; reconnecting would consume a doomed
		// handshake, so this stops.
		{"token source exhausted", causeTokenSourceExhausted, classTerminal},
		// A consumer that never catches up.
		{"consumer too slow", causeConsumerTooSlow, classTerminal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := lookupInternalCausePolicy(tc.cause, true)
			if !ok {
				t.Fatalf("internalCausePolicy has no row for %v", tc.cause)
			}
			if got.class != tc.want {
				t.Errorf("class = %v, want %v", got.class, tc.want)
			}
		})
	}
}

// TestStateTransitions pins FR-009's entry/exit table, which this structure
// transcribes. NFR-005(ggg) generates the *StateChange matrix from it.
func TestStateTransitions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		from ConnectionState
		on   trigger
		want ConnectionState
	}{
		{"connect from rest", StateDisconnected, triggerConnectCalled, StateConnecting},
		{"close from rest", StateDisconnected, triggerCloseCalled, StateClosed},

		{"handshake succeeds", StateConnecting, triggerHandshakeOK, StateConnected},
		{"terminal dial failure", StateConnecting, triggerTerminalFailure, StateError},
		{"transient dial failure retries", StateConnecting, triggerReconnectClassFailure, StateReconnecting},
		// The caller-ctx-aborted dial. Without this row a Connect whose deadline
		// expired mid-dial left the client parked in connecting forever.
		{"caller aborted the dial", StateConnecting, triggerDialAborted, StateReconnecting},
		{"close while connecting", StateConnecting, triggerCloseCalled, StateClosed},

		{"epoch drops", StateConnected, triggerReconnectClassFailure, StateReconnecting},
		{"terminal outcome while connected", StateConnected, triggerTerminalFailure, StateError},
		{"close while connected", StateConnected, triggerCloseCalled, StateClosed},

		{"re-handshake succeeds", StateReconnecting, triggerHandshakeOK, StateConnected},
		{"gives up while reconnecting", StateReconnecting, triggerTerminalFailure, StateError},
		{"close while reconnecting", StateReconnecting, triggerCloseCalled, StateClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := lookupStateTransition(tc.from, tc.on)
			if !ok {
				t.Fatalf("no transition for %v on %v", tc.from, tc.on)
			}
			if got != tc.want {
				t.Errorf("%v on %v -> %v, want %v", tc.from, tc.on, got, tc.want)
			}
		})
	}
}

// With reconnect disabled, a reconnect-class failure is a clean-stop rather than
// a return to reconnecting — the same rule the close tables apply, expressed in
// the state machine so the two cannot disagree.
func TestStateTransitionsReconnectDisabled(t *testing.T) {
	t.Parallel()

	for _, from := range []ConnectionState{StateConnecting, StateConnected} {
		got, ok := lookupStateTransitionWithReconnect(from, triggerReconnectClassFailure, false)
		if !ok {
			t.Fatalf("no transition for %v on a reconnect-class failure with reconnect disabled", from)
		}
		if got != StateClosed {
			t.Errorf("%v with reconnect disabled -> %v, want %v", from, got, StateClosed)
		}
	}
}

// The resting states are terminal: nothing transitions out of them, which is
// what makes "every stop lands in one of them" true. A transition out of closed
// would mean the client could resurrect after its channel was closed.
func TestRestingStatesHaveNoExit(t *testing.T) {
	t.Parallel()

	for _, from := range []ConnectionState{StateClosed, StateError} {
		for _, on := range allTriggers() {
			if to, ok := lookupStateTransition(from, on); ok {
				t.Errorf("%v is resting but transitions to %v on %v", from, to, on)
			}
		}
	}
}

// Every non-resting state must be able to reach a resting one, or a client could
// stall somewhere with no way to terminate — the §XI unreachable-state check
// applied in the other direction.
func TestEveryLiveStateCanTerminate(t *testing.T) {
	t.Parallel()

	for _, from := range []ConnectionState{StateDisconnected, StateConnecting, StateConnected, StateReconnecting} {
		to, ok := lookupStateTransition(from, triggerCloseCalled)
		if !ok || to != StateClosed {
			t.Errorf("%v cannot reach %v via Close (got %v, ok=%v)", from, StateClosed, to, ok)
		}
	}
}

// ─── structural checks ───
//
// The four tables claim to be exhaustive enumerations, and two test matrices are
// generated from them. That only holds if every lookup is total: a key that
// resolves to nothing would silently drop out of the generated matrix, and the
// case would go untested while the suite still reported green.

// Every close row must classify, and the reconnect flag must never change a
// row's existence — only its class. A key that resolved with reconnect enabled
// and vanished without it would drop half the matrix.
func TestClosePolicyIsTotalUnderBothReconnectSettings(t *testing.T) {
	t.Parallel()

	for key := range closeBaseRows {
		for _, reconnect := range []bool{true, false} {
			out, ok := lookupClosePolicy(closeKey{key.code, key.direction, reconnect})
			if !ok {
				t.Errorf("closePolicy has no row for %+v with reconnect=%v", key, reconnect)
				continue
			}
			if !isKnownClass(out.class) {
				t.Errorf("%+v resolved to an unknown class %v", key, out.class)
			}
		}
	}
}

// Disabling reconnect may only ever remove a retry. If it changed a terminal
// row into a clean stop, a client would report Err() nil for a failure — or
// worse, turn a clean stop into a terminal error and report a failure that did
// not happen.
func TestReconnectFlagOnlyDowngradesReconnectRows(t *testing.T) {
	t.Parallel()

	for key := range closeBaseRows {
		enabled, _ := lookupClosePolicy(closeKey{key.code, key.direction, true})
		disabled, _ := lookupClosePolicy(closeKey{key.code, key.direction, false})

		switch enabled.class {
		case classReconnect:
			if disabled.class != classCleanStop {
				t.Errorf("%+v: reconnect-class became %v with reconnect disabled, want clean-stop",
					key, disabled.class)
			}
		default:
			if disabled.class != enabled.class {
				t.Errorf("%+v: %v changed to %v with reconnect disabled; the flag must only remove retries",
					key, enabled.class, disabled.class)
			}
		}
	}
}

// The handshake table is total by construction — unlisted statuses fall back by
// class — so no HTTP status may leave an outcome unclassified.
func TestHandshakePolicyIsTotal(t *testing.T) {
	t.Parallel()

	for _, status := range []int{
		100, 200, 301, 400, 401, 402, 403, 404, 418, 429, 451,
		500, 502, 503, 504, 599,
	} {
		for _, reconnect := range []bool{true, false} {
			out := lookupHandshakePolicy(status, "", reconnect)
			if !isKnownClass(out.class) {
				t.Errorf("status %d (reconnect=%v) resolved to an unknown class %v",
					status, reconnect, out.class)
			}
		}
	}
}

// Every internal cause must classify under both settings, for the same reason
// the close table must.
func TestInternalCausePolicyIsTotal(t *testing.T) {
	t.Parallel()

	for cause := range internalCauseRows {
		for _, reconnect := range []bool{true, false} {
			out, ok := lookupInternalCausePolicy(cause, reconnect)
			if !ok {
				t.Errorf("internalCausePolicy has no row for %v (reconnect=%v)", cause, reconnect)
				continue
			}
			if !isKnownClass(out.class) {
				t.Errorf("%v resolved to an unknown class %v", cause, out.class)
			}
		}
	}
}

// Every transition must land on one of the six states. A transition to a value
// outside the enum would leave State() reporting something no caller can match,
// which is the defect the six-value closed set exists to prevent.
func TestStateTransitionsLandInsideTheEnum(t *testing.T) {
	t.Parallel()

	valid := map[ConnectionState]bool{
		StateDisconnected: true, StateConnecting: true, StateConnected: true,
		StateReconnecting: true, StateError: true, StateClosed: true,
	}

	for key, to := range stateTransitions {
		if !valid[key.from] {
			t.Errorf("transition from %q, which is not one of the six states", key.from)
		}
		if !valid[to] {
			t.Errorf("%v on %v lands on %q, which is not one of the six states", key.from, key.on, to)
		}
	}
}

// Every state must be reachable. An unreachable state is dead API: it appears
// in the enum, a caller writes a branch for it, and the branch never runs.
func TestEveryStateIsReachable(t *testing.T) {
	t.Parallel()

	reached := map[ConnectionState]bool{
		// The initial state needs no transition to reach it.
		StateDisconnected: true,
	}
	for _, to := range stateTransitions {
		reached[to] = true
	}

	for _, state := range []ConnectionState{
		StateDisconnected, StateConnecting, StateConnected,
		StateReconnecting, StateError, StateClosed,
	} {
		if !reached[state] {
			t.Errorf("state %q is unreachable; no transition lands on it", state)
		}
	}
}

// Every non-resting state must reach a resting one under some trigger, or a
// client could stall somewhere with no way to terminate.
func TestEveryLiveStateHasAPathToRest(t *testing.T) {
	t.Parallel()

	for _, from := range []ConnectionState{
		StateDisconnected, StateConnecting, StateConnected, StateReconnecting,
	} {
		var canRest bool
		for _, on := range allTriggers() {
			if to, ok := lookupStateTransition(from, on); ok && (to == StateClosed || to == StateError) {
				canRest = true
				break
			}
		}
		if !canRest {
			t.Errorf("state %q has no transition to a resting state", from)
		}
	}
}

// The class vocabulary is closed: three values, each with a distinct name. A
// duplicate name would make a test failure ambiguous about which class was meant.
func TestTerminationClassVocabularyIsClosed(t *testing.T) {
	t.Parallel()

	names := map[string]bool{}
	for _, class := range []terminationClass{classReconnect, classCleanStop, classTerminal} {
		name := class.String()
		if name == "unknown" {
			t.Errorf("class %d renders as \"unknown\"", class)
		}
		if names[name] {
			t.Errorf("duplicate class name %q", name)
		}
		names[name] = true
	}
	if len(names) != 3 {
		t.Errorf("expected 3 distinct classes, got %d", len(names))
	}
	// An out-of-range value must degrade rather than index past the switch.
	if got := terminationClass(99).String(); got != "unknown" {
		t.Errorf("out-of-range class rendered as %q, want \"unknown\"", got)
	}
}

// isKnownClass reports whether a class is one of the three.
func isKnownClass(c terminationClass) bool {
	return c == classReconnect || c == classCleanStop || c == classTerminal
}
