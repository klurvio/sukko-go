package sukko

// This file holds the four policy tables that classify every way a connection
// can end, and the state machine those endings drive.
//
// They are data, not code, for one reason: the test matrices that assert
// termination behavior and state-change events are generated from these
// structures. A table and its matrix therefore cannot drift — adding a row adds
// its test case. The tables claim to be exhaustive, so a termination that is not
// enumerated here is a defect in the enumeration, not an unhandled case.

// terminationClass is the three-way vocabulary every ending is classified into.
type terminationClass int

const (
	// classReconnect means the SDK re-dials through the normal backoff path.
	classReconnect terminationClass = iota
	// classCleanStop means the client stops without a failure: the final
	// *Terminal is delivered, Messages() is closed, and Err() stays nil. It is
	// what the caller asked for.
	classCleanStop
	// classTerminal means the client stops on a failure it will not retry:
	// Messages() is closed and Err() is non-nil.
	classTerminal
)

// String makes the class readable in test failures and log lines.
func (c terminationClass) String() string {
	switch c {
	case classReconnect:
		return "reconnect"
	case classCleanStop:
		return "clean-stop"
	case classTerminal:
		return "terminal"
	default:
		return "unknown"
	}
}

// closeDirection records who initiated a close. It is what disambiguates codes
// both sides can send — a local 1000 is the caller's own Close, a remote 1000 is
// the server closing normally, and only one of those is worth reconnecting.
type closeDirection int

const (
	// directionLocal marks a close the SDK initiated.
	directionLocal closeDirection = iota
	// directionRemote marks a close the server initiated.
	directionRemote
)

// String makes the direction readable in test failures and log lines.
func (d closeDirection) String() string {
	if d == directionLocal {
		return "local"
	}
	return "remote"
}

// closeKey identifies a row of closePolicy. reconnectEnabled is part of the key
// because WithReconnect(false) turns every reconnect-class outcome into a
// clean-stop, and a policy that ignored the flag would need the caller to
// re-derive it at every call site.
type closeKey struct {
	code             int
	direction        closeDirection
	reconnectEnabled bool
}

// closeOutcome is what a close resolves to.
type closeOutcome struct {
	class terminationClass
	// surfacesCloseError is false only for the caller's own Close, which
	// surfaces *Terminal{Err: nil} instead — there is no error to report.
	surfacesCloseError bool
	// countsTowardBackpressure marks the slow-client close, the one ending that
	// feeds the consecutive-cycle counter behind ErrConsumerTooSlow.
	countsTowardBackpressure bool
}

// closeBaseRows maps a close to its classification with reconnect enabled.
// Applying WithReconnect(false) is a uniform transformation over this table
// rather than a second copy of it — see lookupClosePolicy.
//
// A local 4000 is deliberately absent: the SDK allocates from the top of the
// application range, so an observed 4000 is unambiguously the server's
// force-disconnect and the direction alone identifies it.
var closeBaseRows = map[struct {
	code      int
	direction closeDirection
}]closeOutcome{
	{1000, directionLocal}:  {class: classCleanStop, surfacesCloseError: false},
	{1000, directionRemote}: {class: classReconnect, surfacesCloseError: true},
	{1001, directionRemote}: {class: classReconnect, surfacesCloseError: true},
	{1006, directionLocal}:  {class: classReconnect, surfacesCloseError: true},
	{1008, directionRemote}: {class: classReconnect, surfacesCloseError: true, countsTowardBackpressure: true},
	{1011, directionRemote}: {class: classReconnect, surfacesCloseError: true},
	{4000, directionRemote}: {class: classTerminal, surfacesCloseError: true},

	{CloseCodeHeartbeatTimeout, directionLocal}: {class: classReconnect, surfacesCloseError: true},
}

// lookupClosePolicy resolves a close to its outcome, reporting whether the code
// and direction are enumerated at all.
func lookupClosePolicy(key closeKey) (closeOutcome, bool) {
	out, ok := closeBaseRows[struct {
		code      int
		direction closeDirection
	}{key.code, key.direction}]
	if !ok {
		return closeOutcome{}, false
	}
	return applyReconnectFlag(out, key.reconnectEnabled), true
}

// applyReconnectFlag downgrades a reconnect-class outcome to a clean-stop when
// the caller disabled reconnect. Clean-stop and terminal rows are unaffected:
// they already stop, and the flag only ever removes a retry.
func applyReconnectFlag(out closeOutcome, reconnectEnabled bool) closeOutcome {
	if !reconnectEnabled && out.class == classReconnect {
		out.class = classCleanStop
	}
	return out
}

// handshakeOutcome is what a failed WebSocket upgrade resolves to. Handshake
// failures need their own table because they carry an HTTP status and a body
// code and no close code at all, so the closePolicy key cannot express them.
type handshakeOutcome struct {
	class terminationClass
	// retryAfterOverrides marks the one row where a non-nil Retry-After replaces
	// the computed backoff delay for that attempt: a tenant connection cap, where
	// the server knows better than the client's exponential guess.
	retryAfterOverrides bool
	// surfacesEditionError is always false. It exists so the "no WS edition
	// gate" decision is a property of the table rather than a comment: the
	// gateway's edition middleware covers four routes and /ws is not one of
	// them, so a WS upgrade cannot return an edition refusal.
	surfacesEditionError bool
}

// lookupHandshakePolicy classifies a failed upgrade. It is total by
// construction — unlisted statuses fall back by class (4xx terminal, 5xx
// reconnect), so no upgrade outcome is ever unclassified.
func lookupHandshakePolicy(status int, bodyCode string, reconnectEnabled bool) handshakeOutcome {
	var out handshakeOutcome
	switch {
	case status == 429 && bodyCode == "TENANT_LIMIT_EXCEEDED":
		out = handshakeOutcome{class: classReconnect, retryAfterOverrides: true}
	case status >= 500:
		// Server-side and probably transient, whatever the specific code.
		out = handshakeOutcome{class: classReconnect}
	case status >= 400:
		// 401 and 403 are the named rows and both are terminal; every other 4xx
		// falls back to the same class, since a client-side refusal does not
		// become acceptable by retrying it.
		out = handshakeOutcome{class: classTerminal}
	default:
		// Not a failure classification the caller should have reached, but the
		// lookup stays total rather than returning a zero value that reads as
		// "reconnect".
		out = handshakeOutcome{class: classTerminal}
	}

	if !reconnectEnabled && out.class == classReconnect {
		out.class = classCleanStop
	}
	return out
}

// internalCause names an epoch failure that carries no wire close code. These
// are enumerated so the termination tables stay exhaustive: a cause that
// terminated the client while absent from the enumeration would falsify the
// claim that these tables cover every ending.
type internalCause int

const (
	// causeRecoveredPanic is a panic caught by a goroutine's recover helper.
	// Treated as a disconnect — tear the epoch down and reconnect — so an
	// SDK-internal fault degrades like a network fault rather than killing the
	// caller's process.
	causeRecoveredPanic internalCause = iota
	// causeTokenSourceExhausted is MaxTokenSourceAttempts consecutive
	// TokenSource failures. Terminal: reconnecting would consume a handshake
	// with a credential the SDK already knows it cannot refresh.
	causeTokenSourceExhausted
	// causeConsumerTooSlow is MaxBackpressureReconnects consecutive
	// back-pressure-induced cycles. Terminal, and reported as such, so the
	// caller learns the cause instead of watching a silent reconnect loop.
	causeConsumerTooSlow
)

// String makes the cause readable in test failures and log lines.
func (c internalCause) String() string {
	switch c {
	case causeRecoveredPanic:
		return "recovered-panic"
	case causeTokenSourceExhausted:
		return "token-source-exhausted"
	case causeConsumerTooSlow:
		return "consumer-too-slow"
	default:
		return "unknown"
	}
}

// internalCauseRows classifies the code-less endings with reconnect enabled.
var internalCauseRows = map[internalCause]closeOutcome{
	causeRecoveredPanic:       {class: classReconnect},
	causeTokenSourceExhausted: {class: classTerminal},
	causeConsumerTooSlow:      {class: classTerminal},
}

// lookupInternalCausePolicy resolves an internal epoch failure to its outcome.
func lookupInternalCausePolicy(cause internalCause, reconnectEnabled bool) (closeOutcome, bool) {
	out, ok := internalCauseRows[cause]
	if !ok {
		return closeOutcome{}, false
	}
	return applyReconnectFlag(out, reconnectEnabled), true
}

// trigger is an event that can move the client between states. Triggers are
// deliberately coarse — they name what happened to the connection, not which
// close code caused it, because the close tables above have already reduced
// every ending to a class.
type trigger int

const (
	// triggerConnectCalled is the caller invoking Connect.
	triggerConnectCalled trigger = iota
	// triggerCloseCalled is the caller invoking Close, or the client-lifetime
	// context being canceled.
	triggerCloseCalled
	// triggerHandshakeOK is a completed handshake and initial auth.
	triggerHandshakeOK
	// triggerReconnectClassFailure is any ending classified classReconnect.
	triggerReconnectClassFailure
	// triggerTerminalFailure is any ending classified classTerminal, including
	// giving up while reconnecting.
	triggerTerminalFailure
	// triggerDialAborted is a dial cut short by the caller's own context — the
	// Connect(ctx) deadline expiring or the caller canceling mid-dial. It is
	// classified exactly like any other non-terminal dial failure; without it a
	// client whose Connect deadline expired parked in connecting forever with no
	// dial in flight, no *Terminal, and an open channel.
	triggerDialAborted
)

// String makes the trigger readable in test failures and log lines.
func (t trigger) String() string {
	switch t {
	case triggerConnectCalled:
		return "connect-called"
	case triggerCloseCalled:
		return "close-called"
	case triggerHandshakeOK:
		return "handshake-ok"
	case triggerReconnectClassFailure:
		return "reconnect-class-failure"
	case triggerTerminalFailure:
		return "terminal-failure"
	case triggerDialAborted:
		return "dial-aborted"
	default:
		return "unknown"
	}
}

// allTriggers returns every trigger, for exhaustiveness checks over the state
// machine.
func allTriggers() []trigger {
	return []trigger{
		triggerConnectCalled,
		triggerCloseCalled,
		triggerHandshakeOK,
		triggerReconnectClassFailure,
		triggerTerminalFailure,
		triggerDialAborted,
	}
}

// stateKey identifies a row of the state machine.
type stateKey struct {
	from ConnectionState
	on   trigger
}

// stateTransitions transcribes the entry/exit table: every state has a defined
// entry and exit, and every stop lands in one of the two resting states
// (StateClosed for a clean stop, StateError for a terminal failure), so no
// transition targets a value outside the enum.
//
// The resting states appear only as destinations. Nothing transitions out of
// them, which is what makes them resting — a client cannot resurrect after its
// channel has been closed.
var stateTransitions = map[stateKey]ConnectionState{
	// disconnected is the initial state after NewClient and is never re-entered.
	{StateDisconnected, triggerConnectCalled}: StateConnecting,
	{StateDisconnected, triggerCloseCalled}:   StateClosed,

	{StateConnecting, triggerHandshakeOK}:           StateConnected,
	{StateConnecting, triggerTerminalFailure}:       StateError,
	{StateConnecting, triggerReconnectClassFailure}: StateReconnecting,
	{StateConnecting, triggerDialAborted}:           StateReconnecting,
	{StateConnecting, triggerCloseCalled}:           StateClosed,

	{StateConnected, triggerReconnectClassFailure}: StateReconnecting,
	{StateConnected, triggerTerminalFailure}:       StateError,
	{StateConnected, triggerCloseCalled}:           StateClosed,

	{StateReconnecting, triggerHandshakeOK}:     StateConnected,
	{StateReconnecting, triggerTerminalFailure}: StateError,
	{StateReconnecting, triggerCloseCalled}:     StateClosed,
}

// lookupStateTransition resolves a transition with reconnect enabled.
func lookupStateTransition(from ConnectionState, on trigger) (ConnectionState, bool) {
	return lookupStateTransitionWithReconnect(from, on, true)
}

// lookupStateTransitionWithReconnect resolves a transition, applying the
// reconnect flag. With reconnect disabled a reconnect-class failure is a
// clean-stop, exactly as the close tables classify it — the rule lives in both
// places because the state machine and the termination tables must agree, and
// deriving it here keeps them from disagreeing.
func lookupStateTransitionWithReconnect(from ConnectionState, on trigger, reconnectEnabled bool) (ConnectionState, bool) {
	to, ok := stateTransitions[stateKey{from, on}]
	if !ok {
		return "", false
	}
	if !reconnectEnabled && (on == triggerReconnectClassFailure || on == triggerDialAborted) {
		return StateClosed, true
	}
	return to, ok
}
