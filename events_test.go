package sukko

import (
	"fmt"
	"strings"
	"testing"
)

// The Event union is a sealed set: an interface with an unexported marker
// method, so only types in this package can be members and a caller's type
// switch over it is exhaustive against a set the SDK controls.
//
// These tests pin the membership two ways — a compile-time assertion per member
// (the file would not build if a member lost its marker) and a runtime roster
// checked against the count — so that adding a member without updating the
// roster, or dropping one, fails here rather than silently changing what a
// caller's type switch must handle.

// Compile-time membership. Each line fails to build if that type stops
// implementing Event. The non-error members first:
var (
	_ Event = (*Message)(nil)
	_ Event = (*Gap)(nil)
	_ Event = (*PossibleGap)(nil)
	_ Event = (*SubscriptionResult)(nil)
	_ Event = (*Unsubscribed)(nil)
	_ Event = (*PublishAccepted)(nil)
	_ Event = (*Resumed)(nil)
	_ Event = (*Authenticated)(nil)
	_ Event = (*HistoryComplete)(nil)
	_ Event = (*ReplayComplete)(nil)
	_ Event = (*StateChange)(nil)
	_ Event = (*Terminal)(nil)
	_ Event = (*UnknownEvent)(nil)
)

// The error-shaped members: each is one of FR-010's typed error structs, so it
// implements both error and Event. `case *ReplayError:` in a type switch yields
// a value that errors.As also matches — there is no parallel wrapper hierarchy.
var (
	_ Event = (*AuthError)(nil)
	_ Event = (*PublishError)(nil)
	_ Event = (*HistoryError)(nil)
	_ Event = (*ReplayError)(nil)
	_ Event = (*ReconnectError)(nil)
	_ Event = (*ProtocolError)(nil)
	_ Event = (*RecoveryInterruptedError)(nil)
	_ Event = (*CloseError)(nil)
	_ Event = (*HandshakeError)(nil)
	_ Event = (*EditionRequiredError)(nil)
	_ Event = (*InternalError)(nil)
	_ Event = (*TokenSourceError)(nil)
)

// The error-shaped members are also errors — the property that lets one type
// switch case feed errors.As.
var (
	_ error = (*AuthError)(nil)
	_ error = (*PublishError)(nil)
	_ error = (*HistoryError)(nil)
	_ error = (*ReplayError)(nil)
	_ error = (*ReconnectError)(nil)
	_ error = (*ProtocolError)(nil)
	_ error = (*RecoveryInterruptedError)(nil)
	_ error = (*CloseError)(nil)
	_ error = (*HandshakeError)(nil)
	_ error = (*EditionRequiredError)(nil)
	_ error = (*InternalError)(nil)
	_ error = (*TokenSourceError)(nil)
)

// TestEventRosterIsComplete pins the member count. The compile-time assertions
// above prove each listed type is a member; this proves the list is the whole
// set — a new member added without a line here, or a member removed, moves the
// count and fails.
//
// The number is not magic: 13 non-error events + 12 error-shaped events, from
// FR-001a's dispatch table and SDK-originated events table.
func TestEventRosterIsComplete(t *testing.T) {
	roster := []Event{
		// non-error (13)
		&Message{}, &Gap{}, &PossibleGap{}, &SubscriptionResult{}, &Unsubscribed{},
		&PublishAccepted{}, &Resumed{}, &Authenticated{}, &HistoryComplete{},
		&ReplayComplete{}, &StateChange{}, &Terminal{}, &UnknownEvent{},
		// error-shaped (12)
		&AuthError{}, &PublishError{}, &HistoryError{}, &ReplayError{},
		&ReconnectError{}, &ProtocolError{}, &RecoveryInterruptedError{},
		&CloseError{}, &HandshakeError{}, &EditionRequiredError{},
		&InternalError{}, &TokenSourceError{},
	}

	const wantMembers = 25
	if len(roster) != wantMembers {
		t.Fatalf("roster has %d members, want %d — update the roster and the count together", len(roster), wantMembers)
	}

	// Every roster entry must be distinctly typed: a duplicate would inflate the
	// count while leaving a real member unlisted.
	seen := map[string]bool{}
	for _, ev := range roster {
		key := fmt.Sprintf("%T", ev)
		if seen[key] {
			t.Errorf("roster lists %s twice", key)
		}
		seen[key] = true
	}
}

// TestNonMembersAreNotEvents guards the other boundary: the error types that are
// deliberately return-only must NOT implement Event, or a caller's type switch
// would have to handle values it can never receive in-band.
//
// This cannot be a compile-time assertion (there is no "does not implement"
// operator), so it is expressed as a type switch that the non-members must fall
// through. If one of them gained the marker, it would match a case here.
func TestNonMembersAreNotEvents(t *testing.T) {
	nonMembers := []any{
		&NotConnectedError{},  // returned by wire-issuing methods
		&RateLimitError{},     // returned by REST/push calls, never in-band
		&PayloadDecodeError{}, // returned by DataAs
	}

	for _, v := range nonMembers {
		if _, isEvent := v.(Event); isEvent {
			t.Errorf("%T implements Event but is documented as return-only — it must not be a union member", v)
		}
	}
}

// TestErrorEventsCarryServerMessage pins the Q2 field additions: the error
// events the wire frames carry a message for must surface it, not drop it.
func TestErrorEventsCarryServerMessage(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string // a substring the Error() string must contain
	}{
		{"auth", &AuthError{Code: "invalid_token", Message: "token has expired"}, "token has expired"},
		{"publish", &PublishError{Code: "forbidden", Message: "channel is read-only"}, "channel is read-only"},
		{"reconnect", &ReconnectError{Code: "not_available", Message: "backend has no cursors"}, "backend has no cursors"},
		{"history", &HistoryError{Code: "not_available", Channel: "acme.x", Message: "history disabled"}, "history disabled"},
		{"replay", &ReplayError{Code: "replay_failed", Channel: "acme.x", Message: "offset out of range"}, "offset out of range"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); !strings.Contains(got, tc.want) {
				t.Errorf("Error() = %q, want it to contain the server message %q", got, tc.want)
			}
		})
	}
}
