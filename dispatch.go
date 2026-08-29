package sukko

import (
	"errors"
	"fmt"
)

// errNotSurfaceFrame is returned by surfaceEvent for a frame whose disposition
// is not dispSurface — a client-derived or internal-silent control frame that
// the decode loop handles by its side effect, not by surfacing an event. The
// decode loop distinguishes it from a presence/protocol error (which IS
// surfaced) with errors.Is.
var errNotSurfaceFrame = errors.New("sukko: frame is not a surface-disposition frame")

// The wire→event mapping: FR-001a's dispatch table in code.
//
// Every server frame the SDK decodes reaches the caller in one of three ways,
// and the disposition table names which. The mapping here handles the stateless
// rows — the frame carries everything the public event needs. The two rows that
// need SDK state (a grant diff needs the requested set; an auth mode is the
// SDK's own knowledge) and the one that surfaces nothing (pong) are marked and
// left to the client, so the decode goroutine of Phase 5 has one place to learn
// each frame's fate.

// eventDisposition classifies how a decoded frame reaches the caller.
type eventDisposition int

const (
	// dispSurface: surfaceEvent produces the public Event directly from the
	// frame, with no SDK state. Covers surfaced data and the stateless derived
	// events.
	dispSurface eventDisposition = iota
	// dispClientDerived: internal-only, and the derived event needs SDK state
	// the decode layer does not hold — the requested set for subscription_ack's
	// grant diff, the owned auth mode for auth_ack. The client completes these.
	dispClientDerived
	// dispInternalSilent: internal-only and surfaces nothing at all. Only pong:
	// its whole effect is resetting the pong deadline.
	dispInternalSilent
)

func (d eventDisposition) String() string {
	switch d {
	case dispSurface:
		return "surface"
	case dispClientDerived:
		return "client-derived"
	case dispInternalSilent:
		return "internal-silent"
	default:
		return fmt.Sprintf("eventDisposition(%d)", int(d))
	}
}

// dispositionTable places every one of the 18 decodable wire types. It is the
// referent for the completeness test: a decodable type with no entry cannot
// reach the caller, and an entry with no decodable type is a stale row.
var dispositionTable = map[string]eventDisposition{
	// Surfaced data and stateless derived events.
	typeMessage:           dispSurface,
	typeReplayMessage:     dispSurface,
	typeGap:               dispSurface,
	typeUnsubscriptionAck: dispSurface,
	typePublishAck:        dispSurface,
	typePublishError:      dispSurface,
	typeReconnectAck:      dispSurface,
	typeReconnectError:    dispSurface,
	typeAuthError:         dispSurface,
	typeHistoryComplete:   dispSurface,
	typeReplayComplete:    dispSurface,
	typeHistoryError:      dispSurface,
	typeSubscribeError:    dispSurface,
	typeUnsubscribeError:  dispSurface,
	typeError:             dispSurface,

	// Need SDK state — the client completes these.
	typeSubscriptionAck: dispClientDerived,
	typeAuthAck:         dispClientDerived,

	// Surfaces nothing.
	typePong: dispInternalSilent,
}

// surfaceEvent maps a decoded wire struct to the public Event it surfaces.
//
// It handles the dispSurface rows only; a dispClientDerived or dispInternalSilent
// frame reaching here is a caller bug, reported as an error rather than silently
// producing nothing. Presence validation (T024a, §II) rejects a frame whose
// channel is empty where the contract requires it and empty is meaningless — the
// only presence check decidable after decode, since a missing string and an
// empty string are then indistinguishable. gap's empty last_pos is deliberately
// not rejected: FR-006 treats it as a valid un-anchorable gap.
//
// Phase-5 constraint: several dispSurface frames are also single-flight
// terminators (history_complete/history_error end a history, replay_complete
// releases a replay slot). When this returns a *ProtocolError for such a frame,
// the client MUST still apply the frame's side effect — keyed on the wire type,
// not on this succeeding — or a malformed terminator would wedge the
// single-flight it was meant to end.
func surfaceEvent(decoded any) (Event, error) {
	switch f := decoded.(type) {
	case *wireMessage:
		if err := requireChannel(typeMessage, f.Channel); err != nil {
			return nil, err
		}
		return &Message{
			Channel: f.Channel,
			Seq:     f.Seq,
			TS:      f.TS,
			Pos:     f.Pos,
			Mid:     f.Mid,
			Data:    f.Data,
			// live, or history when the frame says so. The replay-window
			// override (SourceReplay for a live frame arriving inside the
			// reconnect window) is client state, applied in Phase 5.
			Source: sourceForMessage(f.History),
		}, nil

	case *wireReplayMessage:
		if err := requireChannel(typeReplayMessage, f.Channel); err != nil {
			return nil, err
		}
		return &Message{
			Channel: f.Channel,
			Seq:     f.Seq,
			TS:      f.TS,
			Pos:     f.Pos,
			Mid:     f.Mid,
			Data:    f.Data,
			Source:  SourceReplay,
		}, nil

	case *wireGap:
		if err := requireChannel(typeGap, f.Channel); err != nil {
			return nil, err
		}
		// LastPos may be empty; that is a valid un-anchorable gap, not a
		// malformed frame (FR-006).
		return &Gap{
			Channel: f.Channel,
			FromSeq: f.FromSeq,
			ToSeq:   f.ToSeq,
			LastPos: f.LastPos,
			TS:      f.TS,
		}, nil

	case *wireUnsubscriptionAck:
		return &Unsubscribed{Channels: f.Unsubscribed, Count: f.Count, Forced: f.Forced}, nil

	case *wirePublishAck:
		if err := requireChannel(typePublishAck, f.Channel); err != nil {
			return nil, err
		}
		return &PublishAccepted{Channel: f.Channel, Mid: f.Mid}, nil

	case *wirePublishError:
		return &PublishError{Code: f.Code, Message: f.Message}, nil

	case *wireReconnectAck:
		return &Resumed{MessagesReplayed: f.MessagesReplayed}, nil

	case *wireReconnectError:
		return &ReconnectError{Code: f.Code, Message: f.Message}, nil

	case *wireAuthError:
		return &AuthError{Code: f.Data.Code, Message: f.Data.Message}, nil

	case *wireHistoryComplete:
		if err := requireChannel(typeHistoryComplete, f.Channel); err != nil {
			return nil, err
		}
		return &HistoryComplete{
			Channel: f.Channel,
			Count:   f.Count,
			// Source is preserved verbatim, not validated against the known
			// set. An unknown value is a well-formed frame carrying a value
			// this SDK predates — the same forward-compatibility rule that
			// preserves an unknown error code rather than rejecting it (a newer
			// server must be able to talk to an older client). Rejecting here
			// would also discard a single-flight terminator and wedge history.
			// Callers switching on Source therefore need a default; the type's
			// doc says so.
			Source:    HistorySource(f.Source),
			Truncated: f.Truncated,
		}, nil

	case *wireReplayComplete:
		if err := requireChannel(typeReplayComplete, f.Channel); err != nil {
			return nil, err
		}
		return &ReplayComplete{Channel: f.Channel, MessagesReplayed: f.MessagesReplayed, Truncated: f.Truncated}, nil

	case *wireHistoryError:
		// history_error names exactly one channel (contract-required), and it
		// is the only argument the documented "call History for that channel"
		// fallback has — so an empty channel is unusable, the same presence
		// check its sibling terminators enforce.
		if err := requireChannel(typeHistoryError, f.Channel); err != nil {
			return nil, err
		}
		return &HistoryError{Code: f.Code, Channel: f.Channel, Message: f.Message}, nil

	case *wireSubscribeError:
		return &ProtocolError{Type: typeSubscribeError, Code: f.Code, Message: f.Message}, nil

	case *wireUnsubscribeError:
		return &ProtocolError{Type: typeUnsubscribeError, Code: f.Code, Message: f.Message}, nil

	case *wireError:
		// classifyErrorFrame returns *ReplayError (channel present) or
		// *ProtocolError (channel absent); both implement Event, so the
		// assertion states that invariant rather than guarding a case no input
		// can reach.
		//nolint:errcheck // total assertion: classifyErrorFrame returns only *ReplayError or *ProtocolError, both of which implement Event; the two-value form would return nil on an impossible miss, hiding the bug the panic surfaces.
		return classifyErrorFrame(f).(Event), nil

	default:
		return nil, fmt.Errorf("%w: %T", errNotSurfaceFrame, decoded)
	}
}

// sourceForMessage resolves a message frame's history flag to its Source. The
// replay case is never reached from a `message` frame — a replayed record is a
// `replay_message`, or a live frame the client re-tags inside the reconnect
// window — so this maps only the two the frame itself can declare.
func sourceForMessage(history bool) MessageSource {
	if history {
		return SourceHistory
	}
	return SourceLive
}

// requireChannel is the §II presence check for the one field where empty is
// itself invalid regardless of whether it was absent or explicitly empty.
func requireChannel(frameType, channel string) error {
	if channel == "" {
		return &ProtocolError{
			Type:    frameType,
			Message: "required channel field is absent or empty",
		}
	}
	return nil
}
