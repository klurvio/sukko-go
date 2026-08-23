package sukko

import (
	"encoding/json"
	"errors"
	"fmt"
)

// DataAs decodes a message's raw payload into a caller type.
//
// The envelope keeps Data as json.RawMessage so the hot path never forces a
// decode; DataAs is the opt-in convenience for callers who want their own struct
// back. A decode failure surfaces as a *PayloadDecodeError carrying the channel
// and wrapping the cause — the public contract is the typed error, never a bare
// encoding/json error, so a caller matches on one SDK type while the underlying
// detail stays reachable through errors.Unwrap.
//
// It is a package-level generic rather than a method because a method cannot
// introduce its own type parameter; committing the symbol to the public surface
// is accepted and semver-frozen from v0.1.0.
func DataAs[T any](msg *Message) (T, error) {
	var out T

	// A nil message or an absent payload is a caller-visible error, not a
	// panic and not a silent zero value that would read as a successful decode.
	if msg == nil {
		return out, &PayloadDecodeError{
			Cause: errors.New("message is nil"),
		}
	}
	if len(msg.Data) == 0 {
		return out, &PayloadDecodeError{
			Channel: msg.Channel,
			Cause:   errors.New("message carries no payload"),
		}
	}

	if err := json.Unmarshal(msg.Data, &out); err != nil {
		// json.Unmarshal fills fields left to right and can populate several
		// before hitting a type error, so `out` may be half-decoded here.
		// Return a clean zero instead: a caller that logs the error and reads
		// the value anyway must not find a struct that is part real data, part
		// zero and looks valid.
		var zero T
		return zero, &PayloadDecodeError{
			Channel: msg.Channel,
			Cause:   fmt.Errorf("decoding into %T: %w", zero, err),
		}
	}
	return out, nil
}
