package sukko

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestDataAsRoundTrip decodes a well-formed payload into a caller type.
func TestDataAsRoundTrip(t *testing.T) {
	type order struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}
	msg := &Message{Channel: "acme.orders", Data: json.RawMessage(`{"id":"o-1","amount":42}`)}

	got, err := DataAs[order](msg)
	if err != nil {
		t.Fatalf("DataAs: %v", err)
	}
	if got.ID != "o-1" || got.Amount != 42 {
		t.Errorf("got %+v, want {o-1 42}", got)
	}
}

// TestDataAsSchemaMismatchIsTyped is the contract: a payload that will not decode
// into the caller's type yields a *PayloadDecodeError carrying the channel and
// wrapping the cause — never a bare encoding/json error leaking through the
// public surface.
func TestDataAsSchemaMismatchIsTyped(t *testing.T) {
	type order struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}
	// id decodes fine and is populated first; amount is a string where an int
	// is expected and fails second — so a naive return would hand back a struct
	// with ID set and Amount zero.
	msg := &Message{Channel: "acme.orders", Data: json.RawMessage(`{"id":"o-1","amount":"not-a-number"}`)}

	got, err := DataAs[order](msg)
	if err == nil {
		t.Fatal("schema mismatch decoded without error")
	}
	// The error path returns a clean zero, not the half-decoded value.
	if got != (order{}) {
		t.Errorf("error path returned a partially-populated value %+v, want the zero value", got)
	}

	var pde *PayloadDecodeError
	if !errors.As(err, &pde) {
		t.Fatalf("got %T, want *PayloadDecodeError", err)
	}
	if pde.Channel != "acme.orders" {
		t.Errorf("channel = %q, want acme.orders", pde.Channel)
	}
	// The underlying decode failure stays reachable via Unwrap, so a caller can
	// still inspect it — the public type is the wrapper, the detail is not lost.
	if errors.Unwrap(err) == nil {
		t.Error("underlying decode error was not wrapped")
	}
}

// TestDataAsNilMessage guards the nil-receiver edge: DataAs on a nil *Message is
// a caller mistake, and it must return a typed error rather than panic.
func TestDataAsNilMessage(t *testing.T) {
	_, err := DataAs[struct{}](nil)
	if err == nil {
		t.Fatal("DataAs(nil) returned no error")
	}
	var pde *PayloadDecodeError
	if !errors.As(err, &pde) {
		t.Errorf("got %T, want *PayloadDecodeError", err)
	}
}

// TestDataAsEmptyPayload covers an absent payload: a *Message whose Data is nil
// or empty cannot decode into a non-empty type and must report that as the typed
// error, not as a zero value that looks like a successful decode.
func TestDataAsEmptyPayload(t *testing.T) {
	msg := &Message{Channel: "acme.x", Data: nil}
	_, err := DataAs[map[string]any](msg)
	if err == nil {
		t.Fatal("empty payload decoded without error")
	}
	var pde *PayloadDecodeError
	if !errors.As(err, &pde) {
		t.Errorf("got %T, want *PayloadDecodeError", err)
	}
}
