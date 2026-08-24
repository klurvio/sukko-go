package sukko

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// This file is T084: the termination matrix, driven from the same policy tables
// the supervisor classifies against (policy.go). Each row asserts the observable
// outcome of one ending — a re-dial (reconnect-class), a terminal stop (Err
// non-nil, one dial), or, under WithReconnect(false), a clean stop (Err nil).
// Because the rows enumerate the table, a row that stops matching its policy row
// fails here.

type terminationClassExpectation int

const (
	expectReconnect terminationClassExpectation = iota
	expectTerminal
)

// assertReconnects connects, drives the backoff, and asserts a second dial
// happened with the client still live (Err nil), then closes.
func assertReconnects(t *testing.T, c *Client, fc *fakeClock, f *fakeWS) {
	t.Helper()
	_ = c.Connect(context.Background()) // may return a handshake error; reconnect proceeds in the background
	fc.BlockUntilTimer(purposeBackoff)
	fc.Advance(DefaultBackoff.Initial)
	waitForDialCount(t, f, 2)
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil while reconnecting", err)
	}
	closeClient(t, c)
}

// assertTerminal connects and asserts the client stopped on a failure: Messages()
// closed, Err() non-nil, exactly one dial, resting in StateError.
func assertTerminal(t *testing.T, c *Client, f *fakeWS, ec *eventCollector) {
	t.Helper()
	_ = c.Connect(context.Background())
	ec.waitClosed(t)
	if err := c.Err(); err == nil {
		t.Error("Err() = nil, want a terminal failure")
	}
	if got := f.dialCount(); got != 1 {
		t.Errorf("dialCount = %d, want 1 — a terminal outcome must not re-dial", got)
	}
	if got := c.State(); got != StateError {
		t.Errorf("State() = %v, want error", got)
	}
}

// assertCleanStop connects and asserts the WithReconnect(false) downgrade: closed,
// Err() nil, one dial, resting in StateClosed.
func assertCleanStop(t *testing.T, c *Client, f *fakeWS, ec *eventCollector) {
	t.Helper()
	_ = c.Connect(context.Background())
	ec.waitClosed(t)
	if err := c.Err(); err != nil {
		t.Errorf("Err() = %v, want nil — WithReconnect(false) makes a reconnect-class outcome a clean stop", err)
	}
	if got := f.dialCount(); got != 1 {
		t.Errorf("dialCount = %d, want 1 — reconnect disabled must not re-dial", got)
	}
	if got := c.State(); got != StateClosed {
		t.Errorf("State() = %v, want closed", got)
	}
	// The second-cause-field contract: Err() is nil, but the final *Terminal
	// still carries the downgraded cause for diagnosis.
	if term := drainLastTerminal(ec); term == nil {
		t.Error("no *Terminal before close")
	} else if term.Err == nil {
		t.Error("Terminal.Err = nil, want the downgraded cause for diagnosis")
	}
}

func closeClient(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestTerminationMatrixCloseCodes drives every close-policy row through the fake
// and asserts its class. Reconnect-class rows are re-run under WithReconnect(false)
// and must become clean stops.
func TestTerminationMatrixCloseCodes(t *testing.T) {
	// scriptFor builds the fake's ending for a row: a coded remote close, or an
	// abnormal drop for the synthesized {1006, local}.
	tests := []struct {
		name   string
		script epochScript
		expect terminationClassExpectation
	}{
		{"1000 remote", epochScript{closeAfter: websocket.StatusCode(1000)}, expectReconnect},
		{"1001 remote", epochScript{closeAfter: websocket.StatusCode(1001)}, expectReconnect},
		{"1006 local (abnormal drop)", epochScript{dropRaw: true}, expectReconnect},
		{"1008 remote", epochScript{closeAfter: websocket.StatusCode(1008)}, expectReconnect},
		{"1011 remote", epochScript{closeAfter: websocket.StatusCode(1011)}, expectReconnect},
		{"4000 remote force-disconnect", epochScript{closeAfter: websocket.StatusCode(4000)}, expectTerminal},
		{"unlisted 3000 remote", epochScript{closeAfter: websocket.StatusCode(3000)}, expectTerminal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeWS(t)
			f.script(tc.script)
			c, fc := newClockedClient(t, f, WithRand(newFakeRand()))
			ec := collectEvents(c)
			if tc.expect == expectReconnect {
				assertReconnects(t, c, fc, f)
			} else {
				assertTerminal(t, c, f, ec)
			}
		})

		if tc.expect != expectReconnect {
			continue
		}
		t.Run(tc.name+" (reconnect disabled → clean stop)", func(t *testing.T) {
			f := newFakeWS(t)
			f.script(tc.script)
			c, _ := newClockedClient(t, f, WithRand(newFakeRand()), WithReconnect(false))
			ec := collectEvents(c)
			assertCleanStop(t, c, f, ec)
		})
	}
}

// TestTerminationMatrixHandshake drives the handshake-policy rows: named terminal
// statuses, the 429 tenant-limit and 5xx reconnect rows, and the class fallbacks
// (unlisted 4xx terminal, unlisted 5xx reconnect).
func TestTerminationMatrixHandshake(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		expect terminationClassExpectation
	}{
		{"401 unauthorized", 401, `{"code":"UNAUTHORIZED"}`, expectTerminal},
		{"403 forbidden", 403, `{"code":"FORBIDDEN"}`, expectTerminal},
		{"404 unlisted 4xx", 404, `{"code":"NOT_FOUND"}`, expectTerminal},
		{"429 tenant limit", 429, `{"code":"TENANT_LIMIT_EXCEEDED"}`, expectReconnect},
		{"500 server error", 500, `{"code":"INTERNAL"}`, expectReconnect},
		{"503 unlisted 5xx", 503, `{"code":"UNAVAILABLE"}`, expectReconnect},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeWS(t)
			f.rejectUpgrade(tc.status, tc.body)
			c, fc := newClockedClient(t, f, WithRand(newFakeRand()))
			ec := collectEvents(c)
			if tc.expect == expectReconnect {
				assertReconnects(t, c, fc, f)
			} else {
				assertTerminal(t, c, f, ec)
			}
		})

		if tc.expect != expectReconnect {
			continue
		}
		t.Run(tc.name+" (reconnect disabled → clean stop)", func(t *testing.T) {
			f := newFakeWS(t)
			f.rejectUpgrade(tc.status, tc.body)
			c, _ := newClockedClient(t, f, WithRand(newFakeRand()), WithReconnect(false))
			ec := collectEvents(c)
			assertCleanStop(t, c, f, ec)
		})
	}
}
