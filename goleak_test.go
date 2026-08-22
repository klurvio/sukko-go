package sukko

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

// Goroutine-leak detection is the SDK's standing check that no goroutine
// outlives its epoch or its client. It is the counterpart to the race detector:
// -race proves the absence of data races, goleak proves the absence of
// abandoned goroutines, and the SDK's whole concurrency design — a supervisor
// owning per-epoch goroutine sets, torn down before every re-dial — is only
// worth stating if something checks it.
//
// The check is easy to disable by accident. An over-broad ignore rule silences
// real leaks while still reporting green, so the ignore list is deliberately
// minimal, lives in exactly one place, and is itself covered by a meta-test
// asserting the checker still fires.

// TestMain runs the package-level leak check after every test has finished.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, leakIgnores()...)
}

// leakIgnores is the single shared ignore list. Every entry names a goroutine
// the Go runtime or standard library owns, which the SDK cannot shut down and
// which is not a leak.
//
// Nothing from this package may ever appear here: ignoring an SDK goroutine
// would silence exactly the defect the check exists to catch. If a sukko
// goroutine shows up as leaked, the fix is in the SDK, not in this list.
func leakIgnores() []goleak.Option {
	return []goleak.Option{
		// net/http keeps pooled connections warm. A test that used an
		// httptest server can leave these parked briefly after the server
		// closes; they belong to the transport, not to the SDK.
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		// The runtime's network poller, parked for the process lifetime.
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	}
}

// A per-test verifyNoLeaks helper belongs here too, so a leak is attributed to
// the test that caused it rather than surfacing later as a package-level
// failure with no obvious owner. It lands with the first lifecycle test that
// calls it — defining it now would ship an unused symbol, and its teardown
// contract (close the client, then CloseIdleConnections, then the httptest
// server, and only then verify) is easier to state correctly against a real
// caller than in the abstract.
//
// Note that any such helper must run in a NON-parallel test, for the reason the
// meta-test below documents.

// ─── meta-test ───

// TestLeakCheckerActuallyFires is the check on the check.
//
// An ignore list that grew one entry too broad — or a helper that silently
// stopped calling goleak — would leave every lifecycle test reporting green
// while detecting nothing. So: deliberately leak a goroutine, run the same
// verification lifecycle tests use, and assert it is *reported*.
//
// goleak.VerifyNone takes a TestingT, so a recording stub captures the failure
// instead of failing this test. The leaked goroutine is released before
// returning, or it would trip the package-level check in TestMain.
//
// This test deliberately does NOT call t.Parallel(). VerifyNone inspects every
// goroutine in the process, so a parallel sibling's own goroutines would read
// as leaks — the check would pass for the wrong reason on the way in, and never
// come clean on the way out. Any test that calls VerifyNone must be sequential.
func TestLeakCheckerActuallyFires(t *testing.T) {
	release := make(chan struct{})
	leaked := make(chan struct{})
	go func() {
		close(leaked)
		<-release // parked: this is the "leak"
	}()
	<-leaked // ensure it is running before verifying

	var rec recordingT
	goleak.VerifyNone(&rec, leakIgnores()...)

	if !rec.failed {
		t.Error("goleak did not report a deliberately leaked goroutine — " +
			"the ignore list or the helper has disabled the check")
	}

	// Release it and confirm it is gone, so this test leaves nothing behind.
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		var after recordingT
		goleak.VerifyNone(&after, leakIgnores()...)
		if !after.failed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the deliberately leaked goroutine did not exit after release")
		}
		time.Sleep(time.Millisecond)
	}
}

// recordingT captures a goleak failure instead of propagating it, so the
// meta-test can assert that a failure *happened*.
type recordingT struct {
	failed bool
}

func (r *recordingT) Error(...any)          { r.failed = true }
func (r *recordingT) Errorf(string, ...any) { r.failed = true }

// The helper must satisfy goleak's own interface — a compile-time check, so a
// signature drift in a future goleak release fails the build rather than
// silently bypassing the meta-test.
var _ goleak.TestingT = (*recordingT)(nil)
