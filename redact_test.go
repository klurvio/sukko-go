package sukko

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// Redaction is asserted, never assumed.
//
// The failure mode it guards against is quiet and permanent: a credential
// reaches a log aggregator or an error report, and by the time anyone notices
// it has been retained, indexed, and possibly shipped to a third party. It
// cannot be un-leaked, so the check is mechanical rather than a review habit.
//
// Two mechanisms, because either alone leaves a hole. Value-based masking
// catches the exact secrets this client holds wherever they appear — including
// inside a URL some transport library embedded in an error string. Pattern
// masking catches secrets the client does not hold: a token in a redirect URL,
// a header echoed back by a proxy, a credential belonging to someone else.

// Synthetic credentials, shaped like the real thing on purpose: a redaction
// test that used "secret123" would not exercise the patterns that matter, and
// would pass while leaving a realistic token untouched. Neither value has ever
// been valid anywhere.
const (
	testJWT    = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"
	testAPIKey = "sk_live_9f8e7d6c5b4a3210"
)

func newTestRedactor() *redactor {
	return newRedactor(testJWT, testAPIKey)
}

// The registered secrets must be masked wherever they appear. The URL cases are
// the ones that matter most in practice: Go's own url.Error embeds the full
// request URL in its message, so a client using query-parameter auth leaks its
// token through any transport error unless something intervenes.
func TestRedactsRegisteredSecrets(t *testing.T) {
	t.Parallel()

	r := newTestRedactor()

	for _, tc := range []struct {
		name string
		give string
	}{
		{"bare token", testJWT},
		{"token in a sentence", "dial failed with token " + testJWT + " rejected"},
		{"token in a query string", "wss://gw.example.com/ws?token=" + testJWT},
		{"token in a url.Error", `Get "https://gw.example.com/api?token=` + testJWT + `": connection refused`},
		{"api key bare", testAPIKey},
		{"api key in a header dump", "X-API-Key: " + testAPIKey},
		{"both secrets", "token=" + testJWT + " api_key=" + testAPIKey},
		{"repeated occurrences", testJWT + " ... " + testJWT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := r.redact(tc.give)
			if strings.Contains(got, testJWT) {
				t.Errorf("the JWT survived redaction: %q", got)
			}
			if strings.Contains(got, testAPIKey) {
				t.Errorf("the API key survived redaction: %q", got)
			}
			if !strings.Contains(got, redactedMarker) {
				t.Errorf("redacted output %q carries no marker; a reader cannot tell masking occurred", got)
			}
		})
	}
}

// Pattern masking covers credentials the client never held. A token in a
// redirect URL or a header echoed by a proxy is still a credential, and still
// must not reach a log.
func TestRedactsUnregisteredCredentialsByPattern(t *testing.T) {
	t.Parallel()

	r := newRedactor() // holds no secrets at all

	for _, tc := range []struct {
		name   string
		give   string
		secret string
	}{
		{"token query param", "https://other.example.com/cb?token=someoneelsestoken", "someoneelsestoken"},
		{"api_key query param", "https://gw/api?api_key=leakedkey123", "leakedkey123"},
		{"access_token query param", "https://gw/api?access_token=bearertoken456", "bearertoken456"},
		{"authorization header", "Authorization: Bearer abcdef123456", "abcdef123456"},
		{"api key header", "X-API-Key: headerkey789", "headerkey789"},
		{"token amid other params", "https://gw/ws?tenant=acme&token=hidden999&v=1", "hidden999"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := r.redact(tc.give)
			if strings.Contains(got, tc.secret) {
				t.Errorf("secret %q survived pattern redaction: %q", tc.secret, got)
			}
		})
	}
}

// Redaction must not damage the surrounding message. An error stripped of its
// context is only marginally better than one that leaked — the reader still
// cannot act on it.
func TestRedactionPreservesContext(t *testing.T) {
	t.Parallel()

	r := newTestRedactor()
	got := r.redact("dial wss://gw.example.com/ws?token=" + testJWT + "&tenant=acme: connection refused")

	for _, want := range []string{"dial", "gw.example.com", "tenant=acme", "connection refused"} {
		if !strings.Contains(got, want) {
			t.Errorf("redaction removed %q from the message: %q", want, got)
		}
	}
}

// Push subscriber keys are credentials too: they authorize delivery to a
// specific browser endpoint, so leaking them lets anyone push to that user.
func TestRedactsPushSubscriberKeys(t *testing.T) {
	t.Parallel()

	// Web Push key material, in the real encoding. Synthetic, never valid.
	//
	const (
		p256dh = "BEl62iUYgUivxIkv69yViEuiBIa-Ib9-SkvMeAtA3LFgDzkrxZJjSgSnfckjBJuBkr3qBUYIHBQFLXYp5Nksh8U"
		secret = "tBHItJI5svbpez7KI4CCXg"
	)

	r := newRedactor(p256dh, secret)
	got := r.redact(fmt.Sprintf("push subscribe failed: p256dh_key=%s auth_secret=%s", p256dh, secret))

	if strings.Contains(got, p256dh) {
		t.Errorf("the p256dh key survived redaction: %q", got)
	}
	if strings.Contains(got, secret) {
		t.Errorf("the auth secret survived redaction: %q", got)
	}
}

// Empty registrations must not turn into a mask that matches everything. A
// client with no API key configured registers an empty string, and a naive
// strings.ReplaceAll of "" would insert the marker between every character.
func TestEmptySecretsAreIgnored(t *testing.T) {
	t.Parallel()

	r := newRedactor("", "", testJWT)
	got := r.redact("a normal message with no secrets")

	if got != "a normal message with no secrets" {
		t.Errorf("an empty registered secret corrupted the message: %q", got)
	}
}

// Very short secrets are not masked by value. Masking a two-character string
// would redact it out of every unrelated word, destroying messages wholesale —
// and a credential that short is not protecting anything.
func TestVeryShortSecretsAreNotMaskedByValue(t *testing.T) {
	t.Parallel()

	r := newRedactor("ab")
	got := r.redact("a fabulous message about absolutes")

	if got != "a fabulous message about absolutes" {
		t.Errorf("a two-character secret was masked by value and corrupted the message: %q", got)
	}
}

// redactError is the wrapper applied at every error boundary. The result must
// still be an error, still be matchable, and still unwrap.
func TestRedactErrorPreservesMatching(t *testing.T) {
	t.Parallel()

	r := newTestRedactor()
	inner := fmt.Errorf("%w: dialing with token=%s", ErrInsecureTransport, testJWT)
	got := r.redactError(inner)

	if got == nil {
		t.Fatal("redactError returned nil for a non-nil error")
	}
	if strings.Contains(got.Error(), testJWT) {
		t.Errorf("the token survived error redaction: %q", got)
	}
	if !errors.Is(got, ErrInsecureTransport) {
		t.Error("redaction broke errors.Is; a caller could no longer branch on the condition")
	}
}

func TestRedactErrorPassesNilThrough(t *testing.T) {
	t.Parallel()

	if got := newTestRedactor().redactError(nil); got != nil {
		t.Errorf("redactError(nil) = %v, want nil", got)
	}
}

// The log handler is the other boundary. A secret in a structured attribute
// must be masked exactly as one in a message is — a caller who logs a URL as a
// field would otherwise leak what the message path protects.
func TestRedactingLogHandlerMasksMessagesAndAttrs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(newTestRedactor().wrapHandler(base))

	logger.Info("dial failed",
		slog.String("url", "wss://gw.example.com/ws?token="+testJWT),
		slog.String("key", testAPIKey),
		slog.Group("nested", slog.String("inner", "api_key=nestedsecret123")),
	)

	out := buf.String()
	for _, secret := range []string{testJWT, testAPIKey, "nestedsecret123"} {
		if strings.Contains(out, secret) {
			t.Errorf("secret %q reached the log output: %s", secret, out)
		}
	}
	// The record must survive: redaction that dropped the log line would hide
	// the incident along with the credential.
	if !strings.Contains(out, "dial failed") {
		t.Errorf("the log message was lost: %s", out)
	}
}

// A group's attributes are rewritten too — a secret does not become safe by
// being one level deeper.
func TestRedactingHandlerMasksThroughWithAttrsAndGroups(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(newTestRedactor().wrapHandler(base)).
		With(slog.String("token", testJWT)).
		WithGroup("conn")

	logger.Info("connected", slog.String("key", testAPIKey))

	out := buf.String()
	if strings.Contains(out, testJWT) || strings.Contains(out, testAPIKey) {
		t.Errorf("a secret survived through With/WithGroup: %s", out)
	}
}

// The wrapped handler must respect the base handler's level, or wrapping would
// silently turn a quiet logger noisy.
func TestRedactingHandlerRespectsLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := newTestRedactor().wrapHandler(base)

	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("the wrapper reported Info enabled against a Warn-level handler")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Error("the wrapper reported Error disabled against a Warn-level handler")
	}
}
