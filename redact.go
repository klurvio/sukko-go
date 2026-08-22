package sukko

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

// Credential redaction for everything the SDK emits.
//
// A leaked credential cannot be recalled. Once it reaches a log aggregator it
// has been retained, indexed, and quite possibly forwarded, so the guarantee
// has to hold at every boundary rather than at the ones someone remembered.
// There are two of those boundaries — returned errors and log records — and
// both route through here.
//
// Two mechanisms, because either alone leaves a hole:
//
//   - Value masking removes the exact secrets this client holds, wherever they
//     appear. It is the one that catches the common accident: Go's url.Error
//     embeds the full request URL in its message, so a client using
//     query-parameter auth would otherwise leak its token through any transport
//     error, without the SDK ever formatting the credential itself.
//   - Pattern masking removes credential-shaped material the client never held
//     — a token in a redirect URL, a header echoed by a proxy, someone else's
//     key. Value masking cannot see these because it does not know them.

// redactedMarker replaces a masked value. It is deliberately visible: a reader
// needs to know a value was removed rather than absent, or they will chase a
// missing field that was never missing.
const redactedMarker = "[REDACTED]"

// minMaskableSecretLength is the shortest value masked by value.
//
// Below this, masking does more harm than good: a two-character secret appears
// inside ordinary words, and replacing every occurrence would shred unrelated
// messages while protecting a credential too short to be protecting anything.
// Pattern masking still covers such a value when it appears in a credential
// position.
const minMaskableSecretLength = 8

// credentialPatterns match credential-shaped material regardless of whether the
// SDK holds the value. Each captures the credential's introducer so it can be
// preserved, leaving the reader able to tell *what* was redacted.
var credentialPatterns = []*regexp.Regexp{
	// Query parameters: token, access_token, api_key, apikey, auth, key.
	regexp.MustCompile(`(?i)\b((?:access_)?token|api[-_]?key|auth|key)=([^&\s"'\\]+)`),
	// Header forms, including the Bearer scheme, which is kept so the line
	// still reads as an Authorization header.
	regexp.MustCompile(`(?i)\b(Authorization:\s*(?:Bearer|Basic)?\s*)([^\s"'\\]+)`),
	regexp.MustCompile(`(?i)\b(X-API-Key:\s*)([^\s"'\\]+)`),
	// Push subscriber keys, which authorize delivery to one browser endpoint.
	regexp.MustCompile(`(?i)\b(p256dh(?:_key)?|auth_secret)=([^&\s"'\\]+)`),
}

// redactor masks credentials in strings, errors and log records. The zero value
// is not usable; construct one with newRedactor.
type redactor struct {
	// secrets holds the exact values to mask, longest first so a secret that
	// contains another is masked whole rather than leaving a fragment behind.
	secrets []string
}

// newRedactor returns a redactor that masks the given values by exact match, in
// addition to the pattern rules that always apply. Empty and very short values
// are ignored — see minMaskableSecretLength.
func newRedactor(secrets ...string) *redactor {
	kept := make([]string, 0, len(secrets))
	for _, s := range secrets {
		if len(s) >= minMaskableSecretLength {
			kept = append(kept, s)
		}
	}
	// Longest first: if one secret is a substring of another, masking the
	// shorter one first would leave the remainder of the longer one exposed.
	for i := 1; i < len(kept); i++ {
		for j := i; j > 0 && len(kept[j]) > len(kept[j-1]); j-- {
			kept[j], kept[j-1] = kept[j-1], kept[j]
		}
	}
	return &redactor{secrets: kept}
}

// redact masks every known secret and every credential-shaped pattern in s,
// leaving the surrounding text intact. Context matters: an error stripped of
// everything but a marker is barely better than one that leaked, because the
// reader still cannot act on it.
func (r *redactor) redact(s string) string {
	if s == "" {
		return s
	}

	for _, secret := range r.secrets {
		s = strings.ReplaceAll(s, secret, redactedMarker)
	}
	for _, pattern := range credentialPatterns {
		// $1 keeps the introducer (the parameter name or header), so the reader
		// still knows which credential was removed.
		s = pattern.ReplaceAllString(s, "${1}"+redactedMarker)
	}
	return s
}

// redactError returns err with its message redacted, preserving the chain so
// errors.Is and errors.As keep working. Redaction that broke matching would
// trade one failure for another: the credential would be safe and the caller
// would have lost the ability to branch on the condition.
func (r *redactor) redactError(err error) error {
	if err == nil {
		return nil
	}
	redacted := r.redact(err.Error())
	if redacted == err.Error() {
		return err // nothing masked; keep the original allocation
	}
	return &redactedError{msg: redacted, cause: err}
}

// redactedError carries a masked message while still unwrapping to the original.
type redactedError struct {
	msg   string
	cause error
}

func (e *redactedError) Error() string { return e.msg }

// Unwrap keeps errors.Is and errors.As working through the redaction.
//
// The wrapped error's own message still contains the credential, which is safe
// only because nothing prints it: Error() returns the masked text, and callers
// reach the cause through errors.As to inspect its type, not its message.
func (e *redactedError) Unwrap() error { return e.cause }

// wrapHandler returns a slog.Handler that redacts messages and attribute values
// before passing them to base.
//
// Log attributes need this as much as messages do. A caller who logs a URL as a
// structured field would otherwise leak exactly what the message path protects,
// and structured logging makes that the more natural thing to write.
func (r *redactor) wrapHandler(base slog.Handler) slog.Handler {
	return &redactingHandler{base: base, redactor: r}
}

type redactingHandler struct {
	base     slog.Handler
	redactor *redactor
}

// Enabled defers to the base handler, so wrapping never changes what is logged
// — only what the output contains.
func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

// Handle redacts the message and every attribute, then hands the record on.
func (h *redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	clone := slog.NewRecord(
		record.Time,
		record.Level,
		h.redactor.redact(record.Message),
		record.PC,
	)
	record.Attrs(func(attr slog.Attr) bool {
		clone.AddAttrs(h.redactor.redactAttr(attr))
		return true
	})
	return h.base.Handle(ctx, clone)
}

// WithAttrs redacts the attributes as they are bound, so a secret attached once
// to a logger is masked on every record it produces.
func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i, attr := range attrs {
		redacted[i] = h.redactor.redactAttr(attr)
	}
	return &redactingHandler{base: h.base.WithAttrs(redacted), redactor: h.redactor}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{base: h.base.WithGroup(name), redactor: h.redactor}
}

// redactAttr masks an attribute's value, recursing into groups — a secret does
// not become safe by being nested one level deeper.
func (r *redactor) redactAttr(attr slog.Attr) slog.Attr {
	value := attr.Value.Resolve()

	switch value.Kind() {
	case slog.KindString:
		return slog.String(attr.Key, r.redact(value.String()))
	case slog.KindGroup:
		members := value.Group()
		redacted := make([]any, 0, len(members))
		for _, member := range members {
			redacted = append(redacted, r.redactAttr(member))
		}
		return slog.Group(attr.Key, redacted...)
	case slog.KindAny:
		// A stringer or an error can carry a credential in its rendering, so
		// format it and mask the result rather than trusting the type.
		return slog.String(attr.Key, r.redact(fmt.Sprint(value.Any())))
	default:
		// Numbers, booleans, times and durations cannot carry a credential.
		return attr
	}
}
