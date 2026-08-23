package sukko

import (
	"errors"
	"fmt"
	"strings"
)

// Channels are "{tenant}.{suffix}": the tenant is the segment before the first
// dot, and the suffix is the opaque dotted remainder.
//
// The validation rules here come from the contract's channel-format section and
// the gateway's own validator. They are deliberately few — there is no charset
// restriction and no length limit in either source, so the SDK imposes none.
// Inventing a stricter rule would reject channels the platform accepts, and the
// caller would have no way to tell whose rule they had broken.
//
// `string` is the canonical type across the client surface: Subscribe, Publish,
// Message.Channel and the recovery cursor map all take and return strings.
// Channel is a parse/validation helper for callers who want the split view, not
// a type threaded through the API — that would force conversions on the hot
// path for safety the wire cannot honor anyway.

// ErrInvalidChannel reports a channel that does not satisfy the format. It is a
// sentinel so a caller can branch on "this channel is malformed" without
// matching on message text.
var ErrInvalidChannel = errors.New("sukko: invalid channel")

// Channel is the split view of a channel string.
type Channel struct {
	// Tenant is the segment before the first dot.
	Tenant string
	// Suffix is the remainder, which may itself contain dots.
	Suffix string
}

// String renders the channel in wire form. The zero Channel renders as the
// empty string rather than a bare dot, so an unset value cannot be mistaken for
// a real channel in a log line.
func (c Channel) String() string {
	if c.Tenant == "" && c.Suffix == "" {
		return ""
	}
	return c.Tenant + "." + c.Suffix
}

// ParseChannel splits a channel into its tenant and suffix.
//
// It rejects a channel that is empty, has fewer than MinChannelParts
// dot-separated parts, or contains an empty segment. An empty segment matters
// because it makes the channel ambiguous: ".a", "a." and "a..b" each render
// indistinguishably from a channel that does not have it.
func ParseChannel(s string) (Channel, error) {
	if s == "" {
		return Channel{}, fmt.Errorf("%w: channel is empty", ErrInvalidChannel)
	}

	parts := strings.Split(s, ".")
	if len(parts) < MinChannelParts {
		return Channel{}, fmt.Errorf(
			"%w: %q has %d dot-separated part(s), need at least %d (\"{tenant}.{suffix}\")",
			ErrInvalidChannel, s, len(parts), MinChannelParts)
	}
	for i, part := range parts {
		if part == "" {
			return Channel{}, fmt.Errorf(
				"%w: %q has an empty segment at position %d (no leading, trailing or repeated dots)",
				ErrInvalidChannel, s, i)
		}
	}

	// The tenant is the first segment; everything after the first dot is the
	// suffix, dots included.
	return Channel{Tenant: parts[0], Suffix: strings.Join(parts[1:], ".")}, nil
}

// BuildChannel joins a tenant and suffix into a channel string, ready to pass to
// any client method.
//
// The tenant must be non-empty and must not contain a dot. A dotted tenant
// would make the prefix ambiguous — "a.b" + "c" and "a" + "b.c" both render
// "a.b.c", and no parse could recover which tenant was meant. The suffix must be
// non-empty and free of empty segments, by the same rule ParseChannel applies.
func BuildChannel(tenant, suffix string) (string, error) {
	if tenant == "" {
		return "", fmt.Errorf("%w: tenant is empty", ErrInvalidChannel)
	}
	if strings.Contains(tenant, ".") {
		return "", fmt.Errorf(
			"%w: tenant %q contains a dot, which would make the tenant prefix ambiguous",
			ErrInvalidChannel, tenant)
	}
	if suffix == "" {
		return "", fmt.Errorf("%w: suffix is empty", ErrInvalidChannel)
	}
	for i, part := range strings.Split(suffix, ".") {
		if part == "" {
			return "", fmt.Errorf(
				"%w: suffix %q has an empty segment at position %d",
				ErrInvalidChannel, suffix, i)
		}
	}

	return tenant + "." + suffix, nil
}
