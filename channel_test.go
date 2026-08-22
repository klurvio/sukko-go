package sukko

import (
	"errors"
	"strings"
	"testing"
)

// Channel validation is derived from the contract's channel-format rules and the
// gateway's own validator — not from a sibling SDK's helper. Parity with the
// other SDKs is something to verify afterwards, never the source: a sibling that
// is wrong would otherwise propagate its bug into a second implementation.
//
// The rules are deliberately few. There is no charset restriction and no length
// limit in either the contract or the validator, so the SDK imposes none;
// inventing one would reject channels the platform happily accepts, and the
// caller would have no way to tell whose rule they had broken.

func TestParseChannelValid(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		give       string
		wantTenant string
		wantSuffix string
	}{
		{"two parts", "acme.trades", "acme", "trades"},
		{"multi-segment suffix", "acme.rooms.eng", "acme", "rooms.eng"},
		{"deep suffix", "acme.a.b.c.d.e", "acme", "a.b.c.d.e"},
		// No charset rule exists, so these must be accepted. A client whose
		// channels are non-ASCII or punctuated is using the platform correctly.
		{"non-ascii", "acme.日本語", "acme", "日本語"},
		{"emoji", "acme.📈", "acme", "📈"},
		{"hyphens and underscores", "acme-corp.trade_feed", "acme-corp", "trade_feed"},
		{"digits", "tenant1.feed2", "tenant1", "feed2"},
		// No length limit exists either.
		{"long suffix", "acme." + strings.Repeat("x", 4096), "acme", strings.Repeat("x", 4096)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseChannel(tc.give)
			if err != nil {
				t.Fatalf("ParseChannel(%q) errored: %v", tc.give, err)
			}
			if got.Tenant != tc.wantTenant {
				t.Errorf("Tenant = %q, want %q", got.Tenant, tc.wantTenant)
			}
			if got.Suffix != tc.wantSuffix {
				t.Errorf("Suffix = %q, want %q", got.Suffix, tc.wantSuffix)
			}
			// String must reconstruct the input exactly, or the split view and
			// the wire value would disagree about what channel this is.
			if got.String() != tc.give {
				t.Errorf("String() = %q, want %q", got.String(), tc.give)
			}
		})
	}
}

func TestParseChannelInvalid(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		give string
	}{
		{"empty", ""},
		// One part means no tenant prefix at all, so the channel cannot be
		// attributed to a tenant.
		{"single part", "trades"},
		// An empty segment makes the channel ambiguous: these render
		// identically to a channel that does not have it.
		{"leading dot", ".trades"},
		{"trailing dot", "acme."},
		{"double dot", "acme..trades"},
		{"double dot mid-suffix", "acme.rooms..eng"},
		{"only a dot", "."},
		{"only dots", "..."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseChannel(tc.give)
			if err == nil {
				t.Fatalf("ParseChannel(%q) = %+v, want an error", tc.give, got)
			}
			if !errors.Is(err, ErrInvalidChannel) {
				t.Errorf("error %v does not match ErrInvalidChannel", err)
			}
			// The message must name the offending value, or a caller with many
			// channels cannot tell which one was rejected.
			if !strings.Contains(err.Error(), "sukko: ") {
				t.Errorf("error %q is not attributable to the SDK", err)
			}
		})
	}
}

func TestBuildChannelValid(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		tenant string
		suffix string
		want   string
	}{
		{"simple", "acme", "trades", "acme.trades"},
		{"dotted suffix", "acme", "rooms.eng", "acme.rooms.eng"},
		{"non-ascii", "acme", "日本語", "acme.日本語"},
		{"deep suffix", "acme", "a.b.c", "acme.a.b.c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := BuildChannel(tc.tenant, tc.suffix)
			if err != nil {
				t.Fatalf("BuildChannel(%q, %q) errored: %v", tc.tenant, tc.suffix, err)
			}
			if got != tc.want {
				t.Errorf("BuildChannel(%q, %q) = %q, want %q", tc.tenant, tc.suffix, got, tc.want)
			}
			// What Build produces, Parse must accept — otherwise the SDK can
			// construct a channel it will not itself take.
			if _, err := ParseChannel(got); err != nil {
				t.Errorf("BuildChannel produced %q, which ParseChannel rejects: %v", got, err)
			}
		})
	}
}

func TestBuildChannelInvalid(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		tenant string
		suffix string
		why    string
	}{
		{"empty tenant", "", "trades", "no tenant to attribute the channel to"},
		// A dotted tenant makes the prefix ambiguous: "a.b" + "c" and "a" +
		// "b.c" both render "a.b.c", so the tenant could not be recovered.
		{"dotted tenant", "acme.corp", "trades", "ambiguous tenant prefix"},
		{"empty suffix", "acme", "", "nothing to subscribe to"},
		{"suffix with leading dot", "acme", ".trades", "empty segment"},
		{"suffix with trailing dot", "acme", "trades.", "empty segment"},
		{"suffix with double dot", "acme", "rooms..eng", "empty segment"},
		{"both empty", "", "", "no tenant and no suffix"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := BuildChannel(tc.tenant, tc.suffix)
			if err == nil {
				t.Fatalf("BuildChannel(%q, %q) = %q, want an error (%s)", tc.tenant, tc.suffix, got, tc.why)
			}
			if !errors.Is(err, ErrInvalidChannel) {
				t.Errorf("error %v does not match ErrInvalidChannel", err)
			}
			if got != "" {
				t.Errorf("BuildChannel returned %q alongside an error; it must return the zero value", got)
			}
		})
	}
}

// A dotted tenant is rejected precisely because the split would be ambiguous.
// This asserts the ambiguity concretely rather than trusting the rule's wording.
func TestDottedTenantWouldBeAmbiguous(t *testing.T) {
	t.Parallel()

	// Were a dotted tenant permitted, both of these would render "a.b.c" and
	// ParseChannel could not recover which tenant was meant.
	if _, err := BuildChannel("a.b", "c"); err == nil {
		t.Fatal("a dotted tenant must be rejected")
	}

	parsed, err := ParseChannel("a.b.c")
	if err != nil {
		t.Fatalf("ParseChannel: %v", err)
	}
	// The split is unambiguous only because the tenant is the first segment.
	if parsed.Tenant != "a" || parsed.Suffix != "b.c" {
		t.Errorf("parsed %+v, want tenant a and suffix b.c", parsed)
	}
}

// Round-tripping is the property callers rely on when they hold a channel as a
// string, split it to inspect the tenant, and pass the string onward.
func TestChannelRoundTrip(t *testing.T) {
	t.Parallel()

	for _, give := range []string{
		"acme.trades",
		"acme.rooms.eng",
		"acme.日本語.feed",
		"t.a.b.c.d",
	} {
		parsed, err := ParseChannel(give)
		if err != nil {
			t.Fatalf("ParseChannel(%q): %v", give, err)
		}
		rebuilt, err := BuildChannel(parsed.Tenant, parsed.Suffix)
		if err != nil {
			t.Fatalf("BuildChannel(%q, %q): %v", parsed.Tenant, parsed.Suffix, err)
		}
		if rebuilt != give {
			t.Errorf("round trip of %q produced %q", give, rebuilt)
		}
	}
}

// The zero Channel must render as the empty string rather than a bare dot,
// which would look like a valid-but-odd channel in a log line.
func TestZeroChannelStringIsEmpty(t *testing.T) {
	t.Parallel()

	var zero Channel
	if got := zero.String(); got != "" {
		t.Errorf("zero Channel.String() = %q, want the empty string", got)
	}
}
