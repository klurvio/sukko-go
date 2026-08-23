package sukko

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"gopkg.in/yaml.v3"
)

// The contract-example tests are the only ones in this package whose
// expectations the SDK did not author.
//
// Every other test compares the code against fixtures and tables written
// alongside it, which proves the pieces agree with each other and says nothing
// about whether they agree with the server. These read the `examples:` blocks
// out of the vendored AsyncAPI — the contract's own statement of what a frame
// looks like — and require each wire struct to reproduce one exactly.
//
// That distinction is not academic. A struct can round-trip a hand-written
// fixture perfectly while modeling a field the server never sends, or missing
// the nesting the server requires, and no amount of internal consistency will
// reveal it.

// TestContractExamplesRoundTrip loads every frame example the contract defines
// into the struct the SDK uses for that type, re-encodes it, and requires the
// result to carry exactly what the contract's example carried.
//
// A dropped field means the SDK ignores something the server sends. An invented
// field means the SDK emits something the server does not define. Both are
// contract violations under §I, and both are invisible to a fixture the SDK
// wrote for itself.
func TestContractExamplesRoundTrip(t *testing.T) {
	examples, skipped := loadContractExamples(t)

	// Every type in either registry must have contract examples to check
	// against; a type with none is a type this test cannot vouch for.
	covered := map[string]bool{}

	for _, ex := range examples {
		t.Run(ex.schema, func(t *testing.T) {
			construct, ok := registryFor(ex.wireType)
			if !ok {
				t.Fatalf("contract schema %s defines type %q, which the SDK neither decodes nor sends", ex.schema, ex.wireType)
			}
			covered[ex.wireType] = true

			target := construct()
			if err := json.Unmarshal(ex.frame, target); err != nil {
				t.Fatalf("contract example does not load into %T: %v\nexample: %s", target, err, ex.frame)
			}

			reencoded, err := json.Marshal(target)
			if err != nil {
				t.Fatalf("re-encode %T: %v", target, err)
			}

			if diff := jsonDiff(t, ex.frame, reencoded); diff != "" {
				t.Errorf("%s does not match the contract's example:\n%s\n  contract: %s\n  sdk:      %s",
					ex.schema, diff, ex.frame, reencoded)
			}
		})
	}

	for _, wireType := range sortedKeys(allHandledTypes()) {
		if covered[wireType] {
			continue
		}
		// A type whose only example is deliberately skipped is accounted for —
		// the skip carries a written reason. A type with no example at all is
		// not, and the SDK's shape for it rests on nothing but assertion.
		if reason, ok := skipped[wireType]; ok {
			t.Logf("type %q is verified only by its skip decision: %s", wireType, reason)
			continue
		}
		t.Errorf("type %q has no contract example, so its shape is unverified against the server", wireType)
	}
}

// contractExample is one `examples:` entry from the vendored AsyncAPI, carrying
// the schema or message that declared it so failures name something a reader can
// find in the document.
type contractExample struct {
	schema   string
	wireType string
	frame    []byte
}

// skippedExamples names contract examples the SDK deliberately does not
// implement, with the reason. Anything not listed here must round-trip.
//
// An explicit list is the point. Silently ignoring an example the SDK cannot
// satisfy is how the send frames got shipped broken in the first place; a named
// exclusion with a reason is a decision, and an unnamed one is a bug.
var skippedExamples = map[string]string{
	"subscribeWithHistory": "the contract's second subscribe mode (single channel + inline history) " +
		"is not implemented: History() covers the capability with a simpler surface, and a field the " +
		"SDK never sets is a field that cannot be exercised. Modeling both modes in one struct would " +
		"make `channels` and `channel` dual-purpose, which §IX forbids.",

	"heartbeatExample": "heartbeat is the one send frame whose `data` the contract marks optional " +
		"(required: [type]), and the example includes it only as an empty object. The SDK sends the " +
		"minimal valid frame instead. This is the sole example where omitting a field is correct — " +
		"everywhere else `data` is required, which is what the rest of this table enforces.",
}

// loadContractExamples collects every complete frame example the contract
// carries, from both places AsyncAPI puts them.
//
// Schema examples (`components.schemas.<Name>.examples`) are bare frames.
// Message examples (`components.messages.<name>.examples`) wrap the frame in a
// `payload` key and carry a `name`. Server-sent types are described by the
// former, client-sent types almost entirely by the latter — so reading only one
// section leaves an entire direction unverified, which is precisely the gap that
// let the send frames ship un-nested.
//
// Property-level examples (a sample token string, a channel name) are skipped by
// the same rule that selects frames: a frame is a mapping with a string `type`.
//
// Parsing with a real YAML library rather than scanning lexically matters here —
// these examples nest, and an approximate parser would be the same class of
// mistake this test exists to catch.
func loadContractExamples(t *testing.T) (examples []contractExample, skippedTypes map[string]string) {
	t.Helper()

	path := filepath.Join(contractsDir, "client-ws.asyncapi.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Examples []map[string]any `yaml:"examples"`
			} `yaml:"schemas"`
			Messages map[string]struct {
				Examples []struct {
					Name    string         `yaml:"name"`
					Payload map[string]any `yaml:"payload"`
				} `yaml:"examples"`
			} `yaml:"messages"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(doc.Components.Schemas) == 0 || len(doc.Components.Messages) == 0 {
		t.Fatalf("%s has %d schemas and %d messages — the document's shape changed",
			path, len(doc.Components.Schemas), len(doc.Components.Messages))
	}

	var out []contractExample
	skippedTypes = map[string]string{}
	add := func(label string, example map[string]any) {
		wireType, ok := example["type"].(string)
		if !ok {
			// A property-level example, not a frame.
			return
		}
		frame, err := json.Marshal(example)
		if err != nil {
			t.Fatalf("convert %s example to JSON: %v", label, err)
		}
		out = append(out, contractExample{schema: label, wireType: wireType, frame: frame})
	}

	for _, name := range sortedSchemaNames(doc.Components.Schemas) {
		for _, example := range doc.Components.Schemas[name].Examples {
			add(name, example)
		}
	}
	for _, name := range sortedSchemaNames(doc.Components.Messages) {
		for _, example := range doc.Components.Messages[name].Examples {
			if reason, skip := skippedExamples[example.Name]; skip {
				t.Logf("skipping contract example %q: %s", example.Name, reason)
				if wireType, ok := example.Payload["type"].(string); ok {
					skippedTypes[wireType] = reason
				}
				continue
			}
			add(name, example.Payload)
		}
	}

	// The contract carries examples for every message type; finding almost none
	// means the traversal stopped matching rather than that the contract emptied.
	const minPlausibleExamples = 25
	if len(out) < minPlausibleExamples {
		t.Fatalf("found only %d frame examples in the contract — the traversal no longer matches the document's shape", len(out))
	}
	return out, skippedTypes
}

// registryFor resolves a wire type to its struct constructor from whichever
// direction defines it.
func registryFor(wireType string) (func() any, bool) {
	if construct, ok := decodeRegistry[wireType]; ok {
		return construct, true
	}
	construct, ok := sendRegistry[wireType]
	return construct, ok
}

// allHandledTypes is the union of both directions.
func allHandledTypes() map[string]bool {
	all := make(map[string]bool, len(decodeRegistry)+len(sendRegistry))
	for wireType := range decodeRegistry {
		all[wireType] = true
	}
	for wireType := range sendRegistry {
		all[wireType] = true
	}
	return all
}

func sortedSchemaNames[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
