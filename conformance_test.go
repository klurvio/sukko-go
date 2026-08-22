package sukko

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Conformance tests check the SDK against the vendored platform contracts in
// testdata/contracts.
//
// They answer a question unit tests cannot: not "does the code do what it says"
// but "does what it says still match what the server speaks". Both failures are
// silent without these — a stale contract still round-trips its own fixtures,
// and an unhandled message type is indistinguishable from one that never
// arrives.

const contractsDir = "testdata/contracts"

// TestVendoredContractsMatchChecksums fails when a vendored contract was edited
// without its checksum being regenerated, or the reverse.
//
// The point is not to detect tampering. It is to make a half-finished refresh —
// contract updated, checksums not, or a file hand-edited to make a test pass —
// impossible to leave in the tree unnoticed. Same idea as go.sum, applied to a
// document.
func TestVendoredContractsMatchChecksums(t *testing.T) {
	recorded := readChecksums(t)
	if len(recorded) == 0 {
		t.Fatal("CHECKSUMS is empty")
	}

	for name, want := range recorded {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(contractsDir, name))
			if err != nil {
				t.Fatalf("CHECKSUMS names %s but it is not vendored: %v", name, err)
			}

			sum := sha256.Sum256(data)
			if got := hex.EncodeToString(sum[:]); got != want {
				t.Errorf("checksum mismatch for %s\n  have: %s\n  want: %s\n"+
					"Re-vendor the contract and regenerate CHECKSUMS together — see %s/README.md",
					name, got, want, contractsDir)
			}
		})
	}

	// The reverse direction: a contract added to the directory without an entry
	// would otherwise be unverified while everything stays green.
	entries, err := os.ReadDir(contractsDir)
	if err != nil {
		t.Fatalf("read %s: %v", contractsDir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".yaml") {
			continue
		}
		if _, ok := recorded[name]; !ok {
			t.Errorf("%s is vendored but has no CHECKSUMS entry", name)
		}
	}
}

// TestSDKCoversEveryContractMessageType is the guard that makes a platform
// release visible.
//
// It enumerates every message type the AsyncAPI contract defines and asserts the
// SDK accounts for each one — decodable types in the registry, client-originated
// types in the send set. When the platform ships a new type, this test names it
// and fails. Without it, the SDK would route the new frame through the
// unknown-type path forever, which is deliberately silent because that same path
// is what lets an old client talk to a new server.
//
// Failing here is not a broken test. It is the contract reporting a feature the
// SDK has not implemented yet.
func TestSDKCoversEveryContractMessageType(t *testing.T) {
	contractTypes := extractMessageTypes(t)

	// A contract this small should never parse to a handful of types; if the
	// extraction silently stopped matching, every assertion below would pass
	// vacuously and the test would guard nothing.
	const minPlausibleTypes = 20
	if len(contractTypes) < minPlausibleTypes {
		t.Fatalf("extracted only %d message types from the contract (%v) — the extraction rule no longer matches the document's shape",
			len(contractTypes), sortedKeys(contractTypes))
	}

	handled := make(map[string]bool, len(decodeRegistry)+len(sendRegistry))
	for wireType := range decodeRegistry {
		handled[wireType] = true
	}
	for wireType := range sendRegistry {
		handled[wireType] = true
	}

	for _, wireType := range sortedKeys(contractTypes) {
		if !handled[wireType] {
			t.Errorf("contract defines message type %q but the SDK neither decodes nor sends it", wireType)
		}
	}

	for _, wireType := range sortedKeys(handled) {
		if !contractTypes[wireType] {
			t.Errorf("SDK handles message type %q but the contract does not define it — the type was renamed or removed upstream", wireType)
		}
	}
}

// TestDecodeAndSendSetsAreDisjoint keeps the two directions from drifting into
// each other. A type in both sets means the SDK would try to decode a frame it
// only ever emits, which is a sign the direction was misread when the type was
// added.
func TestDecodeAndSendSetsAreDisjoint(t *testing.T) {
	for wireType := range sendRegistry {
		if _, ok := decodeRegistry[wireType]; ok {
			t.Errorf("type %q is registered as both decodable and client-originated", wireType)
		}
	}
}

// ─── contract extraction ───

// constLine matches a JSON-Schema const declaration and captures its
// indentation, so the property it belongs to can be identified.
var constLine = regexp.MustCompile(`^(\s*)const:\s*(\S+)\s*$`)

// propertyLine matches a bare mapping key — a property name with no inline
// value, which is how a schema property opens.
var propertyLine = regexp.MustCompile(`^(\s*)([a-z_]+):\s*$`)

// extractMessageTypes reads the message type names out of the vendored AsyncAPI.
//
// It scans lexically rather than parsing YAML, because a YAML parser would be a
// dependency this module does not otherwise need, and the structure being read
// is trivially regular: a message type is a const under a property named "type".
// That rule is what separates the type names from the other consts in the
// document — publish_ack's "accepted" and reconnect_ack's "completed" are consts
// under "status", and counting them as message types would demand the SDK handle
// two frames that do not exist.
//
// The rule is narrow enough to break if the contract's shape changes, which is
// why the caller asserts a plausible minimum: an extraction that quietly stops
// matching must fail rather than report an empty contract.
func extractMessageTypes(t *testing.T) map[string]bool {
	t.Helper()

	path := filepath.Join(contractsDir, "client-ws.asyncapi.yaml")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close %s: %v", path, err)
		}
	}()

	// propertyAt records the most recent bare key seen at each indentation, so a
	// const can look up the property enclosing it.
	propertyAt := map[int]string{}
	types := map[string]bool{}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if m := constLine.FindStringSubmatch(line); m != nil {
			// A property's const sits one nesting level in from the property
			// key itself.
			indent := len(m[1])
			if propertyAt[indent-2] == "type" {
				types[strings.Trim(m[2], `"'`)] = true
			}
			continue
		}

		if m := propertyLine.FindStringSubmatch(line); m != nil {
			indent := len(m[1])
			propertyAt[indent] = m[2]
			// Keys nested deeper belong to a scope that just ended; leaving
			// them would let a const match a property from a sibling schema.
			for recorded := range propertyAt {
				if recorded > indent {
					delete(propertyAt, recorded)
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}

	return types
}

// readChecksums parses the shasum-format CHECKSUMS file.
func readChecksums(t *testing.T) map[string]string {
	t.Helper()

	path := filepath.Join(contractsDir, "CHECKSUMS")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	sums := map[string]string{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("%s line %d is not in shasum format: %q", path, i+1, line)
		}
		sums[strings.TrimPrefix(fields[1], "*")] = fields[0]
	}
	return sums
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
