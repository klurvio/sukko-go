package sukko

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These are the SDK's standing structural guarantees — the ones that hold today
// and would erode one convenient import or one debugging print at a time.
//
// Each is cheap to state and impossible to enforce by review alone: a reviewer
// has to notice an absence, which is exactly what reviewers are worst at. So
// they are tests, and they fail the build.

// TestExactlyOneRuntimeDependency pins the SDK's dependency claim.
//
// "One runtime dependency" is a promise to callers: it bounds their audit
// surface, their supply-chain exposure, and their upgrade burden. It is also
// the kind of promise that decays invisibly — a transitive dependency arrives
// with an unrelated convenience and nobody re-checks the count.
//
// Test-only dependencies are exempt, which is why this inspects the non-test
// dependency graph specifically.
func TestExactlyOneRuntimeDependency(t *testing.T) {
	t.Parallel()

	out, err := exec.CommandContext(context.Background(), "go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}

	std := stdlibPackages(t)
	external := map[string]bool{}
	for pkg := range strings.FieldsSeq(string(out)) {
		if pkg == "github.com/sukko-dev/sdk-go" || std[pkg] {
			continue
		}
		// Reduce a package path to its module root for counting: the promise is
		// about modules a caller must vet, not packages within one.
		external[moduleRoot(pkg)] = true
	}

	// The invariant is "no more than one, and only this one". It is expressed as
	// a ceiling rather than an equality because the count is legitimately zero
	// until the WebSocket transport lands: coder/websocket is currently imported
	// only by the test harness, so it is not yet a *runtime* dependency. An
	// equality would fail today for the wrong reason and have to be loosened
	// later — which is how a guard ends up deleted instead of trusted.
	const allowed = "github.com/coder/websocket"

	for dep := range external {
		if dep != allowed {
			t.Errorf("unexpected runtime dependency %q; the SDK ships with at most one (%s)", dep, allowed)
		}
	}
	if len(external) > 1 {
		t.Errorf("runtime dependencies = %v, want at most one", keys(external))
	}
}

// TestNoPlatformRepoDependency pins the module boundary.
//
// The SDK derives its behavior from the published contracts, never from the
// platform's own source. Go's internal/ rule already blocks importing the
// server's protocol package, but nothing blocks importing some other part of
// the platform module — and doing so would couple an independently-released
// client to the server's release cycle, and make the SDK's CI depend on a repo
// it does not check out.
func TestNoPlatformRepoDependency(t *testing.T) {
	t.Parallel()

	out, err := exec.CommandContext(context.Background(), "go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}

	for pkg := range strings.FieldsSeq(string(out)) {
		if strings.Contains(pkg, "sukko-dev/sukko/") || pkg == "github.com/sukko-dev/sukko" {
			t.Errorf("imports the platform module (%s); the SDK must derive behavior from the contracts alone", pkg)
		}
	}
}

// TestLibraryIsQuietByDefault pins the logging discipline at the source level.
//
// A library that writes to the process's default logger is writing into its
// caller's output stream uninvited. The rule is that the SDK logs only through
// a logger the caller supplied, so it must never call slog.SetDefault, and must
// never hold a package-level logger that could be used without one being passed.
//
// This is the source half of the guarantee; the behavioral half — running a
// full lifecycle with no logger configured and asserting nothing is emitted —
// belongs with the client tests, where there is a lifecycle to run.
func TestLibraryIsQuietByDefault(t *testing.T) {
	t.Parallel()

	forEachNonTestFile(t, func(_ string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "slog" {
				return true
			}
			// SetDefault installs a logger process-wide; Default reads whatever
			// the process happens to have, which is the same violation from the
			// other direction — both bypass the caller's choice.
			if sel.Sel.Name == "SetDefault" || sel.Sel.Name == "Default" {
				t.Errorf("%s: calls slog.%s; the SDK must log only through a caller-supplied logger",
					fset.Position(call.Pos()), sel.Sel.Name)
			}
			return true
		})

		// A package-level logger is a logger nobody passed in.
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				if typeMentions(value.Type, "slog") {
					t.Errorf("%s: declares a package-level slog value; a logger must be per-client, from the caller",
						fset.Position(value.Pos()))
				}
			}
		}
	})
}

// TestClockIsTheOnlyTimeSource pins the determinism seam at the source level.
//
// Every timing path goes through the injectable Clock, so tests advance time
// instead of sleeping. A single direct time.Now() undoes that quietly: the
// affected path still works, its test still passes, and it becomes flaky only
// under load on someone else's machine. The failure mode is remote and
// intermittent, which is precisely why it needs a mechanical check.
//
// clock.go is exempt: it is where the real clock is legitimately read.
func TestClockIsTheOnlyTimeSource(t *testing.T) {
	t.Parallel()

	banned := map[string]string{
		"Now":       "use Clock.Now()",
		"Since":     "use Clock.Now() and subtract",
		"After":     "use Clock.NewTimer(d, purpose)",
		"Tick":      "use Clock.NewTicker(d, purpose)",
		"NewTimer":  "use Clock.NewTimer(d, purpose)",
		"NewTicker": "use Clock.NewTicker(d, purpose)",
		"Sleep":     "never sleep in the SDK; arm a timer on the Clock",
	}

	forEachNonTestFile(t, func(path string, file *ast.File, fset *token.FileSet) {
		if filepath.Base(path) == "clock.go" {
			return // the one place the real clock is read
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "time" {
				return true
			}
			if advice, bad := banned[sel.Sel.Name]; bad {
				t.Errorf("%s: calls time.%s; %s",
					fset.Position(call.Pos()), sel.Sel.Name, advice)
			}
			return true
		})
	})
}

// ─── helpers ───

// forEachNonTestFile parses every non-test Go file in the package and hands it
// to fn. The guards inspect source rather than behavior because they are
// asserting an absence, and an absence has no runtime signal to observe.
func forEachNonTestFile(t *testing.T, fn func(path string, file *ast.File, fset *token.FileSet)) {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package directory: %v", err)
	}

	fset := token.NewFileSet()
	var seen int
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		seen++
		fn(name, file, fset)
	}

	// A guard that silently inspected nothing would report green forever.
	if seen == 0 {
		t.Fatal("no non-test source files found; the guard would pass vacuously")
	}
}

// typeMentions reports whether a type expression references a package. A
// declaration written without an explicit type (var x = expr) has a nil Type,
// which ast.Inspect will not accept.
func typeMentions(expr ast.Expr, pkg string) bool {
	if expr == nil {
		return false
	}
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == pkg {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// stdlibPackages returns the standard library's package set, so the dependency
// guard can tell "stdlib" from "a module a caller must vet" without hardcoding
// a list that would drift with each Go release.
func stdlibPackages(t *testing.T) map[string]bool {
	t.Helper()

	out, err := exec.CommandContext(context.Background(), "go", "list", "std").Output()
	if err != nil {
		t.Fatalf("go list std: %v", err)
	}
	set := map[string]bool{}
	for pkg := range strings.FieldsSeq(string(out)) {
		set[pkg] = true
	}
	return set
}

// moduleRoot reduces a package path to the module it most likely belongs to —
// the first three path segments for a hosted module. The guard counts modules,
// not packages.
func moduleRoot(pkg string) string {
	parts := strings.Split(pkg, "/")
	if len(parts) >= 3 {
		return strings.Join(parts[:3], "/")
	}
	return pkg
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
