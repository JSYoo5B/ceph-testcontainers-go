package ceph

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// This file guards the shared construction of the invalid-request error
// used by the rbd, cephfs,
// rgw and the two mirror packages each used to keep a copy of
//
//	func invalid(err error) error { return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err} }
//
// and now call internal/servicehook.Invalid, which reaches the one construction
// in package ceph. The tests run without Docker.

// invalidCallSites is the number of non-test call sites of
// servicehook.Invalid in each package, measured by AST; a package has no bare
// invalid function left.
//
// Counts cover the retained standalone service callers. The 65 call sites of
// the former cephfs package are now split across cephfs and the packages it
// was decomposed into. The 64 call sites of the former rgw package are split
// across rgw and its packages in the same way, with one more in rgw: the
// refusal of a root user's account from another cluster or gateway scope.
// S3 key handles add two refusals in rgw and three in rgw/internal/gwbase.
// rgw has eight more for the input, ownership, replaced-target and
// outside-edit refusals of its rate-limit overrides, and thirteen more for the
// input, existing-role, tracked-name, ownership, replaced-role and
// outside-edit refusals of its roles. The watches add their configuration,
// unstarted-Stop and ending-read refusals: one more in cephfs for its MDS
// watch, which share mdsWatchInvalid; one more in cephfs for the MDS
// read-only fault, whose input, ownership, target and recreated-filesystem
// refusals share mdsReadOnlyInvalid; three more in cephfs/subvolume for its
// clone watch; five more in rbd/mirror for its receiver and image watches
// (an invalid configuration, Stop of an unstarted watch, a terminated mirror,
// a terminated cluster and a receiver cohort without a daemon); and five more
// in cephfs/mirror for the refused config, path, fixture and unstarted Stop
// of its directory watch and for cephFSRefusal, the one refusal of a nil or
// terminated mirror or a terminated cluster that ends a watch or a wait and
// that every public method of the mirror shares.
// cephfs/quiesce and cephfs/snapschedule have four more each for their
// watches: the unstarted handle, the refused config, Stop of an unstarted
// watch and the ownership loss that ends one.
// cephfs/session has nine more: the refused admission of a filesystem that
// is no longer the cluster's active owned one, and the input, held-override,
// unavailable-handle, untracked, replaced-filesystem and outside-edit
// refusals of its required client features override, and five more for its
// session watch: the nil filesystem, the refused config, a zero client ID,
// Stop of an unstarted watch and the ownership loss that ends one. Session
// admission also classifies a filesystem with no constructed cluster. RBD
// classifies a cache client socket path that exceeds the librbd address limit
// and a cache path or public socket claimed by a competing setup. RGW
// placement refuses an unavailable gateway generation before restart and a
// committed period outside the expected lineage. Subvolume and group removal
// share one refusal for a directory with an outstanding pin override.
// The CephFS admissions classify their refusals in fsbase: Admit classifies a
// nil core, a refused handle and an identity refusal, and the shared override
// restore its unavailable-handle, already-active, untracked and
// replaced-filesystem refusals, so the session overrides keep only their own
// input and outside-edit refusals there. cephfs classifies the unavailable
// MDS handle and the owner and identity refusals of the status read that
// MDSStatus, its watch and the read-only fault share, and a subvolume pin
// handle classifies its unavailable restore. The RGW typed operations admit
// through gwbase.Admit and AdmitCreation, which classify a gateway without a
// cluster, and ValidateAdmin classifies a replaced container handle; users
// and accounts share gwbase.CheckQuotaLimits for their quota limits, the
// generation-checked gateway restart classifies a gateway that is not
// running, and an inactive user or account, an unowned root account, an
// account-owned user quota, an unconfirmed placement and a placement user
// default's class and empty tag list are classified where they are refused.
var invalidCallSites = map[string]int{
	"rbd": 30, "cephfs": 32, "rgw": 49,
	"cephfs/subvolume": 34, "cephfs/quiesce": 8, "cephfs/internal/fsbase": 9,
	"cephfs/session": 11, "cephfs/snapschedule": 16,
	"rgw/account": 10, "rgw/placement": 23, "rgw/notification": 5, "rgw/internal/gwbase": 21,
	"rbd/mirror": 33, "cephfs/mirror": 17,
}

// countInvalidCalls counts, in the non-test files of dir, the calls of a bare
// invalid(...) and of servicehook.Invalid(...).
func countInvalidCalls(t *testing.T, dir string) (bare, shared int) {
	t.Helper()
	for name, src := range nonTestSources(t, dir) {
		file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				if fn.Name == "invalid" {
					bare++
				}
			case *ast.SelectorExpr:
				if id, ok := fn.X.(*ast.Ident); ok && id.Name == "servicehook" && fn.Sel.Name == "Invalid" {
					shared++
				}
			}
			return true
		})
	}
	return bare, shared
}

func TestInvalidCallSitesAllUseTheSharedConstruction(t *testing.T) {
	for dir, want := range invalidCallSites {
		bare, shared := countInvalidCalls(t, filepath.Join("..", filepath.FromSlash(dir)))
		if bare != 0 || shared != want {
			t.Errorf("%s: bare invalid calls = %d, servicehook.Invalid calls = %d, want 0 and %d", dir, bare, shared, want)
		}
	}
	// Negative controls: the counter sees both shapes.
	bare, shared := countCallsIn(t, "package p\nfunc f() { _ = invalid(nil); _ = servicehook.Invalid(nil) }")
	if bare != 1 || shared != 1 {
		t.Fatalf("the counter missed a call shape: %d %d", bare, shared)
	}
}

func countCallsIn(t *testing.T, src string) (bare, shared int) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "p.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return countInvalidCalls(t, dir)
}

// bodyOf returns the printed body of the function or method named name in src.
func bodyOf(t *testing.T, src, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			var b strings.Builder
			if err := printer.Fprint(&b, fset, fn.Body); err != nil {
				t.Fatal(err)
			}
			return strings.Join(strings.Fields(b.String()), " ")
		}
	}
	t.Fatalf("function %s not found", name)
	return ""
}

// TestInvalidConstructionBodiesAreTheFormerCopy compares function bodies: the
// one construction in ceph is the body that the five copies had, the
// registered hook only forwards to it, the typed accessor only forwards to the
// hook, and ceph's own invalidRequest builds through it.
func TestInvalidConstructionBodiesAreTheFormerCopy(t *testing.T) {
	read := func(dir, file string) string { return nonTestSources(t, dir)[file] }
	if got, want := bodyOf(t, read(".", "errors.go"), "newInvalid"), "{ return &OperationError{Kind: ErrInvalidRequest, Err: err} }"; got != want {
		t.Errorf("newInvalid body = %q, want the former per-package construction %q", got, want)
	}
	if got, want := bodyOf(t, read(".", "errors.go"), "invalidRequest"), "{ return newInvalid(fmt.Errorf(format, args...)) }"; got != want {
		t.Errorf("invalidRequest body = %q, want %q", got, want)
	}
	if got, want := bodyOf(t, read(".", "service_hooks.go"), "Invalid"), "{ return newInvalid(err) }"; got != want {
		t.Errorf("serviceHooks.Invalid body = %q, want %q", got, want)
	}
	if got, want := bodyOf(t, read(filepath.Join("..", "internal", "servicehook"), "servicehook.go"), "Invalid"), "{ return get().Invalid(err) }"; got != want {
		t.Errorf("servicehook.Invalid body = %q, want %q", got, want)
	}
	// Negative control: the comparison tells a classify-based body (which
	// returns nil for a nil error) from the construction.
	if bodyOf(t, "package p\nfunc newInvalid(err error) error { return classify(ErrInvalidRequest, err) }", "newInvalid") == "{ return &OperationError{Kind: ErrInvalidRequest, Err: err} }" {
		t.Fatal("the body comparison does not distinguish a classify-based construction")
	}
}

func TestCoreDoesNotImportTheServiceContract(t *testing.T) {
	if slices.Contains(moduleImports(t, "."), "internal/servicehook") {
		t.Fatal("package ceph imports internal/servicehook; the contract is registered through internal/hookreg")
	}
}
