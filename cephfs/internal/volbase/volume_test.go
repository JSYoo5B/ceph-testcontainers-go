package volbase

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

const refusal = "CephFS filesystem must be an active filesystem created by this cluster"

// Begin keeps the refusal text of the subvolume flow for every predicate it
// owns, returns a cancellation as is, and holds no gate after a refusal.
func TestBeginKeepsItsRefusalTextAndNoOpDone(t *testing.T) {
	for _, change := range []string{"native", "descriptor", "unregistered", "closed"} {
		t.Run(change, func(t *testing.T) {
			fs := fstest.NewFilesystem()
			switch change {
			case "native":
				fs.Core.ReplaceNative(nil)
			case "descriptor":
				fs.Descriptor = "sibling"
			case "unregistered":
				fs.Unregistered = true
			case "closed":
				testhelper.TestSetClosed(fs.Core.Cluster(), true)
			}
			_, _, done, err := Begin(t.Context(), fs.Core)
			if err == nil || err.Error() != refusal || done == nil {
				t.Fatalf("subvolume refusal changed: %v", err)
			}
			done() // a no-op on refusal
			if len(fs.Control.Calls) != 0 {
				t.Fatalf("a refused admission ran native commands: %v", fs.Control.Calls)
			}
			fstest.AssertAdmissionReleased(t, fs.Core.Cluster(), fs.SetupMu)
		})
	}
	fs := fstest.NewFilesystem()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := Begin(canceled, fs.Core); err != context.Canceled {
		t.Fatalf("subvolume admission did not return the cancellation: %v", err)
	}
	if _, _, _, err := Begin(t.Context(), nil); err == nil || err.Error() != "CephFS filesystem is unavailable" {
		t.Fatalf("nil core refusal changed: %v", err)
	}
	_, id, done, err := Begin(t.Context(), fs.Core)
	if err != nil || id != 41 {
		t.Fatalf("a live filesystem was refused or lost its FSMap id: %d %v", id, err)
	}
	done()
	fstest.AssertAdmissionReleased(t, fs.Core.Cluster(), fs.SetupMu)
}

func TestOfReturnsTheSharedVolumeAndCheckIdentityRefusesReplacement(t *testing.T) {
	v := &Volume{FilesystemID: 41, Path: "/volumes/group/volume/id", CreatedAt: "t"}
	owner := &struct{ Handle }{NewHandle(v)}
	copied := *owner
	if Of(owner) != v || Of(&copied) != v || Of(&struct{ Handle }{}) != nil {
		t.Fatal("Of does not return the shared volume of every copy")
	}
	if err := v.CheckIdentity(41, v.Path, v.CreatedAt); err != nil {
		t.Fatal(err)
	}
	for _, replaced := range [][3]any{{int64(42), v.Path, v.CreatedAt}, {int64(41), "/other", v.CreatedAt}, {int64(41), v.Path, "u"}} {
		err := v.CheckIdentity(replaced[0].(int64), replaced[1].(string), replaced[2].(string))
		if err == nil || err.Error() != "CephFS resource was replaced outside this fixture; refusing mutation" {
			t.Fatalf("replacement %v accepted or refused with another text: %v", replaced, err)
		}
	}
}

// featureFiles parses the non-test files of volbase and of the feature
// packages that use it.
func featureFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	out := map[string]*ast.File{}
	for _, dir := range []string{".", "../../subvolume", "../../quiesce"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			out[file] = parsed
		}
	}
	if len(out) < 3 {
		t.Fatalf("feature packages not found: %v", out)
	}
	return out
}

func callee(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

func receiver(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// TestSubvolumeAdmissionHasOneOwnerAndKeepsItsPredicateLocal pins, for the
// subvolume flow that the feature packages share, what the cephfs guards
// TestAdmissionLifetimeHasOneOwnerAndKeepsPredicatesLocal,
// TestFilesystemIdentityIsReadOnlyFromImmutableName and
// TestNativeIdentityIsCapturedNotReplacedInProduction pin for package cephfs:
// in volbase, cephfs/subvolume and cephfs/quiesce only Begin calls Admit, it
// passes exactly one own Check function and takes, releases or unlocks no lock
// itself; only Begin reads the exported descriptor through DescriptorIntact;
// no production code installs or captures a native identity; and only the
// pin restoration reads under the setup gate without an admission.
func TestSubvolumeAdmissionHasOneOwnerAndKeepsItsPredicateLocal(t *testing.T) {
	sites := map[string][]string{}
	var begin *ast.FuncDecl
	for _, parsed := range featureFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if r := receiver(fn); r != "" {
				name = r + "." + name
			}
			if name == "Begin" {
				begin = fn
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch c := callee(call); c {
					case "Admit", "DescriptorIntact", "ReplaceNative", "CaptureNativePools", "WithSetupGate":
						sites[c] = append(sites[c], name)
					}
				}
				return true
			})
		}
	}
	want := map[string][]string{
		"Admit":            {"Begin"},
		"DescriptorIntact": {"Begin"},
		"WithSetupGate":    {"PinOverrideHandle.Restore"},
	}
	if !reflect.DeepEqual(sites, want) {
		t.Errorf("shared admission sites changed: %v, want %v", sites, want)
	}
	if begin == nil {
		t.Fatal("Begin is missing")
	}
	checks := 0
	ast.Inspect(begin.Body, func(n ast.Node) bool {
		if kv, ok := n.(*ast.KeyValueExpr); ok {
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Check" {
				if _, lit := kv.Value.(*ast.FuncLit); lit {
					checks++
				}
			}
		}
		if call, ok := n.(*ast.CallExpr); ok {
			switch callee(call) {
			case "LockMutex", "LockTopology", "Lock", "Unlock":
				t.Errorf("Begin calls %s; the lifetime belongs to Admit", callee(call))
			}
		}
		return true
	})
	if checks != 1 {
		t.Errorf("Begin must pass exactly one own check function, has %d", checks)
	}
}

// TestFeaturePackagesKeepNoLockOrRegistration pins the ownership rules of the
// package docs: volbase keeps no package-level variable, no init and no sync
// value, its Handle has only unexported members, and it imports no public
// cephfs package; cephfs/subvolume and cephfs/quiesce add no init and no
// mutex, so their state is guarded by the filesystem's setup gate alone.
func TestFeaturePackagesKeepNoLockOrRegistration(t *testing.T) {
	for file, parsed := range featureFiles(t) {
		inVolbase := filepath.Dir(file) == "."
		for _, imp := range parsed.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if path == "sync" {
				t.Errorf("%s imports sync: feature state is guarded by the setup gate", file)
			}
			if inVolbase && (strings.HasSuffix(path, "/cephfs") || strings.Contains(path, "/cephfs/") && !strings.Contains(path, "/cephfs/internal/")) {
				t.Errorf("%s imports %s: volbase must not depend on public cephfs packages", file, path)
			}
		}
		for _, decl := range parsed.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv == nil && decl.Name.Name == "init" {
					t.Errorf("%s declares init", file)
				}
			case *ast.GenDecl:
				if decl.Tok == token.VAR && inVolbase {
					t.Errorf("%s declares a package-level variable", file)
				}
				for _, spec := range decl.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok || !inVolbase || ts.Name.Name != "Handle" {
						continue
					}
					for _, field := range ts.Type.(*ast.StructType).Fields.List {
						for _, name := range field.Names {
							if name.IsExported() {
								t.Errorf("Handle has the exported field %s", name.Name)
							}
						}
						if len(field.Names) == 0 {
							t.Error("Handle embeds a type")
						}
					}
				}
			}
		}
	}
	var handleMethods []string
	for _, parsed := range featureFiles(t) {
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && receiver(fn) == "Handle" {
				handleMethods = append(handleMethods, fn.Name.Name)
			}
		}
	}
	if !slices.Equal(handleMethods, []string{"volume"}) {
		t.Errorf("Handle methods changed: %v, want only the unexported volume", handleMethods)
	}
}

// Begin admits the core that it is given; it never looks a filesystem up.
func TestBeginTakesTheCoreItIsGiven(t *testing.T) {
	fs := fstest.NewFilesystem()
	descriptor := "other"
	other := fsbase.NewCore(fsbase.Params{Cluster: fs.Core.Cluster(), Name: "other", DescriptorName: &descriptor, SetupMu: fs.SetupMu, Live: func(*fsbase.Core) bool { return true }})
	if _, _, _, err := Begin(t.Context(), other); err == nil || err.Error() != refusal {
		t.Fatalf("a core without a captured identity was admitted: %v", err)
	}
	fstest.AssertAdmissionReleased(t, fs.Core.Cluster(), fs.SetupMu)
}
