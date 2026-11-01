package snapschedule

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
)

// TestLeaseLocksAreTheTwoLeafMutexes pins the locks that moved here from
// package cephfs (whose TestMDSRosterAddsNoMutex counted them): the lease
// set's mutex and each lease's mutex are the only sync primitives; the set's
// is taken only by reserve and release, which run no command; and the
// package takes no admission, setup gate or init, as before.
func TestLeaseLocksAreTheTwoLeafMutexes(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var mutexes, setLockers []string
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			switch decl := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range st.Fields.List {
						if sel, ok := field.Type.(*ast.SelectorExpr); ok {
							if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sync" {
								for _, name := range field.Names {
									mutexes = append(mutexes, ts.Name.Name+"."+name.Name)
								}
							}
						}
					}
				}
			case *ast.FuncDecl:
				if decl.Recv == nil && decl.Name.Name == "init" {
					t.Errorf("%s declares init", file)
				}
				if decl.Body == nil {
					continue
				}
				ast.Inspect(decl.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch sel.Sel.Name {
					case "Admit", "WithSetupGate", "LockTopology":
						t.Errorf("%s calls %s; snapshot schedules take no admission", decl.Name.Name, sel.Sel.Name)
					case "Ceph", "Command":
						if decl.Name.Name == "reserve" || decl.Name.Name == "release" {
							t.Errorf("%s runs a command while it holds the lease set", decl.Name.Name)
						}
					case "Lock":
						if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "mu" {
							if id, ok := inner.X.(*ast.Ident); ok && id.Name == "s" {
								setLockers = append(setLockers, decl.Name.Name)
							}
						}
					}
					return true
				})
			}
		}
	}
	slices.Sort(mutexes)
	if !slices.Equal(mutexes, []string{"Lease.mu", "leaseSet.mu"}) {
		t.Errorf("sync primitives changed: %v", mutexes)
	}
	slices.Sort(setLockers)
	if !slices.Equal(setLockers, []string{"release", "reserve"}) {
		t.Errorf("the lease set is locked in %v, want only reserve and release", setLockers)
	}
}

// A directory holds one lease per filesystem: the same path of another
// filesystem of the cluster, or of a filesystem recreated under the same
// name, is another key.
func TestLeaseSetKeysDirectoriesByFilesystem(t *testing.T) {
	fake := newFakeSnapSchedule()
	first := snapScheduleFilesystem(fake)
	set := leasesOf(first.Cluster())
	recreated := fsbase.NewCore(fsbase.Params{Cluster: first.Cluster(), Name: "tc"})
	a := &Lease{Path: "/", filesystem: first, leases: set}
	b := &Lease{Path: "/", filesystem: recreated, leases: set}
	if !set.reserve(a) || !set.reserve(b) || set.reserve(&Lease{Path: "/", filesystem: first, leases: set}) {
		t.Fatal("leases are not kept per filesystem and directory")
	}
	if leasesOf(first.Cluster()) != set {
		t.Fatal("the cluster's lease set is not stable")
	}
	a.release()
	if !set.reserve(&Lease{Path: "/", filesystem: first, leases: set}) || set.paths[leaseKey{recreated, "/"}] != b {
		t.Fatal("release freed another filesystem's directory or kept its own")
	}
}
