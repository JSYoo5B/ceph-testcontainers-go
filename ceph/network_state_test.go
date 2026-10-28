package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestNetworkStateBridgesKeepPublicFirst(t *testing.T) {
	public := &testcontainers.DockerNetwork{ID: "p-id", Name: "public"}
	cluster := &testcontainers.DockerNetwork{ID: "c-id", Name: "cluster"}
	for _, tc := range []struct {
		name  string
		state networkState
		want  []string
	}{
		{"host mode has none", networkState{}, []string{}},
		{"single bridge", networkState{public: public}, []string{"public"}},
		{"separate bridge", networkState{public: public, cluster: cluster}, []string{"public", "cluster"}},
		{"only a replication record", networkState{cluster: cluster}, []string{"cluster"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.state.bridgeNames()
			if got == nil || !slices.Equal(got, tc.want) {
				t.Fatalf("bridgeNames = %#v, want %#v", got, tc.want)
			}
			if len(tc.state.bridges()) != len(tc.want) {
				t.Fatalf("bridges = %v", tc.state.bridges())
			}
		})
	}
}

// removeBridges must not call Docker for a bridge that is recorded as removed,
// absent, or named as blocked by a failed interruption undo, and it must not
// record a removal it skipped. A removal Docker would perform cannot run here;
// the Terminate-level control in TestTerminateBlocksOnlyNetworksOfFailedInterruptions
// covers the blocked bridge through the real teardown path.
func TestNetworkStateRemoveBridgesSkipsWithoutDocker(t *testing.T) {
	public := &testcontainers.DockerNetwork{Name: "public"}
	cluster := &testcontainers.DockerNetwork{Name: "cluster"}
	t.Run("already removed", func(t *testing.T) {
		n := networkState{public: public, cluster: cluster, publicRemoved: true, clusterRemoved: true}
		if errs := n.removeBridges(t.Context(), nil); len(errs) != 0 || !n.publicRemoved || !n.clusterRemoved {
			t.Fatalf("errs %v, state %+v", errs, n)
		}
	})
	t.Run("absent", func(t *testing.T) {
		var n networkState
		if errs := n.removeBridges(t.Context(), nil); len(errs) != 0 || n.publicRemoved || n.clusterRemoved {
			t.Fatalf("errs %v, state %+v", errs, n)
		}
	})
	t.Run("blocked", func(t *testing.T) {
		n := networkState{public: public, cluster: cluster}
		blocked := map[string]bool{"public": true, "cluster": true}
		if errs := n.removeBridges(t.Context(), blocked); len(errs) != 0 || n.publicRemoved || n.clusterRemoved {
			t.Fatalf("a blocked bridge was removed or recorded: errs %v, state %+v", errs, n)
		}
	})
}

// TestNetworkStateRemovalFlagsHaveOneWriter guards the lock contract written on
// networkState: the removal flags are assigned only by removeBridges (plus the
// unit-test builders), and removeBridges is called only by terminate, after
// terminate took the topology lock. A flag written or a removal started
// elsewhere would run outside Container.mu.
func TestNetworkStateRemovalFlagsHaveOneWriter(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	allowedWriters := map[string]bool{"removeBridges": true, "TestSetNetworkRemoved": true /* test hook; its doc names the lock contract */}
	var problems []string
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			lockedAt, removeAt := token.NoPos, token.NoPos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				var targets []ast.Expr
				switch s := n.(type) {
				case *ast.AssignStmt:
					targets = s.Lhs
				case *ast.IncDecStmt:
					targets = []ast.Expr{s.X}
				case *ast.UnaryExpr:
					if s.Op == token.AND {
						targets = []ast.Expr{s.X}
					}
				case *ast.CallExpr:
					if sel, ok := s.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "removeBridges":
							if removeAt == token.NoPos {
								removeAt = s.Pos()
							}
							if !isContainerTerminate(fn) {
								problems = append(problems, "removeBridges called from "+fn.Name.Name)
							}
						case "lockTopology":
							if lockedAt == token.NoPos {
								lockedAt = s.Pos()
							}
						}
					}
				}
				for _, target := range targets {
					if sel, ok := target.(*ast.SelectorExpr); ok && (sel.Sel.Name == "publicRemoved" || sel.Sel.Name == "clusterRemoved") && !allowedWriters[fn.Name.Name] {
						problems = append(problems, "removal flag written in "+fn.Name.Name)
					}
				}
				return true
			})
			if isContainerTerminate(fn) && (removeAt == token.NoPos || lockedAt == token.NoPos || lockedAt > removeAt) {
				problems = append(problems, "terminate must take lockTopology before removeBridges")
			}
		}
	}
	if len(problems) != 0 {
		t.Fatalf("network removal state escapes its lock contract: %v", problems)
	}
}

// isContainerTerminate reports whether fn is the Container.terminate method.
func isContainerTerminate(fn *ast.FuncDecl) bool {
	if fn.Name.Name != "terminate" || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	id, ok := typ.(*ast.Ident)
	return ok && id.Name == "Container"
}
