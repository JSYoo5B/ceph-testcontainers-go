package cephfs

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// setupTopologyOps are the four operations that take the setup gate and then
// the topology mutex before their own ownership predicate runs: filesystem
// removal, MDS scaling, stopped-MDS removal and MDS replacement. Each runs the
// real function over the replacement fixture.
func setupTopologyOps(r *replacementMDSFixture) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"remove-filesystem": func(ctx context.Context) error { return removeFilesystem(ctx, r.fs.cluster, r.fs) },
		"scale-mds":         func(ctx context.Context) error { return r.fs.ScaleMDS(ctx, 1, 0) },
		"remove-stopped":    func(ctx context.Context) error { return r.fs.removeStoppedMDS(ctx, r.old) },
		"replace-mds": func(ctx context.Context) error {
			_, err := r.fs.addMDSReplacement(ctx, r.old, func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				return nil, errors.New("must not start")
			})
			return err
		},
	}
}

func setupGateFree(r *replacementMDSFixture) bool {
	mu := &filesystems(r.fs.cluster).setupMu
	if !mu.TryLock() {
		return false
	}
	mu.Unlock()
	return true
}

func topologyFree(t *testing.T, r *replacementMDSFixture) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := servicehook.LockTopology(ctx, r.fs.cluster); err != nil {
		return false
	}
	servicehook.Unlock(r.fs.cluster)
	return true
}

// TestSetupThenTopologyAcquisitionKeepsItsOrderAndRelease characterizes the
// shared prefix of the four operations: a held setup gate stops the operation
// before the topology mutex is touched, a held topology mutex stops it after
// the setup gate was taken and the gate is released again, a deadline is
// reported as the context error, and after an operation that reaches its own
// refusal neither lock is retained. It is a Docker-free control, not native
// evidence.
func TestSetupThenTopologyAcquisitionKeepsItsOrderAndRelease(t *testing.T) {
	names := []string{"remove-filesystem", "scale-mds", "remove-stopped", "replace-mds"}
	for _, name := range names {
		t.Run(name+"/setup-gate-held", func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			mu := &filesystems(r.fs.cluster).setupMu
			mu.Lock()
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err := setupTopologyOps(r)[name](ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("held setup gate: want the deadline error, got %v", err)
			}
			if !topologyFree(t, r) {
				t.Fatal("the topology mutex was taken or retained while the setup gate was held elsewhere")
			}
			mu.Unlock()
			if !setupGateFree(r) {
				t.Fatal("the setup gate is not free after its holder released it")
			}
		})
		t.Run(name+"/topology-held", func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			if err := servicehook.LockTopology(t.Context(), r.fs.cluster); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err := setupTopologyOps(r)[name](ctx)
			servicehook.Unlock(r.fs.cluster)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("held topology mutex: want the deadline error, got %v", err)
			}
			if !setupGateFree(r) {
				t.Fatal("the setup gate was retained after the topology mutex could not be taken")
			}
			if !topologyFree(t, r) {
				t.Fatal("the topology mutex is not free after its holder released it")
			}
		})
		t.Run(name+"/canceled-before-start", func(t *testing.T) {
			r := newReplacementMDSFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := setupTopologyOps(r)[name](ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled context: want the cancellation error, got %v", err)
			}
			if !setupGateFree(r) || !topologyFree(t, r) {
				t.Fatal("a lock is retained after a canceled call")
			}
		})
	}
	t.Run("refusals-release-both-locks", func(t *testing.T) {
		r := newReplacementMDSFixture(t)
		stale := &Filesystem{cluster: r.fs.cluster, FilesystemName: "target", name: "target", config: r.fs.config}
		refused := map[string]error{
			"remove-filesystem": removeFilesystem(t.Context(), r.fs.cluster, stale),
			"scale-mds":         stale.ScaleMDS(t.Context(), 1, 0),
		}
		for name, err := range refused {
			if err == nil {
				t.Fatalf("%s: a stale handle was admitted", name)
			}
			if !setupGateFree(r) || !topologyFree(t, r) {
				t.Fatalf("%s: a lock is retained after the refusal %v", name, err)
			}
		}
	})
}

// TestSetupThenTopologyPrefixHasOneOwnerAndKeepsPredicatesLocal pins the caller
// table and the division of work of lockSetupThenTopology: it is called only by
// the four pinned operations, each releases the setup gate only through the
// returned function (deferred, once) and never unlocks setupMu directly, the
// helper takes setupMu and then the topology mutex and nothing else, reads no
// ownership state and imports no sync, and the only functions that take both
// locks themselves are the helper and admitOperation.
func TestSetupThenTopologyPrefixHasOneOwnerAndKeepsPredicatesLocal(t *testing.T) {
	pinned := []string{"ScaleMDS", "addMDSReplacement", "removeFilesystem", "removeStoppedMDS"}
	var callers []string
	both := map[string][]string{}
	var helper *ast.FuncDecl
	for _, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "lockSetupThenTopology" {
				helper = fn
			}
			deferred, direct := 0, 0
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CallExpr:
					if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "lockSetupThenTopology" {
						callers = append(callers, fn.Name.Name)
					}
					if fn.Name.Name != "lockSetupThenTopology" {
						if sel, ok := node.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Unlock" && isSetupMuSelector(sel.X) {
							direct++
						}
					}
					if fn.Name.Name == "admitOperation" || fn.Name.Name == "lockSetupThenTopology" {
						break
					}
					switch {
					case isSetupMuAcquire(node) && admissionCallee(node) == "LockMutex":
						both[fn.Name.Name] = append(both[fn.Name.Name], "setupMu")
					case admissionCallee(node) == "LockTopology":
						both[fn.Name.Name] = append(both[fn.Name.Name], "topology")
					}
				case *ast.DeferStmt:
					if id, ok := node.Call.Fun.(*ast.Ident); ok && id.Name == "release" {
						deferred++
					}
				}
				return true
			})
			if slices.Contains(pinned, fn.Name.Name) {
				if deferred != 1 || direct != 0 {
					t.Errorf("%s: want one deferred release and no direct setupMu unlock, has %d and %d", fn.Name.Name, deferred, direct)
				}
			}
		}
	}
	slices.Sort(callers)
	if !slices.Equal(callers, pinned) {
		t.Errorf("caller table changed: %v, pinned %v", callers, pinned)
	}
	// Functions that still take the locks themselves are listed so a fifth copy
	// of the prefix cannot appear unnoticed.
	for name, locks := range both {
		if slices.Contains(locks, "setupMu") && slices.Contains(locks, "topology") && name != "startFilesystem" {
			t.Errorf("%s takes setupMu and the topology mutex itself; use lockSetupThenTopology", name)
		}
	}
	if helper == nil {
		t.Fatal("lockSetupThenTopology is missing")
	}
	var order []string
	ast.Inspect(helper.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			switch {
			case isSetupMuAcquire(node):
				order = append(order, "setupMu")
			case admissionCallee(node) == "LockTopology":
				order = append(order, "topology")
			}
		case *ast.SelectorExpr:
			if slices.Contains([]string{"byName", "nativeIdentity", "FilesystemName", "MetadataPool", "DataPool", "config", "roster", "Closed"}, node.Sel.Name) {
				t.Errorf("the helper reads %s: ownership predicates stay in each operation", node.Sel.Name)
			}
		}
		return true
	})
	if !slices.Equal(order, []string{"setupMu", "topology"}) {
		t.Errorf("helper acquisition order changed: %v", order)
	}
	src, err := parser.ParseFile(token.NewFileSet(), "setup_topology_lock.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range src.Imports {
		if strings.Contains(imp.Path.Value, `"sync`) {
			t.Error("the helper adds a mutex type")
		}
	}
}

func isSetupMuSelector(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "setupMu"
}
