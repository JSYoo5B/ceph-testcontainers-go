package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"testing"
)

// backendReach lists how the methods of containerBackend in src use the
// wrapped *Container (the field c of the receiver): every selected member
// as "<method>: <member>", and every other use of the pointer itself as
// "<method>: <pointer>". Comments are excluded.
func backendReach(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var reach []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 {
			continue
		}
		recv := fn.Recv.List[0].Names[0].Name
		isCluster := func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "c" {
				return false
			}
			id, ok := sel.X.(*ast.Ident)
			return ok && id.Name == recv
		}
		var stack []ast.Node
		ast.Inspect(fn, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			defer func() { stack = append(stack, n) }()
			if !isCluster(n) {
				return true
			}
			if len(stack) > 0 {
				if outer, ok := stack[len(stack)-1].(*ast.SelectorExpr); ok && outer.X == n {
					reach = append(reach, fn.Name.Name+": "+outer.Sel.Name)
					return true
				}
			}
			reach = append(reach, fn.Name.Name+": <pointer>")
			return true
		})
	}
	slices.Sort(reach)
	return slices.Compact(reach)
}

func TestContainerBackendReach(t *testing.T) {
	// containerBackend reaches the cluster through three exported methods
	// that wait for or create state, and purpose-named methods declared in
	// shared_cluster_port.go. It names no Container field and takes no lock.
	// The pointer itself goes to the registered service prepare and probe
	// functions, which take the cluster.
	want := []string{
		"createClient: createSharedClient",
		"createPool: CreatePool",
		"daemonsRunning: sharedDaemons",
		"deleteClient: deleteSharedClient",
		"invariants: runSharedScript",
		"prepareServices: <pointer>",
		"purgeNamespace: runSharedScript",
		"ready: WaitForPGClean",
		"ready: WaitForQuorum",
		"resources: <pointer>",
		"setSharing: setSharingMode",
		"terminate: terminate",
	}
	var got []string
	for _, file := range []string{"shared_backend.go", "shared_services.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, backendReach(t, file, string(src))...)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("containerBackend reaches the cluster through\n%q\nwant\n%q\nadd a purpose-named method to the Container side instead of reading a field", got, want)
	}
}

// Negative control: the checker reports field reads, lock use and a copied
// pointer, so a regression of the narrowing would fail the test above.
func TestBackendReachCheckerSeesARegression(t *testing.T) {
	bad := `package ceph
type containerBackend struct{ c *Container }
func (b containerBackend) deleteClient() { b.c.mu.Lock(); _ = b.c.closed }
func (b containerBackend) daemonsRunning() { c := b.c; _ = c.monitors }
func (b containerBackend) ok() { b.c.terminate() }
func (b containerBackend) free() { _ = b.cluster }
func helper(b containerBackend) { _ = b.c.mu }
`
	got := backendReach(t, "bad.go", bad)
	want := []string{
		"daemonsRunning: <pointer>",
		"deleteClient: mu",
		"deleteClient: closed",
		"ok: terminate",
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("checker found %q, want %q", got, want)
	}
}
