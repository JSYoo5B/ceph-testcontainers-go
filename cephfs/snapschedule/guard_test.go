package snapschedule

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
)

// lockGuard is the type-checked production source of the package, with the
// objects that each top-level function refers to.
type lockGuard struct {
	fset  *token.FileSet
	pkg   *types.Package
	info  *types.Info
	decls map[*types.Func]*ast.FuncDecl
	// refs are the objects that each function's body refers to, its function
	// literals included.
	refs map[*types.Func][]types.Object
}

func loadLockGuard(t *testing.T) *lockGuard {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	g := &lockGuard{fset: token.NewFileSet(), decls: map[*types.Func]*ast.FuncDecl{}, refs: map[*types.Func][]types.Object{}}
	var files []*ast.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(g.fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	g.info = &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
	// The compiler's export data of the dependencies, which go list builds
	// or takes from the build cache, is much faster to import than source.
	out, err := exec.Command("go", "list", "-export", "-deps", "-f", "{{.ImportPath}}={{.Export}}", ".").Output()
	if err != nil {
		t.Fatalf("go list -export: %v", err)
	}
	exports := map[string]string{}
	for line := range strings.Lines(string(out)) {
		path, export, _ := strings.Cut(strings.TrimSpace(line), "=")
		exports[path] = export
	}
	conf := types.Config{Importer: importer.ForCompiler(g.fset, "gc", func(path string) (io.ReadCloser, error) {
		if exports[path] == "" {
			return nil, fmt.Errorf("no export data of %s", path)
		}
		return os.Open(exports[path])
	})}
	if g.pkg, err = conf.Check("github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule", g.fset, files, g.info); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fd.Recv == nil && fd.Name.Name == "init" {
				t.Errorf("%s declares init", g.fset.Position(fd.Pos()))
			}
			fn := g.info.Defs[fd.Name].(*types.Func)
			g.decls[fn] = fd
			if fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && g.info.Uses[id] != nil {
					g.refs[fn] = append(g.refs[fn], g.info.Uses[id])
				}
				return true
			})
		}
	}
	return g
}

// refers reports whether fn's body refers to obj.
func (g *lockGuard) refers(fn *types.Func, obj types.Object) bool {
	return slices.Contains(g.refs[fn], obj)
}

// referrers are the functions whose bodies refer to obj.
func (g *lockGuard) referrers(obj types.Object) []*types.Func {
	var result []*types.Func
	for fn := range g.decls {
		if g.refers(fn, obj) {
			result = append(result, fn)
		}
	}
	return result
}

func (g *lockGuard) name(fn *types.Func) string {
	if recv := fn.Signature().Recv(); recv != nil {
		return types.TypeString(recv.Type(), types.RelativeTo(g.pkg)) + "." + fn.Name()
	}
	return fn.Name()
}

// method returns the method name of the type typ of the imported package path.
func method(pkg *types.Package, path, typ, name string) types.Object {
	for _, imported := range pkg.Imports() {
		if imported.Path() == path {
			obj, _, _ := types.LookupFieldOrMethod(types.NewPointer(imported.Scope().Lookup(typ).Type()), true, imported, name)
			return obj
		}
	}
	return nil
}

// TestLeaseLocksAreTheTwoLeafMutexes checks the lock structure that the
// package's comments state, on the type-checked source: identifiers are
// resolved to the objects they denote, so a renamed receiver, field or
// function, a method value or a call through a wrapper is seen as what it is.
//
//   - The package's only sync values are two struct fields, one in Lease and
//     one in leaseSet, and it declares no init.
//   - The lease set's mutex is used only by leaseSet's methods, which call
//     nothing but the mutex and builtins: it is a leaf lock held for the map
//     update alone, never while a command runs.
//   - The lease's mutex is used only by Lease's methods, is taken only through
//     clusterutil.LockMutex, so a waiter gives up when its context ends, and
//     before the filesystem admission; nothing that a holder of the admission
//     reaches in this package takes it.
//   - Only one function refers to fsbase.Core.Admit, and none to
//     WithSetupGate. The functions that return the admission's done, the
//     wrappers, and their callers hold it. Every path from an exported or
//     unreferenced function to a context-taking function of another package
//     passes through one of them.
//
// The guard is a static reading of references, not a proof of runtime safety.
// It does not order calls within a function apart from the lease mutex before
// the admission, does not see that a function holds the admission for the
// whole of its body, follows no call through an interface or a function value
// that is not named in this package, and counts a reference in a function
// literal as one of the enclosing function, whenever the literal runs. The
// behavioral tests of Restore's context-bounded wait and concurrent calls,
// and the race detector, cover what it cannot.
func TestLeaseLocksAreTheTwoLeafMutexes(t *testing.T) {
	g := loadLockGuard(t)
	scope := g.pkg.Scope()
	lease, set := scope.Lookup("Lease").Type(), scope.Lookup("leaseSet").Type()

	// The sync values.
	mutexes := map[types.Type]*types.Var{}
	for id, obj := range g.info.Defs {
		v, ok := obj.(*types.Var)
		if !ok || !isSync(v.Type()) {
			continue
		}
		owner := fieldOwner(v, lease, set)
		if owner == nil || mutexes[owner] != nil {
			t.Errorf("%s: %s is a sync value outside the two lease mutexes", g.fset.Position(id.Pos()), v.Name())
			continue
		}
		mutexes[owner] = v
	}
	if mutexes[lease] == nil || mutexes[set] == nil {
		t.Fatalf("the lease mutexes are not found: %v", mutexes)
	}

	// The lease set's mutex.
	setUsers := g.referrers(mutexes[set])
	if len(setUsers) == 0 {
		t.Error("nothing locks the lease set: the guard is vacuous")
	}
	for _, fn := range setUsers {
		if !isMethodOf(fn, set) {
			t.Errorf("%s uses the lease set's mutex", g.name(fn))
		}
		for _, obj := range g.refs[fn] {
			if callee, ok := obj.(*types.Func); ok && (callee.Pkg() == nil || callee.Pkg().Path() != "sync") {
				t.Errorf("%s calls %s while it can hold the lease set", g.name(fn), callee.FullName())
			}
		}
	}

	// The admission: the one function that admits, the wrappers that return
	// its done, and the functions that hold it through them.
	admit := method(g.pkg, "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase", "Core", "Admit")
	if gate := method(g.pkg, "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase", "Core", "WithSetupGate"); gate != nil {
		for _, fn := range g.referrers(gate) {
			t.Errorf("%s takes the setup gate outside the admission", g.name(fn))
		}
	}
	admitters := g.referrers(admit)
	if len(admitters) != 1 || !returnsDone(admitters[0]) {
		t.Fatalf("Admit is referred to by %v, want one function that returns its done", admitters)
	}
	wrappers := map[*types.Func]bool{admitters[0]: true}
	for grown := true; grown; {
		grown = false
		for fn := range g.decls {
			if !wrappers[fn] && returnsDone(fn) && slices.ContainsFunc(g.refs[fn], func(o types.Object) bool { return wrappers[funcOf(o)] }) {
				wrappers[fn], grown = true, true
			}
		}
	}
	admitted := map[*types.Func]bool{}
	for fn := range g.decls {
		if wrappers[fn] || slices.ContainsFunc(g.refs[fn], func(o types.Object) bool { return wrappers[funcOf(o)] }) {
			admitted[fn] = true
		}
	}

	// The lease's mutex: taken through LockMutex before the admission.
	lockMutex := scopeFunc(g.pkg, "github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil", "LockMutex")
	unlock := method(g.pkg, "sync", "Mutex", "Unlock")
	leaseLockers := map[*types.Func]bool{}
	for _, fn := range g.referrers(mutexes[lease]) {
		leaseLockers[fn] = true
		if !isMethodOf(fn, lease) {
			t.Errorf("%s uses a lease's mutex", g.name(fn))
		}
		var locked token.Pos
		ast.Inspect(g.decls[fn].Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.SelectorExpr:
				if sel, ok := g.info.Uses[n.Sel].(*types.Var); !ok || sel != mutexes[lease] {
					return true
				}
				if !g.leaseMutexUse(n, fn, lockMutex, unlock) {
					t.Errorf("%s: %s uses a lease's mutex other than through clusterutil.LockMutex and Unlock", g.fset.Position(n.Pos()), g.name(fn))
				}
			case *ast.CallExpr:
				if callee := funcOf(g.calleeObject(n)); callee != nil {
					switch {
					case callee == lockMutex && locked == token.NoPos:
						locked = n.Pos()
					case wrappers[callee] && (locked == token.NoPos || n.Pos() < locked):
						t.Errorf("%s takes the admission before its lease's mutex", g.name(fn))
					}
				}
			}
			return true
		})
	}
	if len(leaseLockers) == 0 {
		t.Error("nothing locks a lease: the guard is vacuous")
	}
	// What the holders reach in this package runs under the admission.
	under := map[*types.Func]bool{}
	var hold func(fn *types.Func)
	hold = func(fn *types.Func) {
		if under[fn] || g.decls[fn] == nil {
			return
		}
		under[fn] = true
		for _, obj := range g.refs[fn] {
			if callee := funcOf(obj); callee != nil && callee.Pkg() == g.pkg {
				hold(callee)
			}
		}
	}
	for fn := range admitted {
		hold(fn)
	}
	for fn := range under {
		for _, obj := range g.refs[fn] {
			if callee := funcOf(obj); callee != nil && callee != fn && leaseLockers[callee] {
				t.Errorf("%s waits for a lease's mutex under the admission through %s", g.name(fn), g.name(callee))
			}
		}
	}

	// Every path to another package's context-taking function passes through
	// the admission. internal/watch only runs the read it is given and
	// clusterutil.LockMutex only takes a mutex.
	referenced := map[*types.Func]bool{}
	for fn := range g.decls {
		for _, obj := range g.refs[fn] {
			if callee := funcOf(obj); callee != nil && callee != fn {
				referenced[callee] = true
			}
		}
	}
	holders := 0
	visited := map[*types.Func]bool{}
	var walk func(fn *types.Func, path []string)
	walk = func(fn *types.Func, path []string) {
		if visited[fn] || g.decls[fn] == nil {
			return
		}
		visited[fn] = true
		path = append(path, g.name(fn))
		if admitted[fn] {
			holders++
			return
		}
		for _, obj := range g.refs[fn] {
			callee := funcOf(obj)
			switch {
			case callee == nil:
			case callee.Pkg() == g.pkg:
				walk(callee, path)
			case takesContext(callee) && callee != lockMutex && callee.Pkg().Path() != "github.com/JSYoo5B/ceph-testcontainers-go/internal/watch":
				t.Errorf("%s reaches %s without the filesystem admission", strings.Join(path, " -> "), callee.FullName())
			}
		}
	}
	for fn := range g.decls {
		if fn.Exported() || !referenced[fn] {
			walk(fn, nil)
		}
	}
	if holders == 0 {
		t.Error("no operation takes the admission: the guard is vacuous")
	}
}

// leaseMutexUse reports whether sel, a lease's mutex in fn, is &sel passed to
// clusterutil.LockMutex or the receiver of Unlock.
func (g *lockGuard) leaseMutexUse(sel *ast.SelectorExpr, fn *types.Func, lockMutex, unlock types.Object) bool {
	ok := false
	ast.Inspect(g.decls[fn].Body, func(n ast.Node) bool {
		call, isCall := n.(*ast.CallExpr)
		if !isCall {
			return true
		}
		switch g.calleeObject(call) {
		case lockMutex:
			for _, arg := range call.Args {
				if u, isAddr := arg.(*ast.UnaryExpr); isAddr && u.Op == token.AND && u.X == sel {
					ok = true
				}
			}
		case unlock:
			if s, isSel := call.Fun.(*ast.SelectorExpr); isSel && s.X == sel {
				ok = true
			}
		}
		return true
	})
	return ok
}

// calleeObject is the object that call's function expression names.
func (g *lockGuard) calleeObject(call *ast.CallExpr) types.Object {
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		return g.info.Uses[fun]
	case *ast.SelectorExpr:
		return g.info.Uses[fun.Sel]
	}
	return nil
}

func funcOf(obj types.Object) *types.Func {
	fn, _ := obj.(*types.Func)
	if fn != nil {
		return fn.Origin()
	}
	return nil
}

func scopeFunc(pkg *types.Package, path, name string) types.Object {
	for _, imported := range pkg.Imports() {
		if imported.Path() == path {
			return imported.Scope().Lookup(name)
		}
	}
	return nil
}

func isSync(typ types.Type) bool {
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	named, ok := typ.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "sync"
}

// fieldOwner returns the one of owners whose struct declares v.
func fieldOwner(v *types.Var, owners ...types.Type) types.Type {
	if !v.IsField() {
		return nil
	}
	for _, owner := range owners {
		st := owner.Underlying().(*types.Struct)
		for field := range st.Fields() {
			if field == v {
				return owner
			}
		}
	}
	return nil
}

func isMethodOf(fn *types.Func, typ types.Type) bool {
	recv := fn.Signature().Recv()
	if recv == nil {
		return false
	}
	t := recv.Type()
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	return types.Identical(t, typ)
}

// returnsDone reports whether fn returns a func(), as the admission's done.
func returnsDone(fn *types.Func) bool {
	for result := range fn.Signature().Results().Variables() {
		if sig, ok := result.Type().(*types.Signature); ok && sig.Params().Len() == 0 && sig.Results().Len() == 0 {
			return true
		}
	}
	return false
}

func takesContext(fn *types.Func) bool {
	params := fn.Signature().Params()
	return params.Len() > 0 && types.TypeString(params.At(0).Type(), nil) == "context.Context"
}

// A directory holds one lease per filesystem: the same path of another
// filesystem of the cluster, or of a filesystem recreated under the same
// name, is another key.
func TestLeaseSetKeysDirectoriesByFilesystem(t *testing.T) {
	fake := newFakeSnapSchedule()
	first := snapScheduleFilesystem(fake)
	set := leasesOf(first.Cluster())
	recreated := fsbase.NewCore(fsbase.Params{Cluster: first.Cluster(), Name: "tc"})
	a := &Lease{owned: ownedSchedule{path: "/"}, filesystem: first, leases: set}
	b := &Lease{owned: ownedSchedule{path: "/"}, filesystem: recreated, leases: set}
	if !set.reserve(a) || !set.reserve(b) || set.reserve(&Lease{owned: ownedSchedule{path: "/"}, filesystem: first, leases: set}) {
		t.Fatal("leases are not kept per filesystem and directory")
	}
	if leasesOf(first.Cluster()) != set {
		t.Fatal("the cluster's lease set is not stable")
	}
	a.release()
	if !set.reserve(&Lease{owned: ownedSchedule{path: "/"}, filesystem: first, leases: set}) || set.paths[leaseKey{recreated, "/"}] != b {
		t.Fatal("release freed another filesystem's directory or kept its own")
	}
}
