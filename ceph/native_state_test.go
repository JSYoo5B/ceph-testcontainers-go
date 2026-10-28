package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestNativeStateAdmitChecksClosedThenFenceThenClaim(t *testing.T) {
	claims := 0
	refuse := func() bool { claims++; return false }
	grant := func() bool { claims++; return true }
	inv := &nativeInvocation{targetID: "t"}

	var n nativeState
	if got := n.admit(inv, nil); got != nativeAdmitted || n.registered() != 1 {
		t.Fatalf("plain admit = %v, registered %d", got, n.registered())
	}
	n.forget(inv)
	if n.registered() != 0 {
		t.Fatal("forget kept the registration")
	}
	if got := n.admit(inv, refuse); got != nativeRefusedClaim || n.registered() != 0 || claims != 1 {
		t.Fatalf("claim refusal = %v, registered %d, claims %d", got, n.registered(), claims)
	}
	if got := n.admit(inv, grant); got != nativeAdmitted || n.registered() != 1 || claims != 2 {
		t.Fatalf("claim grant = %v, registered %d, claims %d", got, n.registered(), claims)
	}
	n.forget(inv)

	n.fence("t")
	if got := n.admit(inv, grant); got != nativeRefusedFenced || claims != 2 || n.registered() != 0 {
		t.Fatalf("fenced = %v, claims %d: the claim ran before the fence check", got, claims)
	}
	if got := n.admit(&nativeInvocation{targetID: "other"}, nil); got != nativeAdmitted {
		t.Fatalf("a fence for one instance refused another: %v", got)
	}

	n.close()
	if got := n.admit(inv, grant); got != nativeRefusedClosed || claims != 2 {
		t.Fatalf("closed = %v, claims %d: closed must win over the fence and claim", got, claims)
	}
	if !n.isClosed() {
		t.Fatal("close did not close admission")
	}
}

func TestNativeStateFinishKeepsUnconfirmedInvocations(t *testing.T) {
	var n nativeState
	kept, dropped, emptyMarker := &nativeInvocation{}, &nativeInvocation{}, &nativeInvocation{}
	for _, inv := range []*nativeInvocation{kept, dropped, emptyMarker} {
		if n.admit(inv, nil) != nativeAdmitted {
			t.Fatal("admit refused")
		}
	}
	n.finish(kept, "marker", true)
	n.finish(dropped, "", false)
	// An unconfirmed end without a marker stays registered, as before the move.
	n.finish(emptyMarker, "", true)
	if kept.marker != "marker" || dropped.marker != "" || n.registered() != 2 {
		t.Fatalf("kept %q dropped %q registered %d", kept.marker, dropped.marker, n.registered())
	}
	if pending, _ := n.close(); !slices.Contains(pending, kept) || !slices.Contains(pending, emptyMarker) || slices.Contains(pending, dropped) {
		t.Fatalf("pending after finish: %v", pending)
	}
}

func TestNativeStateCloseAndFenceReturnWhatToCancel(t *testing.T) {
	var n nativeState
	a, b := &nativeInvocation{targetID: "a"}, &nativeInvocation{targetID: "b"}
	n.admit(a, nil)
	n.admit(b, nil)

	pending := n.fence("a")
	if len(pending) != 1 || pending[0] != a || n.isClosed() {
		t.Fatalf("fence returned %v, closed %t: it must name only its target and leave admission open", pending, n.isClosed())
	}
	if !n.beginClientStartup(func() {}) {
		t.Fatal("an open state refused a client startup")
	}
	all, cancel := n.close()
	if len(all) != 2 || cancel == nil {
		t.Fatalf("close returned %d invocations, cancel %t", len(all), cancel != nil)
	}
	if n.beginClientStartup(func() {}) {
		t.Fatal("a closed state accepted a client startup")
	}
	n.endClientStartup()
	if _, cancel := n.close(); cancel != nil {
		t.Fatal("endClientStartup left the cancel function")
	}
}

// The state is shared by RunNativeCommand, the debug-level window, the client
// startup and Terminate. This runs them against each other under -race.
func TestNativeStateConcurrentUseIsRaceFree(t *testing.T) {
	var n nativeState
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inv := &nativeInvocation{targetID: string(rune('a' + i%2))}
			if n.admit(inv, nil) == nativeAdmitted {
				n.finish(inv, "m", i%3 == 0)
			}
			n.fence("a")
			n.registered()
			n.isClosed()
			if n.beginClientStartup(func() {}) {
				n.endClientStartup()
			}
		}()
	}
	n.close()
	wg.Wait()
}

// nativeStateMethods are the Container-visible entry points of nativeState.
var nativeStateMethods = map[string]bool{
	"admit": true, "finish": true, "forget": true, "registered": true, "isClosed": true,
	"close": true, "fence": true, "beginClientStartup": true, "endClientStartup": true,
}

// parseProductionFiles parses the non-test files of this package.
func parseProductionFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*ast.File{}
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
	return out
}

// receiverTypeName returns the type name of a method receiver, or "".
func receiverTypeName(fn *ast.FuncDecl) string {
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

// TestNativeStateFieldsAreOnlyTouchedByItsMethods guards the ownership written
// on nativeState: outside this type, Container.nativeCmds is used only as the
// receiver of one of its methods, so no code reads or writes a field, or takes
// the lock, without the method that does.
func TestNativeStateFieldsAreOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) == "nativeState" {
				continue
			}
			// Selectors that are the called method of a nativeState method.
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok && nativeStateMethods[method.Sel.Name] {
						if recv, ok := method.X.(*ast.SelectorExpr); ok {
							allowed[recv] = true
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "nativeCmds" && !allowed[sel] {
					problems = append(problems, file+": "+fn.Name.Name+" uses Container.nativeCmds outside a nativeState method call")
				}
				return true
			})
		}
	}
	if len(problems) != 0 {
		t.Fatalf("native state escapes its methods: %v", problems)
	}
}

// TestNativeStateMethodsHoldOnlyTheLeafLock guards the lock-order contract:
// every method that touches a field takes the state's own mu, and no method
// calls anything but the lock, the claim hook and builtins or the two
// collection helpers while it runs, so the lock stays a leaf below
// Container.mu and the feature-client mu and never nests another lock.
func TestNativeStateMethodsHoldOnlyTheLeafLock(t *testing.T) {
	var methods int
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) != "nativeState" {
				continue
			}
			methods++
			locks := 0
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident: // builtins and the claim hook
					if !slices.Contains([]string{"make", "delete", "len", "claim", "append"}, fun.Name) {
						problems = append(problems, fn.Name.Name+" calls "+fun.Name)
					}
				case *ast.SelectorExpr:
					name := fun.Sel.Name
					switch name {
					case "Lock", "Unlock":
						// Only the state's own mutex: n.mu.
						inner, ok := fun.X.(*ast.SelectorExpr)
						if !ok || inner.Sel.Name != "mu" {
							problems = append(problems, fn.Name.Name+" locks something other than n.mu")
						}
						if name == "Lock" {
							locks++
						}
					case "Collect", "Keys":
					default:
						problems = append(problems, fn.Name.Name+" calls "+name)
					}
				default:
					problems = append(problems, fn.Name.Name+" has an indirect call")
				}
				return true
			})
			if locks != 1 {
				problems = append(problems, fn.Name.Name+" must take n.mu exactly once")
			}
		}
	}
	if methods != len(nativeStateMethods) {
		problems = append(problems, "the method set changed; update nativeStateMethods and review the lock contract")
	}
	if len(problems) != 0 {
		t.Fatalf("nativeState breaks its leaf-lock contract: %v", problems)
	}
}

// nativeStateMutexOwners is every mutex that Container and its state
// components declare, by owning type and field. The native split moved
// nativeMu into nativeState, the feature-client split moved clientMu and
// clientPublishMu into featureClientState, and the control-plane split moved
// controlMu into controlPlaneState (the same lock, renamed `mu` in its owner). The auth-records, OSD storage,
// run-directory, OSD registry, port-lease and fault-registry splits added no
// locks. debugLevelRegistry has always had its own leaf lock (the registry
// type predates the split); earlier versions of this test did not scan it,
// so the count of seven understated the real eight by that one. A new
// entry needs its own lock-order review.
var nativeStateMutexOwners = []string{
	"Container.mu", "Container.configMu", "Container.extensionsMu",
	"controlPlaneState.mu",
	"nativeState.mu",
	"featureClientState.mu", "featureClientState.publishMu",
	"debugLevelRegistry.mu",
}

// nativeStateComponents are the types scanned for mutexes: Container and the
// state components that were moved out of it, plus the registries it holds.
var nativeStateComponents = []string{
	"Container", "nativeState", "networkState", "featureClientState", "authState", "osdStorageState",
	"runDirState", "osdRegistry", "portLeaseState", "faultRegistry", "debugLevelRegistry",
	"controlPlaneState",
}

// TestNativeStateAddsNoMutex compares the mutexes of Container and its state
// components with nativeStateMutexOwners: exactly eight, each named by owner.
func TestNativeStateAddsNoMutex(t *testing.T) {
	var got []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok || !slices.Contains(nativeStateComponents, ts.Name.Name) {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, field := range st.Fields.List {
					sel, ok := field.Type.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sync" && (sel.Sel.Name == "Mutex" || sel.Sel.Name == "RWMutex") {
						if len(field.Names) == 0 {
							got = append(got, ts.Name.Name+".(embedded "+sel.Sel.Name+")")
						}
						for _, name := range field.Names {
							got = append(got, ts.Name.Name+"."+name.Name)
						}
					}
				}
			}
		}
	}
	slices.Sort(got)
	want := slices.Sorted(slices.Values(nativeStateMutexOwners))
	if !slices.Equal(got, want) {
		t.Fatalf("Container and its state components declare mutexes %v, want %v", got, want)
	}
}

// TestTerminateCancelsNativeInvocationsBeforeTheTopologyLock guards the order
// that lets Terminate run while a caller holds Container.mu inside a native
// command: the leaf lock is taken and the commands are canceled first.
func TestTerminateCancelsNativeInvocationsBeforeTheTopologyLock(t *testing.T) {
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			cancelAt, lockAt := token.NoPos, token.NoPos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "cancelNativeInvocations":
							if cancelAt == token.NoPos {
								cancelAt = call.Pos()
							}
						case "lockTopology":
							if lockAt == token.NoPos {
								lockAt = call.Pos()
							}
						}
					}
				}
				return true
			})
			if cancelAt == token.NoPos || lockAt == token.NoPos || cancelAt > lockAt {
				t.Fatal("terminate must call cancelNativeInvocations before lockTopology")
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
