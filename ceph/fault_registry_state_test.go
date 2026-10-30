package ceph

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"slices"
	"strings"
	"testing"
)

// faultRegistryMethods pins the faultRegistry component: the calls each
// method makes. Only undo takes a lock operation, on the sync.Locker it
// receives; none reads a Container field or calls into Container.
var faultRegistryMethods = map[string][]string{
	"reserve":           {"previous.complete(ctx)", "spec.refuse", "spec.refuse", "spec.refuse", "make"},
	"settle":            {"delete"},
	"release":           {"delete"},
	"tracks":            nil,
	"holds":             nil,
	"handles":           {"make"},
	"openInstanceFault": {"strings.HasSuffix", "entry.complete(ctx)"},
	"held":              {"append"},
	"undo":              {"r.held", "mu.Unlock", "entry.undo(ctx)", "mu.Lock", "entry.undo(ctx)", "append", "fmt.Errorf", "make"},
	// The two checks that need only the registry; the caller holds Container.mu.
	"refuseInstanceFaults":   {"r.openInstanceFault", "fmt.Errorf"},
	"poolDeleteOverrideHeld": {"r.holds", "configOverrideKey"},
}

func isFaultRegistryReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "faultRegistry" }

func faultsUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "faults" }

func faultsUseOf(x ast.Expr) bool {
	sel, ok := x.(*ast.SelectorExpr)
	return ok && faultsUse(sel)
}

// faultCallName renders a call for the pin table; a call whose only argument
// is ctx is written with it.
func faultCallName(call *ast.CallExpr) string {
	name := types.ExprString(call.Fun)
	if len(call.Args) == 1 && types.ExprString(call.Args[0]) == "ctx" {
		return name + "(ctx)"
	}
	return name
}

// TestFaultRegistryFieldIsOnlyTouchedByItsMethods guards the ownership
// written on faultRegistry: outside the type, Container.faults is used only
// as the receiver of one of its methods, and nothing reads the entries map,
// takes the address of the registry or of the map, or ranges over it.
func TestFaultRegistryFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isFaultRegistryReceiver(fn) {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok {
						if _, known := faultRegistryMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && faultsUse(recv) {
								allowed[recv] = true
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.SelectorExpr:
					if faultsUse(x) {
						if allowed[x] {
							uses++
						} else {
							problems = append(problems, file+": "+fn.Name.Name+" uses Container.faults outside a faultRegistry method call")
						}
					}
					if inner, ok := x.X.(*ast.SelectorExpr); ok && x.Sel.Name == "entries" && faultsUse(inner) {
						problems = append(problems, file+": "+fn.Name.Name+" reads the entries map")
					}
				case *ast.UnaryExpr:
					if x.Op == token.AND {
						ast.Inspect(x.X, func(m ast.Node) bool {
							if sel, ok := m.(*ast.SelectorExpr); ok && (faultsUse(sel) || (sel.Sel.Name == "entries" && faultsUseOf(sel.X))) {
								problems = append(problems, file+": takes the address of "+types.ExprString(x.X))
							}
							return true
						})
					}
				}
				return true
			})
			if file == "cluster_facts.go" && strings.Contains(fn.Name.Name, "Facts") {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && faultsUse(sel) {
						problems = append(problems, "FactsOf source reads Container.faults")
					}
					return true
				})
			}
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.faults found: the guard is vacuous")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("fault registry escapes its methods: %v", problems)
	}
}

// TestFaultRegistryMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned, each method makes exactly the calls
// listed, only undo operates on a lock and only through its sync.Locker
// parameter, and no method names Container, its closed flag or its mutex.
func TestFaultRegistryMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isFaultRegistryReceiver(fn) {
				continue
			}
			name := fn.Name.Name
			want, known := faultRegistryMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update faultRegistryMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, faultCallName(x))
				case *ast.SelectorExpr:
					switch x.Sel.Name {
					case "closed", "mu", "controlMu", "configMu":
						problems = append(problems, name+" reads "+types.ExprString(x))
					}
				case *ast.Ident:
					if x.Name == "Container" {
						problems = append(problems, name+" names Container")
					}
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range faultRegistryMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("faultRegistry breaks its lock contract: %v", problems)
	}
}

// faultDocs maps "Receiver.Name" (or "Name" for a function) to the
// lower-cased doc comment of each production function, so that the hook rows
// of the same name on faultHooks and overrideHooks are told apart.
func faultDocs(t *testing.T) map[string]string {
	t.Helper()
	docs := map[string]string{}
	for file := range parseProductionFiles(t) {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Doc != nil {
				key := fn.Name.Name
				if recv := receiverTypeName(fn); recv != "" {
					key = recv + "." + key
				}
				docs[key] = strings.ToLower(strings.Join(strings.Fields(fn.Doc.Text()), " "))
			}
		}
	}
	return docs
}

// faultRegistryCallers lists every function outside the component that calls
// one of its methods, with the methods it calls and how it is guarded:
//
//	"takes"  acquires Container.mu before its first use
//	"hook"   its doc comment says the caller holds the lock or the cluster is
//	         not shared
var faultRegistryCallers = map[string]struct {
	mode    string
	methods []string
}{
	"Container.terminate":   {"takes", []string{"undo"}},
	"Container.detachOSD":   {"takes", []string{"refuseInstanceFaults"}},
	"Container.deletePool":  {"hook", []string{"poolDeleteOverrideHeld"}},
	"faultHooks.Reserve":    {"hook", []string{"reserve"}},
	"faultHooks.Settle":     {"hook", []string{"settle"}},
	"faultHooks.Release":    {"hook", []string{"release"}},
	"faultHooks.Tracks":     {"hook", []string{"tracks"}},
	"unitAccess.UnitFaults": {"hook", []string{"handles"}},
	"overrideHooks.Reserve": {"hook", []string{"reserve"}},
	"overrideHooks.Settle":  {"hook", []string{"settle"}},
	"overrideHooks.Release": {"hook", []string{"release"}},
	"overrideHooks.Tracks":  {"hook", []string{"tracks"}},
	"unitAccess.UnitHeld":   {"hook", []string{"handles"}},
}

// TestFaultRegistryCallersAreAudited guards the callers: the set of functions
// that use the component and the methods each calls are pinned, a "takes"
// function acquires Container.mu before its first use, a "hook" function
// documents the contract, and a stale row fails. Container.mu is the whole
// lock domain of the type.
func TestFaultRegistryCallersAreAudited(t *testing.T) {
	docs := faultDocs(t)
	found := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isFaultRegistryReceiver(fn) {
				continue
			}
			methods := map[string]bool{}
			first := token.NoPos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && faultsUse(recv) {
						methods[sel.Sel.Name] = true
						if first == token.NoPos || call.Pos() < first {
							first = call.Pos()
						}
					}
				}
				return true
			})
			if len(methods) == 0 {
				continue
			}
			key := fn.Name.Name
			if recv := receiverTypeName(fn); recv != "" {
				key = recv + "." + key
			}
			found[key] = true
			row, known := faultRegistryCallers[key]
			if !known {
				problems = append(problems, key+" uses the fault registry but is not in faultRegistryCallers")
				continue
			}
			if at := osdRegistryAcquirePos(fn); row.mode == "takes" && (at == token.NoPos || at > first) {
				problems = append(problems, key+" must take Container.mu before using the fault registry")
			}
			if row.mode == "hook" && !strings.Contains(docs[key], "holds") {
				problems = append(problems, key+" is a hook row but its doc comment does not name the lock contract")
			}
			got := slices.Sorted(func(yield func(string) bool) {
				for m := range methods {
					if !yield(m) {
						return
					}
				}
			})
			if !slices.Equal(got, row.methods) {
				problems = append(problems, key+" calls "+join(got)+", pinned "+join(row.methods))
			}
		}
	}
	for key := range faultRegistryCallers {
		if !found[key] {
			problems = append(problems, key+" is listed but no longer uses the fault registry")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("fault registry callers break the lock contract: %v", problems)
	}
}

// TestTerminateUndoesFaultsFirstUnderTheClusterMutex guards the Terminate
// order of the fault phases: faults.undo runs once, after closed is set and
// before the native commands are stopped and the leases released, and it is
// handed the address of Container.mu (the one lock it may release around a
// shaping restore), not some other locker.
func TestTerminateUndoesFaultsFirstUnderTheClusterMutex(t *testing.T) {
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			var undo, stop, release, closedAt []token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					sel, ok := x.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch sel.Sel.Name {
					case "undo":
						if recv, ok := sel.X.(*ast.SelectorExpr); ok && faultsUse(recv) {
							undo = append(undo, x.Pos())
							if len(x.Args) != 2 || types.ExprString(x.Args[1]) != "&c.mu" {
								t.Fatalf("faults.undo must receive &c.mu, got %v", x.Args)
							}
						}
					case "stopNativeInvocations":
						stop = append(stop, x.Pos())
					case "release":
						if recv, ok := sel.X.(*ast.SelectorExpr); ok && portLeaseUse(recv) {
							release = append(release, x.Pos())
						}
					}
				case *ast.AssignStmt:
					if len(x.Lhs) == 1 && types.ExprString(x.Lhs[0]) == "c.closed" {
						closedAt = append(closedAt, x.Pos())
					}
				}
				return true
			})
			if len(undo) != 1 || len(stop) == 0 || len(release) != 1 || len(closedAt) == 0 {
				t.Fatalf("terminate must call faults.undo once and set closed: %d %d %d %d", len(undo), len(stop), len(release), len(closedAt))
			}
			if !(closedAt[0] < undo[0] && undo[0] < stop[0] && undo[0] < release[0]) {
				t.Fatal("terminate must undo the faults after closed is set and before the native commands and leases")
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}

// debugLevelRegistryMethods pins the debug-level window registry: each method
// takes its own leaf lock first, releases it by defer and calls nothing that
// takes another lock.
var debugLevelRegistryMethods = map[string][]string{
	"Reserve": {"r.mu.Lock", "r.mu.Unlock", "invalid", "errors.New", "make"},
	"Release": {"r.mu.Lock", "r.mu.Unlock", "delete"},
}

func isDebugLevelReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "debugLevelRegistry" }

// TestDebugLevelRegistryStaysALeafBehindItsOwnLock guards the leaf lock of the
// debug-level window registry: outside the type, Container.debugLevels is used
// only to hand the registry out through the override hook, nothing reads its
// map, each method takes its lock before its first access and defers the
// unlock, calls are pinned, and Terminate never touches it.
func TestDebugLevelRegistryStaysALeafBehindItsOwnLock(t *testing.T) {
	var problems []string
	uses := 0
	seen := map[string]bool{}
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if !isDebugLevelReceiver(fn) {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok {
						if sel.Sel.Name == "debugLevels" {
							uses++
							if receiverTypeName(fn) != "overrideHooks" || fn.Name.Name != "DebugWindows" {
								problems = append(problems, file+": "+fn.Name.Name+" uses Container.debugLevels")
							}
						}
						if sel.Sel.Name == "active" {
							problems = append(problems, file+": "+fn.Name.Name+" reads the active map")
						}
					}
					return true
				})
				continue
			}
			name := fn.Name.Name
			want, known := debugLevelRegistryMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract")
				continue
			}
			seen[name] = true
			var got []string
			lockAt, firstAccess := token.NoPos, token.NoPos
			deferred := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, types.ExprString(x.Fun))
					if types.ExprString(x.Fun) == "r.mu.Lock" && lockAt == token.NoPos {
						lockAt = x.Pos()
					}
				case *ast.DeferStmt:
					deferred = types.ExprString(x.Call.Fun) == "r.mu.Unlock"
				case *ast.SelectorExpr:
					if x.Sel.Name == "active" && (firstAccess == token.NoPos || x.Pos() < firstAccess) {
						firstAccess = x.Pos()
					}
				}
				return true
			})
			if lockAt == token.NoPos || firstAccess < lockAt || !deferred {
				problems = append(problems, name+" must take r.mu before touching active and defer the unlock")
			}
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range debugLevelRegistryMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if uses != 1 {
		problems = append(problems, "Container.debugLevels must be named exactly once, by overrideHooks.DebugWindows")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("debug-level registry breaks its leaf contract: %v", problems)
	}
}

// TestSettleHooksReportTheClusterClosedFlagWhateverTheTokenState pins the
// behavior that moved out of the registry: both Settle hooks report
// Container.closed after a live settle, a handle-less settle and a stale
// token alike, and never error for a token that a cluster issued.
func TestSettleHooksReportTheClusterClosedFlagWhateverTheTokenState(t *testing.T) {
	for _, closed := range []bool{false, true} {
		for _, tc := range []struct {
			name            string
			stale, noHandle bool
		}{{name: "live"}, {name: "handle-less", noHandle: true}, {name: "stale", stale: true}} {
			c := &Container{closed: closed}
			token, _, err := c.faults.reserve(context.Background(), faultBlocklist, "k")
			if err != nil {
				t.Fatal(err)
			}
			if tc.stale {
				c.faults.release(token)
			}
			var handle any = "h"
			if tc.noHandle {
				handle = nil
			}
			if got, err := (faultHooks{}).Settle(c, token, "", handle, nil, nil); err != nil || got != closed {
				t.Fatalf("faultHooks.Settle %s closed=%v: %v %v", tc.name, closed, got, err)
			}
			c = &Container{closed: closed}
			token, _, _ = c.faults.reserve(context.Background(), faultBlocklist, "k")
			if tc.stale {
				c.faults.release(token)
			}
			if got, err := (overrideHooks{}).Settle(c, token, handle); err != nil || got != closed {
				t.Fatalf("overrideHooks.Settle %s closed=%v: %v %v", tc.name, closed, got, err)
			}
		}
	}
	if _, err := (faultHooks{}).Settle(&Container{}, "foreign", "", "h", nil, nil); err == nil {
		t.Fatal("a foreign token was accepted")
	}
}

// TestFaultRegistryAccessorsReadWhatTheCallersNeed covers the accessors that
// replaced direct reads of the entries map.
func TestFaultRegistryAccessorsReadWhatTheCallersNeed(t *testing.T) {
	ctx := context.Background()
	var r faultRegistry
	if r.holds(faultConfigOverride, "k") || len(r.handles(faultConfigOverride)) != 0 {
		t.Fatal("an empty registry reports entries")
	}
	token, _, _ := r.reserve(ctx, faultConfigOverride, "k")
	if !r.holds(faultConfigOverride, "k") || r.holds(faultBlocklist, "k") {
		t.Fatal("holds must see a reservation of exactly its kind and key")
	}
	if h := r.handles(faultConfigOverride); len(h) != 1 || h["k"] != nil {
		t.Fatalf("a reservation has a nil handle: %v", h)
	}
	r.settle(token, "", "h", nil, nil)
	if h := r.handles(faultConfigOverride); h["k"] != "h" || len(r.handles(faultBlocklist)) != 0 {
		t.Fatalf("handles = %v", h)
	}
	// A pause and a delay target a container; a blocklist entry never does.
	delay, _, _ := r.reserve(ctx, faultNetworkDelay, "public/abc")
	r.settle(delay, "", "d", nil, func(context.Context) (bool, error) { return false, nil })
	if name, found, err := r.openInstanceFault(ctx, "abc"); !found || err != nil || name != "network delay" {
		t.Fatalf("openInstanceFault = %q %v %v", name, found, err)
	}
	if _, found, _ := r.openInstanceFault(ctx, "bc"); found {
		t.Fatal("a suffix that is not a whole container ID matched")
	}
	delay.complete = func(context.Context) (bool, error) { return true, nil }
	if _, found, err := r.openInstanceFault(ctx, "abc"); found || err != nil {
		t.Fatalf("a complete fault still blocks: %v %v", found, err)
	}
}
