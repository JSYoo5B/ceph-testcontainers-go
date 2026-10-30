package ceph

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestAuthStateFences(t *testing.T) {
	var a authState
	if a.fence("osd.0") != nil {
		t.Fatal("a new state has a fence")
	}
	fence := a.fenceFor("osd.0")
	if fence == nil || a.fenceFor("osd.0") != fence || a.fence("osd.0") != fence {
		t.Fatal("fenceFor must create the fence once and return it again")
	}
	if a.fence("client.app") != nil {
		t.Fatal("a fence of another entity appeared")
	}
	a.dropFence("osd.0")
	if a.fence("osd.0") != nil {
		t.Fatal("dropFence kept the fence")
	}
	a.dropFence("never-existed") // Dropping an absent fence is not an error.
}

func TestAuthStateKeysAndSecrets(t *testing.T) {
	var a authState
	if _, ok := a.confirmedKey("client.app"); ok {
		t.Fatal("a new state confirms a key")
	}
	a.confirmKey("client.app", "AQold")
	a.confirmKey("client.app", "AQnew")
	if key, ok := a.confirmedKey("client.app"); !ok || key != "AQnew" {
		t.Fatalf("confirmedKey = %q, %t", key, ok)
	}
	// A deleted identity is forgotten, but its keys stay for redaction.
	a.confirmKey("client.app", "")
	if _, ok := a.confirmedKey("client.app"); ok {
		t.Fatal("a deleted identity keeps its key")
	}
	a.addSecret("")
	a.addSecret("AQattempted")
	if got, want := a.secretValues(), []string{"AQattempted", "AQnew", "AQold"}; !slices.Equal(got, want) {
		t.Fatalf("secretValues = %v, want %v: replaced, attempted and deleted keys must stay", got, want)
	}
	var empty authState
	empty.confirmKey("client.app", "") // Deleting from a state without keys is not an error.
	if got := empty.secretValues(); len(got) != 0 {
		t.Fatalf("an empty state redacts %v", got)
	}
}

func TestAuthStateStraysKeepFilesThatDockerMayStillWrite(t *testing.T) {
	var a authState
	if got := a.strayPaths(); len(got) != 0 {
		t.Fatalf("a new state lists %v", got)
	}
	a.addStray("/tmp/b", false)
	a.addStray("/tmp/a", true)
	a.addStray("/tmp/b", false)
	a.addStray("/tmp/c", true)
	a.addStray("/tmp/c", false) // A later non-copying record must not clear the copying flag.
	if got, want := a.strayPaths(), []string{"/tmp/a", "/tmp/b", "/tmp/c"}; !slices.Equal(got, want) {
		t.Fatalf("strayPaths = %v, want %v", got, want)
	}
	a.forgetRemovedStrays()
	if got, want := a.strayPaths(), []string{"/tmp/a", "/tmp/c"}; !slices.Equal(got, want) {
		t.Fatalf("after a confirmed removal strayPaths = %v, want %v: a file whose copy may still complete stays listed", got, want)
	}
	var empty authState
	empty.forgetRemovedStrays() // Forgetting in a state without files is not an error.
}

func TestAuthStateRevocations(t *testing.T) {
	var a authState
	first, second := &clientRevocationState{}, &clientRevocationState{}
	if a.revocation("client.app") != nil {
		t.Fatal("a new state has a revocation")
	}
	a.setRevocation("client.app", first)
	if a.revocation("client.app") != first {
		t.Fatal("the revocation was not recorded")
	}
	a.dropRevocationIf("client.app", second)
	if a.revocation("client.app") != first {
		t.Fatal("dropRevocationIf removed a revocation that is not the given one")
	}
	a.dropRevocationIf("client.app", first)
	if a.revocation("client.app") != nil {
		t.Fatal("dropRevocationIf kept the matching revocation")
	}
	a.setRevocation("client.app", second)
	a.dropRevocation("client.app")
	a.dropRevocation("client.app")
	if a.revocation("client.app") != nil {
		t.Fatal("dropRevocation kept the revocation")
	}
}

// The state has no lock: the callers' Container.mu serializes it. The test
// runs several callers one after another under a lock of their own, as the
// real ones do, so -race reports any access that escapes it.
func TestAuthStateUnderTheCallersLockIsRaceFree(t *testing.T) {
	var (
		mu sync.Mutex
		a  authState
		wg sync.WaitGroup
	)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			entity := "client." + string(rune('a'+i%3))
			a.fenceFor(entity).clients = append(a.fenceFor(entity).clients, "f")
			a.confirmKey(entity, "AQ"+entity)
			a.addStray("/tmp/"+entity, i%2 == 0)
			a.setRevocation(entity, &clientRevocationState{})
			_ = a.secretValues()
			_ = a.strayPaths()
			a.forgetRemovedStrays()
			a.dropRevocation(entity)
			a.dropFence(entity)
		}()
	}
	wg.Wait()
}

// authStateMethods are the methods of authState with the exact calls each one
// makes. None locks, and none calls into Container.
var authStateMethods = map[string][]string{
	"fence":               nil,
	"fenceFor":            {"make(map[string]*authFence)"},
	"dropFence":           {"delete(a.fences, entity)"},
	"confirmedKey":        nil,
	"confirmKey":          {"delete(a.keys, entity)", "make(map[string]string)", "a.addSecret(key)"},
	"addSecret":           {"make(map[string]bool)"},
	"secretValues":        {"slices.Sorted(maps.Keys(a.secrets))", "maps.Keys(a.secrets)"},
	"addStray":            {"make(map[string]bool)"},
	"strayPaths":          {"slices.Sorted(maps.Keys(a.strays))", "maps.Keys(a.strays)"},
	"forgetRemovedStrays": {"maps.DeleteFunc(a.strays, (func(_ string, copying bool) bool literal))"},
	"revocation":          nil,
	"setRevocation":       {"make(map[string]*clientRevocationState)"},
	"dropRevocation":      {"delete(a.revocations, entity)"},
	"dropRevocationIf":    {"delete(a.revocations, entity)"},
}

// TestAuthStateFieldsAreOnlyTouchedByItsMethods guards the ownership written
// on authState: outside this type, the records (in authLedgerState, the
// Container field ledger) are used only as the receiver of one of its methods, so no
// code reads or writes one of the five record maps without the method that
// does, and they are never replaced as a whole. Container.ledger itself is
// addressed only by Container.authLedger's single binding.
func TestAuthStateFieldsAreOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses, bindings := 0, 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) == "authState" {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			// Container.authLedger binds the records to the ledger, the one
			// place that hands out their address; the ledger's methods are
			// held to this guard like any other function.
			binding := funcKey(fn) == "Container.authLedger"
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok {
						if _, known := authStateMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok {
								allowed[recv] = true
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.SelectorExpr:
					if n.Sel.Name == "authRecords" {
						if allowed[n] {
							uses++
						} else {
							problems = append(problems, file+": "+fn.Name.Name+" uses Container.authRecords outside an authState method call")
						}
					}
				case *ast.KeyValueExpr:
					if key, ok := n.Key.(*ast.Ident); ok && (key.Name == "authRecords" || key.Name == "authLedgerState") {
						if address, ok := n.Value.(*ast.UnaryExpr); binding && ok && address.Op == token.AND && key.Name == "authLedgerState" {
							if sel, ok := address.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "ledger" {
								bindings++
								allowed[sel] = true
								return true
							}
						}
						problems = append(problems, file+": "+fn.Name.Name+" sets "+key.Name+" in a literal")
					}
				}
				return true
			})
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.authRecords found: the guard is vacuous")
	}
	if bindings != 1 {
		problems = append(problems, fmt.Sprintf("Container.authLedger binds the records %d times, want once", bindings))
	}
	if len(problems) != 0 {
		t.Fatalf("auth state escapes its methods: %v", problems)
	}
}

// TestAuthStateMethodsMakeOnlyTheirPinnedCalls guards the contract of the
// type: the method set is pinned and each method makes exactly its listed
// calls, so none takes a lock, none calls into Container and the records stay
// guarded by the caller's Container.mu alone.
func TestAuthStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) != "authState" {
				continue
			}
			name := fn.Name.Name
			want, known := authStateMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update authStateMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					got = append(got, types.ExprString(call))
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range authStateMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	// The state declares no lock and no field outside the five records.
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.Name == "authState" {
					var fields []string
					for _, field := range ts.Type.(*ast.StructType).Fields.List {
						for _, name := range field.Names {
							fields = append(fields, name.Name)
						}
					}
					if want := []string{"fences", "secrets", "keys", "strays", "revocations"}; !slices.Equal(fields, want) {
						problems = append(problems, "authState fields are "+join(fields)+", pinned "+join(want))
					}
				}
			}
		}
	}
	if len(problems) != 0 {
		t.Fatalf("authState breaks its lock contract: %v", problems)
	}
}

// authOwnerHeld lists the production functions that use Container.authRecords, or
// call another function of this list, without taking Container.mu themselves.
// Each one documents "The caller holds c.mu" (or the cluster mutex), and the
// guard below proves that all of its callers either take Container.mu first or
// are in this list too. A function is named "Receiver.name", or "name" for a
// function without receiver. The value is where the contract is written.
var authOwnerHeld = map[string]string{
	"Container.authLedger":                "auth_state.go: authLedger, whose ledger the caller uses while it holds c.mu",
	"authLedger.settleClient":             "client_key.go: settleClient",
	"authLedger.changeClient":             "client_key.go: changeClient",
	"Container.ownedClientRecord":         "client_key.go: ownedClientRecord",
	"authLedger.checkCurrentClient":       "client_key.go: checkCurrentClient",
	"authLedger.removeStrayAuthInputs":    "daemon_key_rotation.go: removeStrayAuthInputs",
	"authLedger.stopEarlierAuthCommands":  "daemon_key_rotation.go: stopEarlierAuthCommands",
	"authLedger.settleEarlierRotation":    "daemon_key_rotation.go: settleEarlierRotation",
	"authLedger.daemonExec":               "daemon_key_rotation.go: daemonExec, reached from rotateDaemonKey",
	"authLedger.placeDaemonKeyring":       "daemon_key_rotation.go: placeDaemonKeyring, reached from rotateDaemonKey through writeDaemonKeyrings",
	"authLedger.writeDaemonKeyrings":      "daemon_key_rotation.go: reached from rotateDaemonKey",
	"daemonRotation.finishEarlierHandoff": "daemon_key_rotation.go: daemonRotation, whose phases run in rotateDaemonKey's critical section",
	"daemonRotation.abandon":              "daemon_key_rotation.go: daemonRotation",
	"daemonRotation.stageKeyFile":         "daemon_key_rotation.go: daemonRotation",
	"daemonRotation.handOff":              "daemon_key_rotation.go: daemonRotation",
	"daemonRotation.commit":               "daemon_key_rotation.go: daemonRotation",
	"authCall.send":                       "daemon_key_rotation.go: authCall, whose methods the caller of changeClient or rotateDaemonKey runs",
	"authCall.mutate":                     "daemon_key_rotation.go: authCall",
	"authCall.keys":                       "daemon_key_rotation.go: authCall",
	"authCall.settle":                     "daemon_key_rotation.go: authCall",
	"authCall.removeInput":                "daemon_key_rotation.go: authCall",
	"serviceHooks.SetClientRevoked":       "service_hooks.go: SetClientRevoked; its callers are in the cephfs package, guarded by TestAuthHookCallersInCephFSHoldTheClusterMutex",
	"serviceHooks.SettleClient":           "service_hooks.go: SettleClient; same callers",
}

// funcKey names a function declaration as authOwnerHeld does.
func funcKey(fn *ast.FuncDecl) string {
	if recv := receiverTypeName(fn); recv != "" {
		return recv + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// containerMuEvents returns the positions at which fn takes Container.mu and
// the positions at which it releases it outside a defer. It recognizes
// lockTopology, X.mu.Lock and X.mu.TryLock, the diagnostics form that passes
// X.mu.TryLock to a helper, and X.mu.Unlock.
func containerMuEvents(fn *ast.FuncDecl) (acquires, releases []token.Pos) {
	deferred := map[*ast.CallExpr]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if d, ok := n.(*ast.DeferStmt); ok {
			deferred[d.Call] = true
		}
		return true
	})
	isMu := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		return ok && sel.Sel.Name == "mu"
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
			switch {
			case sel.Sel.Name == "lockTopology":
				acquires = append(acquires, call.Pos())
			case isMu(sel.X) && (sel.Sel.Name == "Lock" || sel.Sel.Name == "TryLock"):
				acquires = append(acquires, call.Pos())
			case isMu(sel.X) && sel.Sel.Name == "Unlock" && !deferred[call]:
				releases = append(releases, call.Pos())
			}
		}
		for _, arg := range call.Args {
			if sel, ok := arg.(*ast.SelectorExpr); ok && isMu(sel.X) && sel.Sel.Name == "TryLock" {
				acquires = append(acquires, call.Pos())
			}
		}
		return true
	})
	return acquires, releases
}

// heldAt reports whether Container.mu is held at pos, going by the order of
// the function's acquisitions and its explicit releases.
func heldAt(pos token.Pos, acquires, releases []token.Pos) bool {
	lastAcquire, lastRelease := token.NoPos, token.NoPos
	for _, at := range acquires {
		if at < pos && at > lastAcquire {
			lastAcquire = at
		}
	}
	for _, at := range releases {
		if at < pos && at > lastRelease {
			lastRelease = at
		}
	}
	return lastAcquire != token.NoPos && lastAcquire > lastRelease
}

// TestAuthRecordsAreOnlyUsedWithTheClusterMutexHeld pins the lock of the
// records. Every use of Container.authRecords, and every call of a function in
// authOwnerHeld, happens in a function that either takes Container.mu before
// the use and has not released it, or is itself in authOwnerHeld. The list is
// closed under callers, so a caller chain that reaches the records without
// Container.mu fails here. Closures are judged by the function that contains
// them, and an authCall is only built in a function of the same two kinds.
func TestAuthRecordsAreOnlyUsedWithTheClusterMutexHeld(t *testing.T) {
	var problems []string
	declared := map[string]bool{}
	names := map[string][]string{}
	reached := map[string]bool{}
	files := parseProductionFiles(t)
	for _, parsed := range files {
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				declared[funcKey(fn)] = true
				names[fn.Name.Name] = append(names[fn.Name.Name], funcKey(fn))
			}
		}
	}
	// The call-site match is by name, so a pinned name must not belong to a
	// second function.
	pinnedNames := map[string]bool{}
	for key := range authOwnerHeld {
		if !declared[key] {
			problems = append(problems, key+" is pinned but not declared")
			continue
		}
		name := key[strings.LastIndex(key, ".")+1:]
		if strings.HasPrefix(key, "authCall.") {
			continue // matched through the construction rule below
		}
		pinnedNames[name] = true
		if len(names[name]) != 1 {
			problems = append(problems, name+" is declared "+join(names[name])+": a call by name is ambiguous")
		}
	}
	totalUses := 0
	for file, parsed := range files {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) == "authState" {
				continue
			}
			key := funcKey(fn)
			acquires, releases := containerMuEvents(fn)
			var uses []token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.SelectorExpr:
					if n.Sel.Name == "authRecords" || n.Sel.Name == "ledger" && isContainerReceiver(n.X) {
						uses = append(uses, n.Pos())
					}
				case *ast.CallExpr:
					name := ""
					switch f := n.Fun.(type) {
					case *ast.SelectorExpr:
						name = f.Sel.Name
					case *ast.Ident:
						name = f.Name
					}
					if pinnedNames[name] {
						uses = append(uses, n.Pos())
					}
				case *ast.CompositeLit:
					if id, ok := n.Type.(*ast.Ident); ok && id.Name == "authCall" {
						uses = append(uses, n.Pos())
					}
				}
				return true
			})
			if len(uses) == 0 {
				continue
			}
			totalUses += len(uses)
			if _, pinned := authOwnerHeld[key]; pinned {
				reached[key] = true
				continue
			}
			for _, at := range uses {
				if !heldAt(at, acquires, releases) {
					problems = append(problems, file+": "+key+" reaches the auth records without holding Container.mu; take the lock or add it to authOwnerHeld with its contract")
					break
				}
			}
		}
	}
	for key := range authOwnerHeld {
		if !reached[key] && !strings.HasPrefix(key, "authCall.") {
			problems = append(problems, key+" is pinned but uses no auth record and calls no pinned function: remove it from authOwnerHeld")
		}
	}
	if totalUses < 30 {
		problems = append(problems, "too few uses found: the guard is vacuous")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("auth records are used without the cluster mutex: %v", problems)
	}
}

// TestAuthHookCallersInCephFSHoldTheClusterMutex pins the callers of the two
// hooks that reach the records from another package. A function of the cephfs
// package that calls them, or authorizationClientCaps which does, takes the
// cluster mutex first, unless it is authorizationClientCaps itself, whose
// contract is "must be called with cluster.mu held".
func TestAuthHookCallersInCephFSHoldTheClusterMutex(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "cephfs", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var problems []string
	hookCalls, holders := 0, 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var lockAt token.Pos
			var calls []token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.SelectorExpr:
					switch f.Sel.Name {
					case "LockTopology", "Lock", "TryLock":
						if pkg, ok := f.X.(*ast.Ident); ok && pkg.Name == "servicehook" && lockAt == token.NoPos {
							lockAt = call.Pos()
						}
					case "SettleClient", "SetClientRevoked", "authorizationClientCaps":
						calls = append(calls, call.Pos())
					}
				}
				return true
			})
			if len(calls) == 0 {
				continue
			}
			hookCalls += len(calls)
			if fn.Name.Name == "authorizationClientCaps" {
				continue
			}
			holders++
			for _, at := range calls {
				if lockAt == token.NoPos || at < lockAt {
					problems = append(problems, file+": "+fn.Name.Name+" reaches the client records without taking the cluster mutex first")
					break
				}
			}
		}
	}
	if hookCalls < 6 || holders < 3 {
		problems = append(problems, "too few callers found: the guard is vacuous")
	}
	if len(problems) != 0 {
		t.Fatalf("cephfs reaches the auth records unlocked: %v", problems)
	}
}
