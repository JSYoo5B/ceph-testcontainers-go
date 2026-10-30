package ceph

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestExtensionStoreWithoutStatesIsInert(t *testing.T) {
	var s extensionStore
	if got := s.inKindOrder(); len(got) != 0 {
		t.Fatalf("an empty store returned %v", got)
	}
	if got := extensionsWith[serviceSecrets](&Container{}); len(got) != 0 {
		t.Fatalf("an empty cluster returned %v", got)
	}
}

func TestExtensionStoreCreatesEachKindOnce(t *testing.T) {
	var s extensionStore
	created := 0
	create := func() any { created++; return new(int) }
	first := s.getOrCreate("kind", create)
	if again := s.getOrCreate("kind", create); again != first || created != 1 {
		t.Fatalf("the second call created again: created=%d", created)
	}
	if other := s.getOrCreate("other", create); other == first || created != 2 {
		t.Fatalf("another kind shared the state: created=%d", created)
	}
}

// TestExtensionStoreRunsCreateWithTheLockHeld pins the contract written on the
// type: a concurrent caller of another kind waits for a running create.
func TestExtensionStoreRunsCreateWithTheLockHeld(t *testing.T) {
	var s extensionStore
	entered, release := make(chan struct{}), make(chan struct{})
	go s.getOrCreate("slow", func() any { close(entered); <-release; return 1 })
	<-entered
	done := make(chan struct{})
	go func() { s.getOrCreate("fast", func() any { return 2 }); close(done) }()
	select {
	case <-done:
		t.Fatal("a second kind was created while the first create was running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the second kind never completed")
	}
}

func TestExtensionStoreOrdersByKindAndSnapshots(t *testing.T) {
	var s extensionStore
	for _, kind := range []string{"rgw", "cephfs", "a"} {
		s.getOrCreate(kind, func() any { return kind })
	}
	got := s.inKindOrder()
	if !slices.Equal(got, []any{"a", "cephfs", "rgw"}) {
		t.Fatalf("inKindOrder = %v", got)
	}
	s.getOrCreate("z", func() any { return "z" })
	if len(got) != 3 {
		t.Fatal("a later addition changed an earlier snapshot")
	}
}

func TestExtensionsWithFiltersByInterfaceInKindOrder(t *testing.T) {
	c := &Container{extensions: extensionStore{states: map[string]any{
		"rgw":    testExtension{secrets: []string{"s3"}},
		"cephfs": testExtension{secrets: []string{"fs"}},
		"plain":  struct{}{},
	}}}
	got := extensionsWith[serviceSecrets](c)
	if len(got) != 2 || got[0].Secrets()[0] != "fs" || got[1].Secrets()[0] != "s3" {
		t.Fatalf("extensionsWith = %v", got)
	}
}

func TestExtensionStoreConcurrentUse(t *testing.T) {
	var s extensionStore
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				s.getOrCreate([]string{"a", "b", "c", "d"}[i%4], func() any { return i })
				_ = s.inKindOrder()
			}
		}()
	}
	wg.Wait()
	if len(s.inKindOrder()) != 4 {
		t.Fatal("concurrent callers lost a kind")
	}
}

// TestServiceHooksExtensionReachesTheStore pins that the hook and the store
// are the same state, and that the store is the only place that keeps it.
func TestServiceHooksExtensionReachesTheStore(t *testing.T) {
	c := &Container{}
	state := serviceHooks{}.Extension(c, "kind", func() any { return new(testExtension) })
	if got := c.extensions.inKindOrder(); len(got) != 1 || got[0] != state {
		t.Fatalf("the hook did not store its state: %v", got)
	}
	other := &Container{}
	if got := (serviceHooks{}).Extension(other, "kind", func() any { return new(testExtension) }); got == state {
		t.Fatal("independent clusters share service state")
	}
}

func TestNetworkRemovedHooksReachTheFlag(t *testing.T) {
	c := &Container{}
	hooks := unitAccess{}
	if hooks.UnitNetworkRemoved(c) {
		t.Fatal("a new cluster has its bridge marked removed")
	}
	hooks.UnitSetNetworkRemoved(c, true)
	if !hooks.UnitNetworkRemoved(c) || !c.nets.publicRemoved || c.nets.clusterRemoved {
		t.Fatalf("flags = %+v", c.nets)
	}
	hooks.UnitSetNetworkRemoved(c, false)
	if hooks.UnitNetworkRemoved(c) {
		t.Fatal("the flag was not cleared")
	}
}

// extensionStoreMethods pins the component: the calls each method makes.
// Only the lock calls on its own mutex, the create callback and the builtins
// appear; none calls into Container or takes another lock.
var extensionStoreMethods = map[string][]string{
	"getOrCreate": {"s.mu.Lock()", "s.mu.Unlock()", "make(map[string]any)", "create()"},
	"inKindOrder": {"s.mu.Lock()", "s.mu.Unlock()", "slices.Sorted(maps.Keys(s.states))", "maps.Keys(s.states)", "make([]any, 0, len(kinds))", "len(kinds)", "append(states, s.states[kind])"},
}

// extensionStoreCallers lists every function outside the component that calls
// one of its methods. Both methods take the leaf lock themselves, so a caller
// may or may not hold Container.mu (the order is mu, then the store lock): the
// rows are "free".
var extensionStoreCallers = map[string]stateCaller{
	"Extension":      {"free", []string{"getOrCreate"}},
	"extensionsWith": {"free", []string{"inKindOrder"}},
}

func isExtensionStoreReceiver(fn *ast.FuncDecl) bool {
	return receiverTypeName(fn) == "extensionStore"
}

func extensionsUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "extensions" }

// TestContainerDeclaresNoExtensionsMapOrLock pins that the former
// extensionsMu is gone from Container and that the store has exactly its two
// fields.
func TestContainerDeclaresNoExtensionsMapOrLock(t *testing.T) {
	got := structFieldNames(t, "ceph.go", "Container")
	if slices.Contains(got, "extensionsMu") {
		t.Fatal("Container still declares extensionsMu")
	}
	if !slices.Contains(got, "extensions") {
		t.Fatal("Container does not declare the extension store")
	}
	if fields := structFieldNames(t, "extension_store.go", "extensionStore"); !slices.Equal(fields, []string{"mu", "states"}) {
		t.Fatalf("extensionStore fields = %v", fields)
	}
}

// TestExtensionStoreFieldIsOnlyTouchedByItsMethods guards the ownership written
// on extensionStore: outside this type, Container.extensions is only the
// receiver of one of its methods, its map is never reached directly, and no
// production composite literal of Container sets it.
func TestExtensionStoreFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.extensions", use: extensionsUse, own: isExtensionStoreReceiver, methods: extensionStoreMethods, literalField: "extensions",
		counted: func(_ *ast.FuncDecl, sel *ast.SelectorExpr) bool { return isContainerReceiver(sel.X) }}.check(t)
}

// TestExtensionStoreMethodsMakeOnlyTheirPinnedCalls guards the lock contract of
// the type: the method set is pinned, each method makes exactly the calls
// listed, and no method names Container, closed or another mutex.
func TestExtensionStoreMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "extensionStore", own: isExtensionStoreReceiver, methods: extensionStoreMethods,
		render:    func(call *ast.CallExpr) string { return types.ExprString(call) },
		forbidden: []string{"Container", "closed", "controlPlane", "settings", "bootstrap"}, forbiddenMembers: []string{"mu", "controlMu", "configMu", "extensionsMu"}, self: "s"}.check(t)
}

// TestExtensionStoreMapWritersAreAudited pins every production function that
// assigns the states map or one of its keys: only getOrCreate, and it takes the
// lock first.
func TestExtensionStoreMapWritersAreAudited(t *testing.T) {
	got := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			writes := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				as, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for _, lhs := range as.Lhs {
					if ix, ok := lhs.(*ast.IndexExpr); ok {
						lhs = ix.X
					}
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "states" {
						writes = true
					}
				}
				return true
			})
			if !writes {
				continue
			}
			name := fn.Name.Name
			got[name] = true
			switch {
			case name != "getOrCreate" || !isExtensionStoreReceiver(fn):
				problems = append(problems, name+" writes the extension map but is not getOrCreate")
			case !callsMethod(fn, "mu", "Lock"):
				problems = append(problems, name+" must take the lock around its write")
			}
		}
	}
	if !got["getOrCreate"] {
		problems = append(problems, "getOrCreate is listed as the writer but no longer writes the map")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("extension map writers changed: %v", problems)
	}
}

// TestExtensionStoreCallersAreAudited is the caller-table guard: the functions
// that use the component and the method each calls are pinned, no caller
// publishes the lock identity (the store has no mutex accessor and TestState
// does not mention it), and a stale row fails.
func TestExtensionStoreCallersAreAudited(t *testing.T) {
	callerAudit{label: "the extension store", use: extensionsUse, own: isExtensionStoreReceiver, callers: extensionStoreCallers}.check(t)
}

// TestExtensionStoreSourceIsInTheRightFile pins the file of the component so a
// copy under another name cannot hide a second definition.
func TestExtensionStoreSourceIsInTheRightFile(t *testing.T) {
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.TYPE {
				for _, spec := range gen.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.Name == "extensionStore" && filepath.Base(file) != "extension_store.go" {
						t.Fatalf("extensionStore is declared in %s", file)
					}
				}
			}
		}
	}
}
