package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestFeatureClientStateLifecycle(t *testing.T) {
	var f featureClientState
	if err := f.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.startupAttempted() || f.startupReady() || f.current() != nil {
		t.Fatal("a new state claims a startup")
	}
	daemon := &monitorLifecycleDaemon{}
	f.markAttempted()
	f.publish(daemon) // A partial start is published before its outcome is known.
	if got := f.current(); got != daemon || f.startupReady() {
		t.Fatalf("partial start: current %v, ready %t", got, f.startupReady())
	}
	f.markReady(true)
	f.clear()
	if f.current() != nil || f.startupReady() || !f.startupAttempted() {
		t.Fatalf("clear: current %v, ready %t, attempted %t: a cleared state must not allow another startup",
			f.current(), f.startupReady(), f.startupAttempted())
	}
	f.unlock()
	if got, err := f.snapshot(t.Context()); got != nil || err != nil {
		t.Fatalf("snapshot after clear = %v, %v", got, err)
	}
}

func TestFeatureClientStateLockHonorsContext(t *testing.T) {
	var f featureClientState
	if err := f.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := f.lock(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a held startup lock ignored the caller's deadline: %v", err)
	}
	f.unlock()
	if err := f.lock(t.Context()); err != nil {
		t.Fatalf("the lock was lost: %v", err)
	}
	f.unlock()
}

// Readers of the published client do not wait for a startup: snapshot and the
// diagnostics read need only publishMu, not mu.
func TestFeatureClientStateReadersDoNotWaitForStartup(t *testing.T) {
	var f featureClientState
	daemon := &monitorLifecycleDaemon{}
	if err := f.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.publish(daemon)
	defer f.unlock() // The startup lock stays held while the readers run.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if got, err := f.snapshot(ctx); got != daemon || err != nil {
		t.Fatalf("snapshot waited for or lost the startup: %v, %v", got, err)
	}
	if got, err := f.diagnosticSnapshot(ctx); got != daemon || err != nil {
		t.Fatalf("diagnostic snapshot waited for or lost the startup: %v, %v", got, err)
	}
}

func TestFeatureClientStateDiagnosticSnapshotBoundsItsWait(t *testing.T) {
	var f featureClientState
	f.publishMu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if got, err := f.diagnosticSnapshot(ctx); got != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a blocked publication = %v, %v", got, err)
	}
	f.publishMu.Unlock()
	if _, err := f.diagnosticSnapshot(t.Context()); err != nil {
		t.Fatalf("free publication: %v", err)
	}
}

// startup, termination and every reader run against each other under -race.
func TestFeatureClientStateConcurrentUseIsRaceFree(t *testing.T) {
	var f featureClientState
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if f.lock(t.Context()) != nil {
				return
			}
			defer f.unlock()
			if !f.startupAttempted() {
				f.markAttempted()
				f.publish(&monitorLifecycleDaemon{})
				f.markReady(true)
			} else if i%2 == 0 {
				f.current()
				f.clear()
			}
		}()
		go func() {
			defer wg.Done()
			_, _ = f.snapshot(t.Context())
			_, _ = f.diagnosticSnapshot(t.Context())
		}()
	}
	wg.Wait()
}

// terminableToolkit returns a cluster whose Terminate runs without Docker and
// whose feature client is daemon, started and ready.
func terminableToolkit(daemon *monitorLifecycleDaemon) *Container {
	c := terminableFacts()
	c.toolkit.attempted, c.toolkit.ready, c.toolkit.client = true, true, daemon
	return c
}

func TestTerminateRemovesTheFeatureClientAndKeepsItRetryableOnFailure(t *testing.T) {
	daemon := &monitorLifecycleDaemon{terminateErr: errors.New("remove failed")}
	c := terminableToolkit(daemon)
	if err := c.Terminate(t.Context()); err == nil {
		t.Fatal("a failed removal of the feature client was not reported")
	}
	if got, _ := c.featureClientSnapshot(t.Context()); got != daemon || daemon.terminations != 1 || !c.toolkit.ready {
		t.Fatalf("a client that could not be removed was forgotten: %v, %d terminations, ready %t",
			got, daemon.terminations, c.toolkit.ready)
	}
	daemon.terminateErr = nil
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, _ := c.featureClientSnapshot(t.Context()); got != nil || daemon.terminations != 2 || c.toolkit.ready || !c.toolkit.attempted {
		t.Fatalf("after the retry: client %v, %d terminations, ready %t, attempted %t",
			got, daemon.terminations, c.toolkit.ready, c.toolkit.attempted)
	}
	if got, err := c.ClientContainer(t.Context()); got != nil || !errors.Is(err, errClusterTerminated) {
		t.Fatalf("a terminated cluster started a client: %v, %v", got, err)
	}
}

// featureClientMethods are the methods of featureClientState with the exact
// calls each one makes, receiver included, and whether the caller holds mu.
var featureClientMethods = map[string]struct {
	calls  []string
	needMu bool
}{
	"lock":               {calls: []string{"lockTopologyMutex(ctx, &f.mu)"}},
	"unlock":             {calls: []string{"f.mu.Unlock()"}},
	"startupAttempted":   {needMu: true},
	"startupReady":       {needMu: true},
	"current":            {needMu: true},
	"markAttempted":      {needMu: true},
	"markReady":          {needMu: true},
	"publish":            {calls: []string{"f.publishMu.Lock()", "f.publishMu.Unlock()"}, needMu: true},
	"clear":              {calls: []string{"f.publish(nil)"}, needMu: true},
	"snapshot":           {calls: []string{"lockTopologyReadMutex(ctx, &f.publishMu)", "f.publishMu.RUnlock()"}},
	"diagnosticSnapshot": {calls: []string{"diagnosticLock(ctx, f.publishMu.TryRLock)", "f.publishMu.RUnlock()"}},
}

// TestFeatureClientStateFieldsAreOnlyTouchedByItsMethods guards the ownership
// written on featureClientState: outside this type, Container.toolkit is used
// only as the receiver of one of its methods, so no code reads or writes a
// field, or takes either lock, without the method that does.
func TestFeatureClientStateFieldsAreOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) == "featureClientState" {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok {
						if _, known := featureClientMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok {
								allowed[recv] = true
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "toolkit" {
					if allowed[sel] {
						uses++
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" uses Container.toolkit outside a featureClientState method call")
					}
				}
				return true
			})
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.toolkit found: the guard is vacuous")
	}
	if len(problems) != 0 {
		t.Fatalf("feature client state escapes its methods: %v", problems)
	}
}

// TestFeatureClientStateMethodsMakeOnlyTheirPinnedCalls guards the lock
// contract of the type: the method set is pinned, each method makes exactly
// the calls listed, and none calls into Container, so publishMu stays a leaf
// and mu is taken only by lock.
func TestFeatureClientStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) != "featureClientState" {
				continue
			}
			name := fn.Name.Name
			want, known := featureClientMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update featureClientMethods")
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
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want.calls))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want.calls))
			}
		}
	}
	for name := range featureClientMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		t.Fatalf("featureClientState breaks its lock contract: %v", problems)
	}
}

func join(items []string) string {
	out := "["
	for i, item := range items {
		if i > 0 {
			out += "; "
		}
		out += item
	}
	return out + "]"
}

// isTopologyAcquire reports whether call takes Container.mu.
func isTopologyAcquire(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if sel.Sel.Name == "lockTopology" {
		return true
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "mu" && (sel.Sel.Name == "Lock" || sel.Sel.Name == "TryLock")
}

// TestFeatureClientStateCallersHoldTheStartupLockInOrder guards the callers:
// a function that uses a "mu held" method has taken toolkit.lock before and
// releases it after, never takes Container.mu once it holds it, and Terminate
// takes the topology lock before the startup lock. Only terminate and
// clientContainerContext may hold it.
func TestFeatureClientStateCallersHoldTheStartupLockInOrder(t *testing.T) {
	holders := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) == "featureClientState" {
				continue
			}
			var lockAt, topologyAt token.Pos
			var needs, unlocks, topologyAfterLock []token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if isTopologyAcquire(call) {
					if topologyAt == token.NoPos {
						topologyAt = call.Pos()
					}
					topologyAfterLock = append(topologyAfterLock, call.Pos())
				}
				method, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				recv, ok := method.X.(*ast.SelectorExpr)
				if !ok || recv.Sel.Name != "toolkit" {
					return true
				}
				switch {
				case method.Sel.Name == "lock":
					if lockAt == token.NoPos {
						lockAt = call.Pos()
					}
				case method.Sel.Name == "unlock":
					unlocks = append(unlocks, call.Pos())
				case featureClientMethods[method.Sel.Name].needMu:
					needs = append(needs, call.Pos())
				}
				return true
			})
			if lockAt == token.NoPos && len(needs) == 0 {
				continue
			}
			holders[fn.Name.Name] = true
			if lockAt == token.NoPos {
				problems = append(problems, fn.Name.Name+" uses a mu-held method without toolkit.lock")
				continue
			}
			if len(unlocks) == 0 {
				problems = append(problems, fn.Name.Name+" never calls toolkit.unlock")
			}
			for _, at := range needs {
				if at < lockAt {
					problems = append(problems, fn.Name.Name+" uses a mu-held method before toolkit.lock")
				}
			}
			for _, at := range topologyAfterLock {
				if at > lockAt {
					problems = append(problems, fn.Name.Name+" takes Container.mu after toolkit.lock: the order is Container.mu, then the startup lock")
				}
			}
			if fn.Name.Name == "terminate" && (topologyAt == token.NoPos || topologyAt > lockAt) {
				problems = append(problems, "terminate must hold the topology lock before it takes toolkit.lock")
			}
		}
	}
	if len(holders) != 2 || !holders["terminate"] || !holders["clientContainerContext"] {
		problems = append(problems, "the holders of the startup lock changed: "+join(slices.Sorted(maps.Keys(holders))))
	}
	if len(problems) != 0 {
		t.Fatalf("feature client lock discipline: %v", problems)
	}
}
