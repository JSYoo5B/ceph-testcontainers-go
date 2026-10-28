package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
)

// controlStateFake is a control container that records its removals.
type controlStateFake struct {
	testcontainers.Container
	id           string
	terminateErr error
	terminations int
}

func (f *controlStateFake) GetContainerID() string { return f.id }

func (f *controlStateFake) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	f.terminations++
	return f.terminateErr
}

func TestControlPlaneStateWithoutAHandleIsInert(t *testing.T) {
	var s controlPlaneState
	if s.current() != nil {
		t.Fatal("an empty control plane reports a handle")
	}
	if ctr, err := s.currentContext(t.Context()); ctr != nil || err != nil {
		t.Fatalf("currentContext = %v, %v", ctr, err)
	}
	if ctr, err := s.diagnosticHandle(t.Context()); ctr != nil || err != nil {
		t.Fatalf("diagnosticHandle = %v, %v", ctr, err)
	}
	if owned, err := s.owns(t.Context(), ""); owned || err != nil {
		t.Fatalf("an empty control plane owns the empty ID: %v, %v", owned, err)
	}
	if err := s.terminate(t.Context()); err != nil {
		t.Fatalf("terminate of an empty control plane = %v", err)
	}
}

func TestControlPlaneStatePublishAndOwns(t *testing.T) {
	var s controlPlaneState
	fake := &controlStateFake{id: "control-1"}
	s.publish(fake)
	if s.current() != fake {
		t.Fatal("publish did not store the handle")
	}
	if ctr, err := s.currentContext(t.Context()); ctr != fake || err != nil {
		t.Fatalf("currentContext = %v, %v", ctr, err)
	}
	if ctr, err := s.diagnosticHandle(t.Context()); ctr != fake || err != nil {
		t.Fatalf("diagnosticHandle = %v, %v", ctr, err)
	}
	for id, want := range map[string]bool{"control-1": true, "control-2": false, "": false} {
		if got, err := s.owns(t.Context(), id); got != want || err != nil {
			t.Fatalf("owns(%q) = %v, %v, want %v", id, got, err, want)
		}
	}
}

// TestControlPlaneStateTerminateKeepsAHandleWhoseRemovalFailed pins the retry
// contract: a real failure keeps the handle so the next Terminate retries it;
// a missing container or success forgets it.
func TestControlPlaneStateTerminateKeepsAHandleWhoseRemovalFailed(t *testing.T) {
	var s controlPlaneState
	fake := &controlStateFake{id: "c", terminateErr: errors.New("synthetic removal failure")}
	s.publish(fake)
	if err := s.terminate(t.Context()); !errors.Is(err, fake.terminateErr) || s.current() != fake {
		t.Fatalf("failed terminate = %v, handle kept %v", err, s.current() == fake)
	}
	fake.terminateErr = errdefs.ErrNotFound
	if err := s.terminate(t.Context()); err != nil || s.current() != nil {
		t.Fatalf("missing-container terminate = %v, handle %v", err, s.current())
	}
	if fake.terminations != 2 {
		t.Fatalf("removals = %d, want 2", fake.terminations)
	}
	if err := s.terminate(t.Context()); err != nil || fake.terminations != 2 {
		t.Fatalf("terminate after forgetting reached Docker: %v, %d", err, fake.terminations)
	}
	s.publish(&controlStateFake{id: "ok"})
	if err := s.terminate(t.Context()); err != nil || s.current() != nil {
		t.Fatalf("successful terminate = %v, handle %v", err, s.current())
	}
}

// TestControlPlaneStateReadersHonorTheContextWhileTheLockIsHeld pins the two
// bounded readers: a held write lock, as terminate holds it around a Docker
// removal, ends in the context error and never blocks.
func TestControlPlaneStateReadersHonorTheContextWhileTheLockIsHeld(t *testing.T) {
	var s controlPlaneState
	s.publish(&controlStateFake{id: "c"})
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, read := range map[string]func(context.Context) error{
		"currentContext": func(ctx context.Context) error { _, err := s.currentContext(ctx); return err },
		"owns":           func(ctx context.Context) error { _, err := s.owns(ctx, "c"); return err },
		"diagnosticHandle": func(ctx context.Context) error {
			_, err := s.diagnosticHandle(ctx)
			return err
		},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		done := make(chan error, 1)
		go func() { done <- read(ctx) }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s = %v, want the deadline", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s blocked past its context", name)
		}
		cancel()
	}
}

func TestControlPlaneStateConcurrentReadersAndPublisher(t *testing.T) {
	var s controlPlaneState
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_ = s.current()
				_, _ = s.owns(t.Context(), "c")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			s.publish(&controlStateFake{id: "c"})
			_ = s.terminate(t.Context())
		}
	}()
	wg.Wait()
}

// TestTestStatePublishesTheControlPlaneLockIdentity pins that the ControlMutex
// the service packages queue on is the lock of the component, so the moved lock
// is the same lock domain, not a copy.
func TestTestStatePublishesTheControlPlaneLockIdentity(t *testing.T) {
	c := &Container{}
	if _, control, _, _ := (unitAccess{}).UnitState(c); control != &c.controlPlane.mu {
		t.Fatal("UnitState.ControlMutex is not the control-plane lock")
	}
	fake := &controlStateFake{id: "hook"}
	unitAccess{}.UnitSetControlPlane(c, fake)
	if c.cliContainer() != fake {
		t.Fatal("UnitSetControlPlane did not reach the control selection")
	}
}

// controlPlaneMethods pins the component: the calls each method makes. Only
// the lock calls on its own mutex, the handle's Terminate and the two
// contextual lock helpers appear; none calls into Container or takes another
// lock.
var controlPlaneMethods = map[string][]string{
	"current":          {"s.mu.RLock()", "s.mu.RUnlock()"},
	"currentContext":   {"lockTopologyReadMutex(ctx, &s.mu)", "s.mu.RUnlock()"},
	"owns":             {"s.currentContext(ctx)", "ctr.GetContainerID()"},
	"diagnosticHandle": {"diagnosticLock(ctx, s.mu.TryRLock)", "s.mu.RUnlock()"},
	"publish":          {"s.mu.Lock()", "s.mu.Unlock()"},
	"terminate":        {"s.mu.Lock()", "s.mu.Unlock()", "s.handle.Terminate(ctx, opts...)", "onlyMissingHostResource(err)"},
	"mutex":            nil,
}

// controlPlaneCallers lists every function outside the component that calls
// one of its methods, with the method it calls and how it is guarded:
//
//	"free"     the method takes the leaf lock itself; the caller may or may not
//	           hold Container.mu (the order is mu, then the control lock)
//	"holds"    its doc comment says the caller holds the topology lock
//	"takes"    calls lockTopology before its first use
//	"identity" returns the lock identity for the test state view
var controlPlaneCallers = map[string]map[string]string{
	"terminate":               {"terminate": "takes"},
	"ensureControlPlane":      {"current": "holds", "publish": "holds"},
	"cliContainer":            {"current": "free"},
	"controlContainerContext": {"currentContext": "free"},
	"admitNativeTarget":       {"owns": "free"},
	"sharedDaemons":           {"current": "free"},
	"diagnosticsSnapshot":     {"diagnosticHandle": "free"},
	"UnitState":               {"mutex": "identity"},
}

// isControlPlaneHandleOwner reports whether e is the component itself: the
// receiver s of its methods, or Container.controlPlane.
func isControlPlaneHandleOwner(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name == "s"
	case *ast.SelectorExpr:
		return v.Sel.Name == "controlPlane"
	}
	return false
}

func controlPlaneUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "controlPlane" }

func isControlPlaneReceiver(fn *ast.FuncDecl) bool {
	return receiverTypeName(fn) == "controlPlaneState"
}

// TestControlPlaneStateFieldIsOnlyTouchedByItsMethods guards the ownership
// written on controlPlaneState: outside this type, Container.controlPlane is
// the receiver of one of its methods, or (the UnitSetControlPlane hook only)
// the owner of the handle field; the lock-free FactsOf source never mentions
// it, and no production composite literal of Container sets it.
func TestControlPlaneStateFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isControlPlaneReceiver(fn) {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if method, ok := x.Fun.(*ast.SelectorExpr); ok {
						if _, known := controlPlaneMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && controlPlaneUse(recv) {
								allowed[recv] = true
							}
						}
					}
				case *ast.SelectorExpr:
					// c.controlPlane.handle = control, in the hook only.
					if x.Sel.Name == "handle" && fn.Name.Name == "UnitSetControlPlane" {
						if recv, ok := x.X.(*ast.SelectorExpr); ok && controlPlaneUse(recv) {
							allowed[recv] = true
						}
					}
				case *ast.CompositeLit:
					if id, ok := x.Type.(*ast.Ident); ok && id.Name == "Container" {
						for _, elt := range x.Elts {
							if kv, ok := elt.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "controlPlane" {
									problems = append(problems, file+": "+fn.Name.Name+" sets controlPlane in a Container literal")
								}
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && controlPlaneUse(sel) && isContainerReceiver(sel.X) {
					if allowed[sel] {
						uses++
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" uses Container.controlPlane outside a controlPlaneState method call")
					}
				}
				return true
			})
			if file == "cluster_facts.go" && strings.Contains(fn.Name.Name, "Facts") {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && controlPlaneUse(sel) {
						problems = append(problems, "FactsOf source reads Container.controlPlane")
					}
					return true
				})
			}
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.controlPlane found: the guard is vacuous")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("control plane state escapes its methods: %v", problems)
	}
}

// TestControlPlaneStateMethodsMakeOnlyTheirPinnedCalls guards the lock
// contract of the type: the method set is pinned, each method makes exactly the
// calls listed (no other lock, no Container call), the handle is assigned only
// by publish and terminate, and no method names Container, closed or another
// mutex.
func TestControlPlaneStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isControlPlaneReceiver(fn) {
				continue
			}
			name := fn.Name.Name
			want, known := controlPlaneMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update controlPlaneMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, types.ExprString(x))
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "handle" && name != "publish" && name != "terminate" {
							problems = append(problems, name+" assigns handle")
						}
					}
				case *ast.Ident:
					if x.Name == "Container" || x.Name == "closed" {
						problems = append(problems, name+" names "+x.Name)
					}
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range controlPlaneMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("controlPlaneState breaks its lock contract: %v", problems)
	}
}

// TestControlPlaneStateWritersAreAudited pins the production writers of the
// handle: publish and terminate on the type (each under the write lock) and the
// UnitSetControlPlane hook (a plain write whose doc names the lock contract).
func TestControlPlaneStateWritersAreAudited(t *testing.T) {
	want := map[string]string{"publish": "takes", "terminate": "takes", "UnitSetControlPlane": "hook"}
	docs := osdRegistryDocs(t)
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
				if as, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range as.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "handle" && isControlPlaneHandleOwner(sel.X) {
							writes = true
						}
					}
				}
				return true
			})
			if !writes {
				continue
			}
			name := fn.Name.Name
			got[name] = true
			mode, known := want[name]
			switch {
			case !known:
				problems = append(problems, name+" writes the control-plane handle but is not in the writer table")
			case mode == "takes" && !callsMethod(fn, "mu", "Lock"):
				problems = append(problems, name+" must take the write lock around its write")
			case mode == "hook" && !(strings.Contains(docs[name], "holds") && strings.Contains(docs[name], "not shared")):
				problems = append(problems, name+" is a hook row but its doc comment does not name the lock contract")
			}
		}
	}
	for name := range want {
		if !got[name] {
			problems = append(problems, name+" is listed as a writer but no longer writes the handle")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("control-plane handle writers changed: %v", problems)
	}
}

// TestControlPlaneStateCallersAreAudited is the caller-table guard: the set of
// functions that use the component and the method each calls are pinned, the
// "takes" caller calls lockTopology before its first use, the "holds" callers
// document that the caller holds the topology lock, the lock identity has the
// single caller UnitState, and a stale row fails.
func TestControlPlaneStateCallersAreAudited(t *testing.T) {
	docs := osdRegistryDocs(t)
	found := map[string]map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isControlPlaneReceiver(fn) {
				continue
			}
			var lockAt token.Pos
			uses := map[string]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if isTopologyAcquire(call) && lockAt == token.NoPos {
					lockAt = call.Pos()
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && controlPlaneUse(recv) {
						if _, seen := uses[sel.Sel.Name]; !seen {
							uses[sel.Sel.Name] = call.Pos()
						}
					}
				}
				return true
			})
			name := fn.Name.Name
			for method, at := range uses {
				if found[name] == nil {
					found[name] = map[string]bool{}
				}
				found[name][method] = true
				mode, known := controlPlaneCallers[name][method]
				switch {
				case !known:
					problems = append(problems, name+" calls controlPlane."+method+" but is not in controlPlaneCallers")
				case mode == "takes" && (lockAt == token.NoPos || lockAt > at):
					problems = append(problems, name+" must take lockTopology before controlPlane."+method)
				case mode == "holds" && !strings.Contains(docs[name], "holds c.mu"):
					problems = append(problems, name+" calls controlPlane."+method+" but its doc does not say the caller holds c.mu")
				case mode == "identity" && name != "UnitState":
					problems = append(problems, name+" publishes the control-plane lock identity")
				}
			}
		}
	}
	for name, methods := range controlPlaneCallers {
		for method := range methods {
			if !found[name][method] {
				problems = append(problems, name+" is listed for controlPlane."+method+" but no longer calls it")
			}
		}
	}
	lockers := 0
	for _, methods := range found {
		if methods["mutex"] {
			lockers++
		}
	}
	if lockers != 1 || !found["UnitState"]["mutex"] {
		problems = append(problems, "controlPlane.mutex must have the single caller UnitState")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("control plane callers break the lock contract: %v", problems)
	}
}

// TestTerminateRemovesTheControlPlaneUnderTheTopologyLockBeforeTheFeatureClient
// guards the Terminate order: the control container is removed once, after the
// topology lock is taken and the first MON is terminated, and before the
// feature client and the labeled containers.
func TestTerminateRemovesTheControlPlaneUnderTheTopologyLockBeforeTheFeatureClient(t *testing.T) {
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			at := map[string][]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch s := types.ExprString(call.Fun); s {
				case "c.lockTopology", "c.controlPlane.terminate", "c.Container.Terminate", "c.toolkit.lock", "c.removeLabeledContainers":
					at[s] = append(at[s], call.Pos())
				}
				return true
			})
			order := []string{"c.lockTopology", "c.Container.Terminate", "c.controlPlane.terminate", "c.toolkit.lock", "c.removeLabeledContainers"}
			var problems []string
			for _, name := range order {
				if len(at[name]) != 1 {
					problems = append(problems, name+" must be called exactly once in terminate")
				}
			}
			if len(problems) == 0 {
				for i := 1; i < len(order); i++ {
					if at[order[i-1]][0] >= at[order[i]][0] {
						problems = append(problems, order[i-1]+" must come before "+order[i])
					}
				}
			}
			if len(problems) != 0 {
				t.Fatalf("terminate changed the control-plane removal order: %v", problems)
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
