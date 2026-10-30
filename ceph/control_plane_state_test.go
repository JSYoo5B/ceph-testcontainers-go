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
	if c.conn().cliContainer() != fake {
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
var controlPlaneCallers = map[string]stateCaller{
	"terminate":           {"takes", []string{"terminate"}},
	"ensureControlPlane":  {"holds", []string{"current", "publish"}},
	"monitorJoinMaterial": {"holds", nil},
	"cliContainer":        {"free", []string{"current"}},
	"controlContainer":    {"free", []string{"currentContext"}},
	"admitNativeTarget":   {"free", []string{"owns"}},
	"diagnosticsSnapshot": {"free", []string{"diagnosticHandle"}},
	"UnitState":           {"identity", []string{"mutex"}},
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

// controlPlaneUse reports whether sel names the component: Container's
// controlPlane field, or the control pointer of a cephConn method receiver n,
// which Container.conn sets to &c.controlPlane.
func controlPlaneUse(sel *ast.SelectorExpr) bool {
	if sel.Sel.Name == "controlPlane" {
		return true
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "n" && sel.Sel.Name == "control"
}

func isControlPlaneReceiver(fn *ast.FuncDecl) bool {
	return receiverTypeName(fn) == "controlPlaneState"
}

// TestControlPlaneStateFieldIsOnlyTouchedByItsMethods guards the ownership
// written on controlPlaneState: outside this type, Container.controlPlane is
// the receiver of one of its methods, or (the UnitSetControlPlane hook only)
// the owner of the handle field; the lock-free FactsOf source never mentions
// it, and no production composite literal of Container sets it.
func TestControlPlaneStateFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.controlPlane", use: controlPlaneUse, own: isControlPlaneReceiver, methods: controlPlaneMethods, literalField: "controlPlane",
		counted: func(_ *ast.FuncDecl, sel *ast.SelectorExpr) bool { return isContainerReceiver(sel.X) },
		allow: func(file string, fn *ast.FuncDecl, n ast.Node, allowed map[*ast.SelectorExpr]bool) []string {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				// c.controlPlane.handle = control, in the hook only.
				if x.Sel.Name == "handle" && fn.Name.Name == "UnitSetControlPlane" {
					if recv, ok := x.X.(*ast.SelectorExpr); ok && controlPlaneUse(recv) {
						allowed[recv] = true
					}
				}
			case *ast.CompositeLit:
				// cephConn{control: &c.controlPlane}, in Container.conn only:
				// the transport reaches the component through its pinned
				// readers (TestCephConnReachesTheControlPlaneOnlyThroughItsReaders).
				if id, ok := x.Type.(*ast.Ident); !ok || id.Name != "cephConn" {
					return nil
				}
				var problems []string
				for _, elt := range x.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != "control" {
						continue
					}
					u, ok := kv.Value.(*ast.UnaryExpr)
					var recv *ast.SelectorExpr
					if ok && u.Op == token.AND {
						recv, _ = u.X.(*ast.SelectorExpr)
					}
					if recv != nil && fn.Name.Name == "conn" && receiverTypeName(fn) == "Container" && controlPlaneUse(recv) {
						allowed[recv] = true
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" builds a cephConn control other than &c.controlPlane in Container.conn")
					}
				}
				return problems
			}
			return nil
		}}.check(t)
}

// TestControlPlaneStateMethodsMakeOnlyTheirPinnedCalls guards the lock
// contract of the type: the method set is pinned, each method makes exactly the
// calls listed (no other lock, no Container call), the handle is assigned only
// by publish and terminate, and no method names Container, closed or another
// mutex.
func TestControlPlaneStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "controlPlaneState", own: isControlPlaneReceiver, methods: controlPlaneMethods,
		render:  func(call *ast.CallExpr) string { return types.ExprString(call) },
		writers: map[string][]string{"handle": {"publish", "terminate"}}, forbidden: []string{"Container", "closed"}}.check(t)
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
	callerAudit{label: "the control plane", use: controlPlaneUse, own: isControlPlaneReceiver, callers: controlPlaneCallers,
		only: map[string][]string{"identity": {"UnitState"}}, holdsDoc: "holds c.mu"}.check(t)
}

// TestTerminateRemovesTheControlPlaneUnderTheTopologyLockBeforeTheFeatureClient
// guards the Terminate order: the control container is removed once, after the
// topology lock is taken and the first MON is terminated, and before the
// feature client and the labeled containers.
func TestTerminateRemovesTheControlPlaneUnderTheTopologyLockBeforeTheFeatureClient(t *testing.T) {
	for _, parsed := range parseProductionFilesWithTerminateSteps(t) {
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
				case "c.lockTopology", "c.controlPlane.terminate", "c.Container.Terminate", "c.toolkit.lock", "c.startup.removeLabeledContainers":
					at[s] = append(at[s], call.Pos())
				}
				return true
			})
			order := []string{"c.lockTopology", "c.Container.Terminate", "c.controlPlane.terminate", "c.toolkit.lock", "c.startup.removeLabeledContainers"}
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
