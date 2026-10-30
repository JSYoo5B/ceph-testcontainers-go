package ceph

import (
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"testing"
)

func TestPortLeaseStateZeroValueIsEmptyAndWritable(t *testing.T) {
	var s portLeaseState
	if s.count() != 0 || s.has(nil) || len(s.release(t.Context())) != 0 {
		t.Fatalf("an empty state reports a lease: %+v", s)
	}
	s.track(nil) // a nil lease is not recorded
	if s.count() != 0 {
		t.Fatal("a nil lease was recorded")
	}
	lease := &hostPortLease{}
	s.track(lease)
	if s.count() != 1 || !s.has(lease) || s.has(&hostPortLease{}) {
		t.Fatalf("track did not record exactly the lease: %+v", s)
	}
}

func TestPortLeaseStateReleaseTriesEveryLeaseAndKeepsAFailedOneForRetry(t *testing.T) {
	failing := &hostNetworkFixtureContainer{failTerminationOnce: true}
	fine := &hostNetworkFixtureContainer{}
	var s portLeaseState
	s.track(&hostPortLease{ctr: failing})
	s.track(&hostPortLease{ctr: fine})

	// The failure of the first lease does not stop the second from being
	// released, and is reported once.
	errs := s.release(t.Context())
	if len(errs) != 1 || failing.terminations != 1 || fine.terminations != 1 {
		t.Fatalf("release = %v, terminations %d and %d", errs, failing.terminations, fine.terminations)
	}
	// A retry reaches only the lease that failed; the released one is done.
	if errs = s.release(t.Context()); len(errs) != 0 || failing.terminations != 2 || fine.terminations != 1 {
		t.Fatalf("retry = %v, terminations %d and %d", errs, failing.terminations, fine.terminations)
	}
	if s.count() != 2 {
		t.Fatalf("release forgot a lease: %d", s.count())
	}
	boom := errors.New("synthetic removal failure")
	s.track(&hostPortLease{ctr: &hostNetworkFixtureContainer{terminationErr: boom}})
	if errs = s.release(t.Context()); len(errs) != 1 || !errors.Is(errs[0], boom) {
		t.Fatalf("release did not report the new failure: %v", errs)
	}
}

// portLeaseMethods pins the component: the calls each method makes. None takes
// a lock or calls into Container.
var portLeaseMethods = map[string][]string{
	"track":   {"append"},
	"count":   {"len"},
	"has":     nil,
	"release": {"lease.Release(ctx)", "append"},
}

func portLeaseUse(sel *ast.SelectorExpr) bool { return sel.Sel.Name == "portLeases" }

func isPortLeaseReceiver(fn *ast.FuncDecl) bool { return receiverTypeName(fn) == "portLeaseState" }

// TestPortLeaseStateFieldIsOnlyTouchedByItsMethods guards the ownership
// written on portLeaseState: outside this type, Container.portLeases is used
// only as the receiver of one of its methods, so no code reads the slice or
// takes its address, and the lock-free FactsOf source never mentions it.
func TestPortLeaseStateFieldIsOnlyTouchedByItsMethods(t *testing.T) {
	fieldAudit{label: "Container.portLeases", use: portLeaseUse, own: isPortLeaseReceiver, methods: portLeaseMethods,
		allow: func(file string, fn *ast.FuncDecl, n ast.Node, _ map[*ast.SelectorExpr]bool) []string {
			var problems []string
			if un, ok := n.(*ast.UnaryExpr); ok && un.Op == token.AND {
				ast.Inspect(un.X, func(m ast.Node) bool {
					if sel, ok := m.(*ast.SelectorExpr); ok && (portLeaseUse(sel) || sel.Sel.Name == "leases") {
						problems = append(problems, file+": takes the address of "+types.ExprString(un.X))
					}
					return true
				})
			}
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "leases" {
				problems = append(problems, file+": "+fn.Name.Name+" reads the lease slice")
			}
			return problems
		}}.check(t)
}

// TestPortLeaseStateMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned, each method makes exactly the calls
// listed (none locks or calls into Container), and the slice is assigned only
// by track.
func TestPortLeaseStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	methodAudit{label: "portLeaseState", own: isPortLeaseReceiver, methods: portLeaseMethods, writers: map[string][]string{"leases": {"track"}},
		render: func(call *ast.CallExpr) string {
			if name := types.ExprString(call.Fun); name != "lease.Release" {
				return name
			}
			return "lease.Release(ctx)"
		}}.check(t)
}

// portLeaseCallers lists every function outside the component that calls one
// of its methods, with the methods it calls and how it is guarded:
//
//	"takes"  acquires Container.mu before its first use
//	"holds"  its doc comment says the caller holds the lock (see stateCaller)
//	"hook"   its doc comment says the caller holds the lock or the cluster is
//	         not shared (the exported test state view)
var portLeaseCallers = map[string]stateCaller{
	"reserveTrackedHostPortsWith": {"takes", []string{"track"}},
	"trackHostPortLease":          {"takes", []string{"track"}},
	"startJoiningMonitor":         {"holds", []string{"track"}},
	"terminate":                   {"takes", []string{"release"}},
	"UnitState":                   {"hook", []string{"count"}},
}

// TestPortLeaseStateCallersAreAudited guards the callers: the set of
// functions that use the component and the methods each calls are pinned, a
// "takes" function acquires Container.mu before its first use, a "hook"
// function documents the contract, and a stale row fails. Container.mu is the
// whole lock domain: the type has no lock of its own.
func TestPortLeaseStateCallersAreAudited(t *testing.T) {
	callerAudit{label: "the port lease state", use: portLeaseUse, own: isPortLeaseReceiver, callers: portLeaseCallers}.check(t)
}

// TestTerminateReleasesPortLeasesAfterNativeCommandsAndBeforeTheServices
// guards the Terminate order: leases are released after the faults are undone
// and the native commands are stopped, and before the services, the OSDs and
// the monitors are terminated, exactly once.
func TestTerminateReleasesPortLeasesAfterNativeCommandsAndBeforeTheServices(t *testing.T) {
	for _, parsed := range parseProductionFilesWithTerminateSteps(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isContainerTerminate(fn) {
				continue
			}
			var undo, stop, services []token.Pos
			var release []token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "undo":
							if recv, ok := sel.X.(*ast.SelectorExpr); ok && faultsUse(recv) {
								undo = append(undo, x.Pos())
							}
						case "stopAll":
							if types.ExprString(sel.X) == "c.native" {
								stop = append(stop, x.Pos())
							}
						case "release":
							if recv, ok := sel.X.(*ast.SelectorExpr); ok && portLeaseUse(recv) {
								release = append(release, x.Pos())
							}
						}
					}
				case *ast.RangeStmt:
					if types.ExprString(x.X) == "c.members.roster.eachService()" {
						services = append(services, x.Pos())
					}
				}
				return true
			})
			if len(undo) != 1 || len(stop) == 0 || len(release) != 1 || len(services) != 1 {
				t.Fatalf("terminate must call faults.undo and portLeases.release once, c.native.stopAll at least once and range over services once: %d %d %d %d", len(undo), len(stop), len(release), len(services))
			}
			if !(undo[0] < release[0] && stop[0] < release[0] && release[0] < services[0]) {
				t.Fatal("terminate must release the port leases after the faults and native commands and before the services")
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
