package ceph

import (
	"errors"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
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
	var problems []string
	uses := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isPortLeaseReceiver(fn) {
				continue
			}
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if method, ok := call.Fun.(*ast.SelectorExpr); ok {
						if _, known := portLeaseMethods[method.Sel.Name]; known {
							if recv, ok := method.X.(*ast.SelectorExpr); ok && portLeaseUse(recv) {
								allowed[recv] = true
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && portLeaseUse(sel) {
					if allowed[sel] {
						uses++
					} else {
						problems = append(problems, file+": "+fn.Name.Name+" uses Container.portLeases outside a portLeaseState method call")
					}
				}
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
				return true
			})
			if file == "cluster_facts.go" && strings.Contains(fn.Name.Name, "Facts") {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && portLeaseUse(sel) {
						problems = append(problems, "FactsOf source reads Container.portLeases")
					}
					return true
				})
			}
		}
	}
	if uses == 0 {
		problems = append(problems, "no use of Container.portLeases found: the guard is vacuous")
	}
	if len(problems) != 0 {
		t.Fatalf("port lease state escapes its methods: %v", problems)
	}
}

// TestPortLeaseStateMethodsMakeOnlyTheirPinnedCalls guards the lock contract
// of the type: the method set is pinned, each method makes exactly the calls
// listed (none locks or calls into Container), and the slice is assigned only
// by track.
func TestPortLeaseStateMethodsMakeOnlyTheirPinnedCalls(t *testing.T) {
	seen := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isPortLeaseReceiver(fn) {
				continue
			}
			name := fn.Name.Name
			want, known := portLeaseMethods[name]
			if !known {
				problems = append(problems, name+" is not pinned; review the lock contract and update portLeaseMethods")
				continue
			}
			seen[name] = true
			var got []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					got = append(got, types.ExprString(x.Fun))
					if types.ExprString(x.Fun) == "lease.Release" {
						got[len(got)-1] = "lease.Release(ctx)"
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "leases" && name != "track" {
							problems = append(problems, name+" assigns leases")
						}
					}
				}
				return true
			})
			if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(want))
			}
		}
	}
	for name := range portLeaseMethods {
		if !seen[name] {
			problems = append(problems, name+" is pinned but missing")
		}
	}
	if len(problems) != 0 {
		t.Fatalf("portLeaseState breaks its lock contract: %v", problems)
	}
}

// portLeaseCallers lists every function outside the component that calls one
// of its methods, with the methods it calls and how it is guarded:
//
//	"takes"  acquires Container.mu before its first use
//	"hook"   its doc comment says the caller holds the lock or the cluster is
//	         not shared (the exported test state view)
var portLeaseCallers = map[string]struct {
	mode    string
	methods []string
}{
	"reserveTrackedHostPortsWith": {"takes", []string{"track"}},
	"trackHostPortLease":          {"takes", []string{"track"}},
	"AddMonitor":                  {"takes", []string{"track"}},
	"terminate":                   {"takes", []string{"release"}},
	"TestState":                   {"hook", []string{"count"}},
}

// TestPortLeaseStateCallersAreAudited guards the callers: the set of
// functions that use the component and the methods each calls are pinned, a
// "takes" function acquires Container.mu before its first use, a "hook"
// function documents the contract, and a stale row fails. Container.mu is the
// whole lock domain: the type has no lock of its own.
func TestPortLeaseStateCallersAreAudited(t *testing.T) {
	docs := osdRegistryDocs(t)
	found := map[string]bool{}
	var problems []string
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || isPortLeaseReceiver(fn) {
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
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && portLeaseUse(recv) {
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
			name := fn.Name.Name
			found[name] = true
			row, known := portLeaseCallers[name]
			if !known {
				problems = append(problems, name+" uses the port lease state but is not in portLeaseCallers")
				continue
			}
			if at := osdRegistryAcquirePos(fn); row.mode == "takes" && (at == token.NoPos || at > first) {
				problems = append(problems, name+" must take Container.mu before using the port lease state")
			}
			if row.mode == "hook" && !strings.Contains(docs[name], "holds") {
				problems = append(problems, name+" is a hook row but its doc comment does not name the lock contract")
			}
			got := slices.Sorted(func(yield func(string) bool) {
				for m := range methods {
					if !yield(m) {
						return
					}
				}
			})
			if !slices.Equal(got, row.methods) {
				problems = append(problems, name+" calls "+join(got)+", pinned "+join(row.methods))
			}
		}
	}
	for name := range portLeaseCallers {
		if !found[name] {
			problems = append(problems, name+" is listed but no longer uses the port lease state")
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("port lease callers break the lock contract: %v", problems)
	}
}

// TestTerminateReleasesPortLeasesAfterNativeCommandsAndBeforeTheServices
// guards the Terminate order: leases are released after the faults are undone
// and the native commands are stopped, and before the services, the OSDs and
// the monitors are terminated, exactly once.
func TestTerminateReleasesPortLeasesAfterNativeCommandsAndBeforeTheServices(t *testing.T) {
	for _, parsed := range parseProductionFiles(t) {
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
						case "undoFaults":
							undo = append(undo, x.Pos())
						case "stopNativeInvocations":
							stop = append(stop, x.Pos())
						case "release":
							if recv, ok := sel.X.(*ast.SelectorExpr); ok && portLeaseUse(recv) {
								release = append(release, x.Pos())
							}
						}
					}
				case *ast.RangeStmt:
					if types.ExprString(x.X) == "c.services" {
						services = append(services, x.Pos())
					}
				}
				return true
			})
			if len(undo) != 1 || len(stop) == 0 || len(release) != 1 || len(services) != 1 {
				t.Fatalf("terminate must call undoFaults and portLeases.release once, stopNativeInvocations at least once and range over services once: %d %d %d %d", len(undo), len(stop), len(release), len(services))
			}
			if !(undo[0] < release[0] && stop[0] < release[0] && release[0] < services[0]) {
				t.Fatal("terminate must release the port leases after the faults and native commands and before the services")
			}
			return
		}
	}
	t.Fatal("Container.terminate not found")
}
