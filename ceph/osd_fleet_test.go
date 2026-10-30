package ceph

import (
	"fmt"
	"go/ast"
	"go/token"
	"slices"
	"testing"
)

// osdFleetBinding reports whether fn is Container.osdFleet, the one function
// that hands the OSD state's addresses to the fleet. The field guards of that
// state admit its uses there; TestOSDFleetBindsEachStateOnce pins what it does.
func osdFleetBinding(fn *ast.FuncDecl) bool {
	return fn != nil && receiverTypeName(fn) == "Container" && fn.Name.Name == "osdFleet"
}

// stateBinding reports whether fn is an owner constructor that binds the run
// directory and the startup identity: Container.osdFleet or
// Container.featureClient. Each binds them once (TestOSDFleetBindsEachStateOnce,
// TestFeatureClientBindsEachStateOnce).
func stateBinding(fn *ast.FuncDecl) bool {
	return osdFleetBinding(fn) || fn != nil && receiverTypeName(fn) == "Container" && fn.Name.Name == "featureClient"
}

// inOSDFleetBinding reports whether pos lies in Container.osdFleet of file.
func inOSDFleetBinding(file *ast.File, pos token.Pos) bool {
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && osdFleetBinding(fn) && fn.Pos() <= pos && pos < fn.End() {
			return true
		}
	}
	return false
}

// osdFleetBound is the state that Container.osdFleet binds by address: the
// fleet's own state (osdFleetState, which Container embeds) and the run
// directory and startup identity it reads. The registry, storage and claims
// inside osdFleetState keep their own field guards, which also hold for the
// fleet's methods, because they reach the state under the same field names.
var osdFleetBound = []string{"osdFleetState", "runDir", "startup"}

// TestOSDFleetBindsEachStateOnce guards the fleet's construction and its
// back reference. Container.osdFleet is a single return of one osdFleet
// literal that binds each state in osdFleetBound exactly once, as &c.<same
// field>; no other function builds an osdFleet; and the fleet's methods use
// owner only as the owner of an OSD handle they create.
func TestOSDFleetBindsEachStateOnce(t *testing.T) {
	bindingAudit{typeName: "osdFleet", constructor: osdFleetBinding, bound: osdFleetBound, singleReturn: true, recv: "f", handles: []string{"OSDContainer"}}.check(t)
}

// ownerStates are the owner state types and fields, each with the one
// constructor that may name it.
var ownerStates = map[string]string{
	"osdFleetState": "Container.osdFleet", "fleet": "Container.osdFleet",
	"membershipState": "Container.monitorTopology", "members": "Container.monitorTopology",
	"authLedgerState": "Container.authLedger", "ledger": "Container.authLedger",
}

// TestOwnerStatesAreNamedOnlyByTheirConstructors guards that the owner states
// stay owned: production code names Container.fleet, members or ledger, or
// their types (as a selector, a literal key or a literal type), only in that
// owner's constructor, so no other code takes the
// whole state, copies it or replaces it. A use of a field inside a state
// (c.fleet.osdHandles) is pinned by that field's own guard.
func TestOwnerStatesAreNamedOnlyByTheirConstructors(t *testing.T) {
	var problems []string
	named := map[string]int{}
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			// Reaching into a state (c.fleet.osdHandles) is the inner
			// field's use, pinned by that field's guard; only the whole
			// state is checked here.
			inner := map[ast.Expr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					inner[sel.X] = true
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				name := ""
				switch n := n.(type) {
				case *ast.SelectorExpr:
					// A field is named on the cluster; the owner views reach
					// their state through the embedded type, not by name.
					if _, isType := ownerStateFields[n.Sel.Name]; isType || isContainerReceiver(n.X) && !inner[n] {
						name = n.Sel.Name
					}
				case *ast.KeyValueExpr:
					if key, ok := n.Key.(*ast.Ident); ok {
						name = key.Name
					}
				case *ast.CompositeLit:
					if id, ok := n.Type.(*ast.Ident); ok {
						name = id.Name
					}
				}
				owner, isState := ownerStates[name]
				if !isState {
					return true
				}
				if funcKey(fn) != owner {
					problems = append(problems, file+": "+funcKey(fn)+" names "+name)
				}
				named[name]++
				return true
			})
		}
	}
	for state, owner := range ownerStates {
		// The binding names the type once (the literal key) and the field once (&c.<field>).
		if named[state] != 1 {
			problems = append(problems, fmt.Sprintf("%s is named %d times, want only its binding in %s", state, named[state], owner))
		}
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("an owner state escapes its owner: %v", problems)
	}
}

// featureClientBound is the state that Container.featureClient binds by
// address. Each keeps its own field guard, which also holds for the feature
// client's methods because they reach the state under the same field names.
var featureClientBound = []string{"native", "runDir", "startup", "toolkit"}

// TestFeatureClientBindsEachStateOnce guards the feature client's
// construction: Container.featureClient binds each state in
// featureClientBound once, as &c.<same field>, in its one featureClient
// literal, and no other function builds a featureClient.
func TestFeatureClientBindsEachStateOnce(t *testing.T) {
	bindingAudit{typeName: "featureClient", bound: featureClientBound, skipEmpty: true,
		constructor: func(fn *ast.FuncDecl) bool { return funcKey(fn) == "Container.featureClient" }}.check(t)
}
