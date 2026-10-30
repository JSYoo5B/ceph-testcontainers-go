package ceph

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"testing"
)

// osdFleetBinding reports whether fn is Container.osdFleet, the one function
// that hands the OSD state's addresses to the fleet. The field guards of that
// state admit its uses there; TestOSDFleetBindsEachStateOnce pins what it does.
func osdFleetBinding(fn *ast.FuncDecl) bool {
	return fn != nil && receiverTypeName(fn) == "Container" && fn.Name.Name == "osdFleet"
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
	var problems []string
	constructors := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if id, ok := lit.Type.(*ast.Ident); ok && id.Name == "osdFleet" {
					constructors++
					if !osdFleetBinding(fn) {
						problems = append(problems, file+": "+funcKey(fn)+" builds an osdFleet")
					}
				}
				return true
			})
			if osdFleetBinding(fn) {
				problems = append(problems, checkOSDFleetConstructor(fn)...)
			}
			if receiverTypeName(fn) == "osdFleet" {
				problems = append(problems, checkOSDFleetOwnerUse(file, fn)...)
			}
		}
	}
	if constructors != 1 {
		problems = append(problems, "want exactly one osdFleet literal")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("the OSD fleet binding is not pinned: %v", problems)
	}
}

func checkOSDFleetConstructor(fn *ast.FuncDecl) []string {
	return checkStateBinding(fn, "osdFleet", osdFleetBound)
}

// checkStateBinding checks that fn is a single return of one literal of
// typeName that binds each field in bound exactly once, as &c.<same field>.
func checkStateBinding(fn *ast.FuncDecl, typeName string, bound []string) []string {
	name := "Container." + fn.Name.Name
	if len(fn.Body.List) != 1 {
		return []string{name + " must be a single return"}
	}
	ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return []string{name + " must be a single return"}
	}
	lit, ok := ret.Results[0].(*ast.CompositeLit)
	if !ok {
		return []string{name + " must return a " + typeName + " literal"}
	}
	var problems []string
	seen := map[string]int{}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			problems = append(problems, typeName+" literal must be keyed")
			continue
		}
		key := kv.Key.(*ast.Ident).Name
		if !slices.Contains(bound, key) {
			continue
		}
		address, ok := kv.Value.(*ast.UnaryExpr)
		sel, selOK := (*ast.SelectorExpr)(nil), false
		if ok && address.Op == token.AND {
			sel, selOK = address.X.(*ast.SelectorExpr)
		}
		field := key
		if f, ok := ownerStateFields[key]; ok {
			field = f
		}
		if !selOK || sel.Sel.Name != field || !isContainerReceiver(sel.X) {
			problems = append(problems, typeName+" binds "+key+" to "+types.ExprString(kv.Value)+", want &c."+field)
		}
		seen[key]++
	}
	for _, field := range bound {
		if seen[field] != 1 {
			problems = append(problems, fmt.Sprintf("%s binds %s %d times, want once", typeName, field, seen[field]))
		}
	}
	return problems
}

func checkOSDFleetOwnerUse(file string, fn *ast.FuncDecl) []string {
	return checkOwnerUse(file, fn, "f", "OSDContainer")
}

// checkOwnerUse checks that the methods of a binding type, whose receiver is
// named recv, select recv.owner only as the owner of a new handle literal of
// one of handles.
func checkOwnerUse(file string, fn *ast.FuncDecl, recv string, handles ...string) []string {
	var problems []string
	allowed := map[*ast.SelectorExpr]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if id, ok := lit.Type.(*ast.Ident); !ok || !slices.Contains(handles, id.Name) {
			return true
		}
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "owner" {
					if sel, ok := kv.Value.(*ast.SelectorExpr); ok {
						allowed[sel] = true
					}
				}
			}
		}
		return true
	})
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "owner" {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv && !allowed[sel] {
				problems = append(problems, file+": "+funcKey(fn)+" uses the owner beyond a new handle's owner")
			}
		}
		return true
	})
	return problems
}

// ownerStateFields are the Container fields that hold an owner's state, by
// the state type: the owner view embeds the type, its constructor binds the
// field.
var ownerStateFields = map[string]string{
	"osdFleetState":   "fleet",
	"membershipState": "members",
	"authLedgerState": "ledger",
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
