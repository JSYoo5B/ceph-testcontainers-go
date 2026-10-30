package ceph

import (
	"go/ast"
	"go/token"
	"slices"
	"testing"
)

// monitorTopologyBinding reports whether fn is Container.monitorTopology, the
// one function that hands the membership state's addresses to the topology.
// The roster and bootstrap field guards admit its uses there;
// TestMonitorTopologyBindsEachStateOnce pins what it does.
func monitorTopologyBinding(fn *ast.FuncDecl) bool {
	return fn != nil && receiverTypeName(fn) == "Container" && fn.Name.Name == "monitorTopology"
}

// inMonitorTopologyBinding reports whether pos lies in Container.monitorTopology.
func inMonitorTopologyBinding(file *ast.File, pos token.Pos) bool {
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && monitorTopologyBinding(fn) && fn.Pos() <= pos && pos < fn.End() {
			return true
		}
	}
	return false
}

// monitorTopologyBound is the state that Container.monitorTopology binds by
// address. The roster and the bootstrap record keep their field guards, which
// also hold for the topology's methods because they use the same field
// names; monitorTerminated is audited by TestLifecycleFlagsHaveAuditedWritersAndReaders.
var monitorTopologyBound = []string{"bootstrap", "monitorTerminated", "roster"}

// TestMonitorTopologyBindsEachStateOnce guards the topology's construction
// and back reference: Container.monitorTopology is a single return of one
// monitorTopology literal that binds each state in monitorTopologyBound once,
// as &c.<same field>; no other function builds a monitorTopology; and its
// methods use owner only as the owner of a MON or MGR handle they create.
func TestMonitorTopologyBindsEachStateOnce(t *testing.T) {
	var problems []string
	constructors := 0
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if lit, ok := n.(*ast.CompositeLit); ok {
					if id, ok := lit.Type.(*ast.Ident); ok && id.Name == "monitorTopology" {
						constructors++
						if !monitorTopologyBinding(fn) {
							problems = append(problems, file+": "+funcKey(fn)+" builds a monitorTopology")
						}
					}
				}
				return true
			})
			if monitorTopologyBinding(fn) {
				problems = append(problems, checkStateBinding(fn, "monitorTopology", monitorTopologyBound)...)
			}
			if receiverTypeName(fn) == "monitorTopology" {
				problems = append(problems, checkOwnerUse(file, fn, "t", "MonitorContainer", "ManagerContainer")...)
			}
		}
	}
	if constructors != 1 {
		problems = append(problems, "want exactly one monitorTopology literal")
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("the monitor topology binding is not pinned: %v", problems)
	}
}
