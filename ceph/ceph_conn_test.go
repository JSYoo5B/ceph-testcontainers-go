package ceph

import (
	"go/ast"
	"go/token"
	"slices"
	"strings"
	"testing"
)

// TestCephConnSelectsTheControlContainerThenTheFirstMON pins the selection the
// transport kept from Container: the published control container first, the
// first MON otherwise, and a refusal for a missing cluster.
func TestCephConnSelectsTheControlContainerThenTheFirstMON(t *testing.T) {
	mon, control := &controlStateFake{id: "mon"}, &controlStateFake{id: "control"}
	c := &Container{Container: mon}
	if got := c.conn().cliContainer(); got != mon {
		t.Fatalf("without a control container: %v, want the first MON", got)
	}
	if got, err := c.conn().controlContainer(t.Context()); err != nil || got != mon {
		t.Fatalf("without a control container: %v, %v", got, err)
	}
	c.controlPlane.publish(control)
	if got := c.conn().cliContainer(); got != control {
		t.Fatalf("with a control container: %v", got)
	}
	if got, err := c.conn().controlContainer(t.Context()); err != nil || got != control {
		t.Fatalf("with a control container: %v, %v", got, err)
	}
	var missing *Container
	if _, err := missing.conn().controlContainer(t.Context()); err == nil || !strings.Contains(err.Error(), "ceph cluster is unavailable") {
		t.Fatalf("a missing cluster: %v", err)
	}
	if _, err := missing.conn().ceph(t.Context(), "status"); err == nil {
		t.Fatal("a missing cluster ran a command")
	}
}

// cephConnMethods is the transport's whole method set. A domain query or a
// lifetime check added here fails, so the transport cannot grow into a second
// Container.
var cephConnMethods = []string{"ceph", "cliContainer", "controlContainer", "rotationCommand", "rotationResult", "supervisedLookup"}

// TestCephConnReachesTheControlPlaneOnlyThroughItsReaders guards the pointer
// that Container.conn hands out: in cephConn methods the control pointer is only
// compared with nil or the receiver of current/currentContext, no method
// assigns a field of the transport, and no method names Container's lock,
// lifetime flag or topology lock. With TestControlPlaneStateFieldIsOnlyTouchedByItsMethods
// (the pointer is built only in Container.conn) and
// TestMembershipControlStateIsNeverAddressed (no other address is taken), the
// control plane is still written only by its own methods.
func TestCephConnReachesTheControlPlaneOnlyThroughItsReaders(t *testing.T) {
	var methods, problems []string
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverTypeName(fn) != "cephConn" {
				continue
			}
			methods = append(methods, fn.Name.Name)
			allowed := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if method, ok := x.Fun.(*ast.SelectorExpr); ok && (method.Sel.Name == "current" || method.Sel.Name == "currentContext") {
						if recv, ok := method.X.(*ast.SelectorExpr); ok {
							allowed[recv] = true
						}
					}
				case *ast.BinaryExpr:
					if id, ok := x.Y.(*ast.Ident); ok && id.Name == "nil" && (x.Op == token.EQL || x.Op == token.NEQ) {
						if recv, ok := x.X.(*ast.SelectorExpr); ok {
							allowed[recv] = true
						}
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok {
							if id, ok := sel.X.(*ast.Ident); ok && id.Name == "n" {
								problems = append(problems, file+": "+fn.Name.Name+" assigns n."+sel.Sel.Name)
							}
						}
					}
				case *ast.Ident:
					switch x.Name {
					case "mu", "closed", "lockTopology", "Container":
						problems = append(problems, file+": "+fn.Name.Name+" names "+x.Name)
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && controlPlaneUse(sel) && !allowed[sel] {
					problems = append(problems, file+": "+fn.Name.Name+" uses n.control other than through current/currentContext")
				}
				return true
			})
		}
	}
	slices.Sort(methods)
	if !slices.Equal(methods, cephConnMethods) {
		problems = append(problems, "cephConn methods are "+strings.Join(methods, ",")+", want "+strings.Join(cephConnMethods, ","))
	}
	if len(problems) != 0 {
		slices.Sort(problems)
		t.Fatalf("the transport escapes its contract: %v", problems)
	}
}
