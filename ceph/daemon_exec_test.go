package ceph

import (
	"go/ast"
	"testing"
)

// TestDockerExecGoesThroughDaemonExecutor pins the Docker exec API to its one
// owner. A daemon-container command whose end RotateKey must confirm runs
// through daemonExecutor, which returns an unconfirmed exec's ID for the
// caller's fence; a second caller of ExecCreate, ExecStart or ExecInspect
// would bypass that record.
func TestDockerExecGoesThroughDaemonExecutor(t *testing.T) {
	owners := map[string]bool{"daemonExecutor.run": true, "daemonExecutor.finish": true, "waitDaemonExec": true}
	found := map[string]bool{}
	for file, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := funcKey(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "ExecCreate", "ExecStart", "ExecInspect":
					if !owners[key] {
						t.Errorf("%s: %s calls the Docker exec API %s outside daemonExecutor", file, key, sel.Sel.Name)
					}
					found[key] = true
				}
				return true
			})
		}
	}
	for owner := range owners {
		if !found[owner] {
			t.Errorf("%s no longer calls the Docker exec API; update the pinned owners", owner)
		}
	}
}
