package fsbase

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"
)

// TestAdmitHoldsNoPredicateAndTakesTheLocksInOrder pins the division of work
// of the admission lifetime: Admit takes setupMu through LockMutex and then the
// topology mutex, once each and in that order, and holds no predicate: it does
// not read the registry, the native identity, the descriptor name or the
// original pool names, and does not test Closed. Each operation's own check
// does that (cephfs TestAdmissionLifetimeHasOneOwnerAndKeepsPredicatesLocal).
// admission.go adds no mutex type.
func TestAdmitHoldsNoPredicateAndTakesTheLocksInOrder(t *testing.T) {
	var admit *ast.FuncDecl
	for _, parsed := range parseProductionFiles(t) {
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "Admit" && receiverName(fn) == "Core" {
				admit = fn
			}
		}
	}
	if admit == nil {
		t.Fatal("Core.Admit is missing")
	}
	var order []string
	ast.Inspect(admit.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			switch calleeName(node) {
			case "LockMutex":
				if len(node.Args) == 2 && strings.HasSuffix(exprString(node.Args[1]), ".setupMu") {
					order = append(order, "setupMu")
				} else {
					t.Error("Admit locks a mutex other than setupMu")
				}
			case "LockTopology", "Lock":
				order = append(order, "topology")
			case "Closed", "Registered", "DescriptorIntact", "Native", "live":
				t.Errorf("Admit calls %s: ownership predicates stay in each operation", calleeName(node))
			}
		case *ast.SelectorExpr:
			if slices.Contains([]string{"byName", "native", "descriptorName", "metadataPool", "dataPool", "live", "config", "roster"}, node.Sel.Name) {
				t.Errorf("Admit reads %s: ownership predicates stay in each operation", node.Sel.Name)
			}
		}
		return true
	})
	if !slices.Equal(order, []string{"setupMu", "topology"}) {
		t.Errorf("lock acquisitions of Admit changed (setupMu must come first, once each): %v", order)
	}
	src, err := parser.ParseFile(token.NewFileSet(), "admission.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range src.Imports {
		if strings.Contains(imp.Path.Value, `"sync`) {
			t.Error("admission adds a mutex type")
		}
	}
}

func exprString(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return exprString(e.X) + "." + e.Sel.Name
	case *ast.UnaryExpr:
		return e.Op.String() + exprString(e.X)
	}
	return ""
}
