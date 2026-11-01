package cephfs

import (
	"go/ast"
	"reflect"
	"slices"
	"testing"
)

// TestColdAndReplacementReadsHaveOneDecode pins the caller tables of the two
// boundaries and the order in the replacement reader: both native queries come
// first, the decode is called once, and the reader ends by returning the decoded
// scope together with the context error.
func TestColdAndReplacementReadsHaveOneDecode(t *testing.T) {
	callers := map[string][]string{}
	var reader *ast.FuncDecl
	for _, parsed := range parseCephFSProductionFiles(t) {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "readMDSReplacementScope" {
				reader = fn
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch name := calleeName(call); name {
					case "decodeColdMDSMap", "decodeMDSReplacementMap", "readMDSReplacementScope":
						callers[name] = append(callers[name], fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	for _, list := range callers {
		slices.Sort(list)
	}
	want := map[string][]string{
		"decodeColdMDSMap":        {"verifyColdMDS"},
		"decodeMDSReplacementMap": {"readMDSReplacementScope"},
		"readMDSReplacementScope": {"addMDSReplacement", "admitMDSReplacement", "observeMDSReplacement"},
	}
	if !reflect.DeepEqual(callers, want) {
		t.Errorf("caller table changed: %v, pinned %v", callers, want)
	}
	if reader == nil {
		t.Fatal("readMDSReplacementScope is missing")
	}
	var order []string
	ast.Inspect(reader.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			switch name := calleeName(call); name {
			case "readNativePools", "Ceph", "decodeMDSReplacementMap":
				order = append(order, name)
			}
		}
		return true
	})
	if !slices.Equal(order, []string{"readNativePools", "Ceph", "decodeMDSReplacementMap"}) {
		t.Errorf("reader order changed: %v", order)
	}
	last, ok := reader.Body.List[len(reader.Body.List)-1].(*ast.ReturnStmt)
	if !ok || len(last.Results) != 3 {
		t.Fatal("reader must end with a three-value return")
	}
	if call, ok := last.Results[2].(*ast.CallExpr); !ok || calleeName(call) != "Err" {
		t.Error("reader must return the context error last, next to the decoded scope")
	}
}
