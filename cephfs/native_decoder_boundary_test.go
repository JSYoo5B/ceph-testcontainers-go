package cephfs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// assertPureDecoderFile guards the contract of a decoder file: it declares only
// the pinned functions and types, imports no package that can reach a lock, a
// command or Docker, and names none of the owner state. So the decoder cannot
// start reading the roster, the cluster or a container without a test failing.
func assertPureDecoderFile(t *testing.T, name string, wantFuncs, wantTypes, wantImports []string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var funcs, types, imports []string
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv != nil {
				t.Errorf("%s: method %s: a decoder file declares functions only", name, d.Name.Name)
			}
			funcs = append(funcs, d.Name.Name)
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					types = append(types, s.Name.Name)
				case *ast.ImportSpec:
					imports = append(imports, strings.Trim(s.Path.Value, `"`))
				}
			}
		}
	}
	slices.Sort(types)
	slices.Sort(imports)
	if !slices.Equal(funcs, wantFuncs) || !slices.Equal(types, wantTypes) {
		t.Errorf("%s declarations changed: funcs %v types %v", name, funcs, types)
	}
	if !slices.Equal(imports, wantImports) {
		t.Errorf("%s imports %v, pinned %v", name, imports, wantImports)
	}
	forbidden := []string{"Filesystem", "fs", "context", "ctx", "servicehook", "clusterutil", "testcontainers", "ceph", "sync", "roster", "members", "Inspect", "Lock", "Unlock", "LockMutex", "Container", "cluster", "setupMu", "Ceph", "readNativePools"}
	ast.Inspect(file, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && slices.Contains(forbidden, id.Name) {
			t.Errorf("%s names %q, which is owner state, a lock, a command or Docker", name, id.Name)
		}
		return true
	})
}

func TestColdAndReplacementDecodersArePureFunctionsOfTheirArguments(t *testing.T) {
	naming := "github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	assertPureDecoderFile(t, "cold_mds_decode.go",
		[]string{"decodeColdMDSMap"}, []string{"coldMDSMapExpectation"},
		[]string{"errors", naming, "slices"})
	assertPureDecoderFile(t, "mds_replacement_decode.go",
		[]string{"decodeMDSReplacementMap", "admitReplacementMDSRow", "checkReplacementMDSMembership", "replacementMDSScope", "replacementMDSRankReady", "checkReplacementMDSStandbys"},
		[]string{"replacementMDSFields", "replacementMDSFilesystem", "replacementMDSMap", "replacementMDSMapExpectation", "replacementMDSMapState", "replacementMDSSeen"},
		[]string{"errors", naming, "slices", "strconv"})
}

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
		"readMDSReplacementScope": {"addMDSReplacement", "addMDSReplacement", "observeMDSReplacement"},
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
