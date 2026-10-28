package ceph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"
)

// monStoreAdapterFiles are the production files of the MON store adapters.
var monStoreAdapterFiles = []string{"mon_store_monmap.go", "mon_store_inspect_container.go", "mon_store_json.go"}

// monStoreAdapterSymbols are the package-level names the adapter files add.
// Production code outside those files must not mention them until a slice adds
// a caller and updates this table in its own diff.
var monStoreAdapterSymbols = []string{
	"monStoreCLI", "monStoreMonmapCLI", "decodeMonStoreMember", "parseMonStoreMap", "parseMonStoreMonDump", "parseMonStoreQuorumStatus",
	"monStoreMonDumpArgv", "monStoreQuorumStatusArgv", "monStoreInspectScript", "monStoreInspectScriptSource", "monStoreInspectRequest",
	"newMonStoreInspectRequest", "monStoreInspectEngine", "monStoreInspectLogs", "monStoreContainerInspector", "parseMonStoreListing",
	"monStoreJSONObject", "monStoreObjectOf", "monStoreJSONCheck", "monStoreInspectorLabel",
}

func monStoreAdapterProblems(files map[string]*ast.File) (problems []string) {
	for file, parsed := range files {
		if slices.Contains(monStoreAdapterFiles, file) {
			continue
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && slices.Contains(monStoreAdapterSymbols, id.Name) {
				problems = append(problems, file+" mentions "+id.Name)
			}
			return true
		})
	}
	return problems
}

func TestMonStoreAdaptersHaveNoProductionCaller(t *testing.T) {
	files := parseProductionFiles(t)
	for _, name := range monStoreAdapterFiles {
		if _, ok := files[name]; !ok {
			t.Fatalf("%s not found: the guard is vacuous", name)
		}
	}
	if problems := monStoreAdapterProblems(files); len(problems) != 0 {
		t.Fatalf("a MON store adapter is used in production: %v", problems)
	}
}

func TestMonStoreAdaptersGuardDetectsACaller(t *testing.T) {
	src := `package ceph
func use() { _, _ = decodeMonStoreMember(nil, nil, ""); _ = monStoreContainerInspector{} }
`
	parsed, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if problems := monStoreAdapterProblems(map[string]*ast.File{"synthetic.go": parsed}); len(problems) != 2 {
		t.Fatalf("problems %v", problems)
	}
}

// TestMonStoreAdaptersDeclareEverySymbolOfTheTable keeps the table honest: a
// renamed or removed symbol must leave the table in the same diff, or the
// guard would silently stop covering it.
func TestMonStoreAdaptersDeclareEverySymbolOfTheTable(t *testing.T) {
	declared := map[string]bool{}
	for _, name := range monStoreAdapterFiles {
		for _, decl := range parseProductionFiles(t)[name].Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					declared[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						declared[s.Name.Name] = true
					case *ast.ValueSpec:
						for _, n := range s.Names {
							declared[n.Name] = true
						}
					}
				}
			}
		}
	}
	for _, symbol := range monStoreAdapterSymbols {
		if !declared[symbol] {
			t.Errorf("%s is in the table but no adapter file declares it", symbol)
		}
	}
}

// TestMonStoreAdaptersHaveNoLockAndNoDockerClient pins that the adapters add
// no mutex and import no Docker client: the engine is an interface and the
// first implementation is a later slice.
func TestMonStoreAdaptersHaveNoLockAndNoDockerClient(t *testing.T) {
	files := parseProductionFiles(t)
	for _, name := range monStoreAdapterFiles {
		parsed := files[name]
		for _, imp := range parsed.Imports {
			switch imp.Path.Value {
			case `"sync"`, `"github.com/moby/moby/client"`, `"github.com/testcontainers/testcontainers-go"`:
				t.Errorf("%s imports %s", name, imp.Path.Value)
			}
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sync" {
					t.Errorf("%s uses sync.%s", name, sel.Sel.Name)
				}
			}
			return true
		})
	}
}
