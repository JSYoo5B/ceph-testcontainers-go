package monstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// monStoreAdapterFiles are the production files of the MON store adapters.
var monStoreAdapterFiles = []string{"monmap.go", "inspect_container.go", "inspect_engine.go", "json.go"}

// monStoreAdapterSymbols are the package-level names the adapter files add.
// Production code outside those files must not mention them until a slice adds
// a caller and updates this table in its own diff.
var monStoreAdapterSymbols = []string{
	"monStoreCLI", "monStoreMonmapCLI", "decodeMonStoreMember", "parseMonStoreMap", "parseMonStoreMonDump", "parseMonStoreQuorumStatus",
	"monStoreMonDumpArgv", "monStoreQuorumStatusArgv", "monStoreInspectScript", "monStoreInspectScriptSource", "monStoreInspectRequest",
	"newMonStoreInspectRequest", "monStoreInspectEngine", "monStoreInspectLogs", "monStoreContainerInspector", "parseMonStoreListing",
	"monStoreJSONObject", "monStoreObjectOf", "monStoreJSONCheck", "monStoreInspectorLabel",
	"monStoreInspectDocker", "monStoreMobyEngine", "monStoreInspectCreateOptions", "readMonStoreInspectFrames",
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
	src := `package monstore
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

// monStoreEngineFile is the one adapter file that may import the moby client.
const monStoreEngineFile = "inspect_engine.go"

// TestMonStoreAdaptersHaveNoLockAndNoDockerClient pins that the adapters add
// no mutex, that only the engine file imports the moby client, and that none
// imports testcontainers: the engine has no default client, so its first caller
// supplies the constructor.
func TestMonStoreAdaptersHaveNoLockAndNoDockerClient(t *testing.T) {
	files := parseProductionFiles(t)
	for _, name := range monStoreAdapterFiles {
		parsed := files[name]
		for _, imp := range parsed.Imports {
			switch imp.Path.Value {
			case `"sync"`, `"github.com/testcontainers/testcontainers-go"`:
				t.Errorf("%s imports %s", name, imp.Path.Value)
			case `"github.com/moby/moby/client"`:
				if name != monStoreEngineFile {
					t.Errorf("%s imports %s", name, imp.Path.Value)
				}
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

// parseProductionFiles parses the package's non-test files by name.
func parseProductionFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	return parseProductionFilesIn(t, ".")
}

// parseProductionFilesIn parses the non-test files of dir by name.
func parseProductionFilesIn(t *testing.T, dir string) map[string]*ast.File {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*ast.File{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		out[filepath.Base(file)] = parsed
	}
	return out
}

// receiverTypeName returns the type name of a method receiver, or "".
func receiverTypeName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// TestPackageAddsNoMutex pins that no record, adapter or inspector of the
// package has a lock of its own: a State is used under the cluster's topology
// lock, so a mutex here would add a lock domain to the cluster's lock order.
func TestPackageAddsNoMutex(t *testing.T) {
	for name, parsed := range parseProductionFiles(t) {
		for _, imp := range parsed.Imports {
			if strings.HasPrefix(imp.Path.Value, `"sync`) {
				t.Errorf("%s imports %s", name, imp.Path.Value)
			}
		}
	}
}
