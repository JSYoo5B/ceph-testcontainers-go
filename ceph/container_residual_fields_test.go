package ceph

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// residualFieldVerdicts is the disposition of every field the Container struct
// still declares. The verdict kinds are:
//
//	public   part of the public type (the embedded first MON)
//	lock     the cluster mutex that every lock-order citation is measured from
//	flag     a boolean that mu guards and that the admission paths read
//	atomic   one atomic that sharedGate owns by name
//	options  immutable options after Run
//	seam     a test seam with one reader and no state of its own
//	owner    a component with its own type, file and guards
//
// A new field must be added here with a verdict and a row in the table of
// docs/design/container-decomposition/residual-fields.md, or the test fails.
// This is the "owner or stays" ledger of issue #146, not a method-count target.
var residualFieldVerdicts = map[string]string{
	"testcontainers.Container": "public",
	"mu":                       "lock",
	"controlPlane":             "owner",
	"toolkit":                  "owner",
	"bootstrap":                "owner",
	"authRecords":              "owner",
	"keyRotationDocker":        "seam",
	"startup":                  "owner",
	"settings":                 "options",
	"roster":                   "owner",
	"extensions":               "owner",
	"nets":                     "owner",
	"osdHandles":               "owner",
	"stores":                   "owner",
	"monStores":                "owner",
	"runDir":                   "owner",
	"portLeases":               "owner",
	"closed":                   "flag",
	"monitorTerminated":        "flag",
	"nativeCmds":               "owner",
	"faults":                   "owner",
	"debugLevels":              "owner",
	"daemonRunner":             "seam",
	"sharing":                  "atomic",
}

// containerFieldTypes returns the declared fields of the struct typeName in
// src as name -> source text of the type. An embedded field is named by its
// type text.
func containerFieldTypes(t *testing.T, src, typeName string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != typeName {
				continue
			}
			st := ts.Type.(*ast.StructType)
			for _, f := range st.Fields.List {
				typ := src[fset.Position(f.Type.Pos()).Offset:fset.Position(f.Type.End()).Offset]
				if len(f.Names) == 0 {
					out[typ] = typ
				}
				for _, n := range f.Names {
					out[n.Name] = typ
				}
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("struct %s not found", typeName)
	}
	return out
}

func readContainerSource(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("ceph.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// residualDocFields returns the field names that the verdict rows of the
// residual-field record name in their first column, in backticks. The record
// is docs/design/container-decomposition/residual-fields.md and holds nothing
// else with such rows.
func residualDocFields(doc string) []string {
	row := regexp.MustCompile("(?m)^\\| `([A-Za-z.]+)` \\|")
	var names []string
	for _, m := range row.FindAllStringSubmatch(doc, -1) {
		names = append(names, m[1])
	}
	slices.Sort(names)
	return names
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func checkResidualFields(fields map[string]string, verdicts map[string]string) error {
	got, want := sortedKeys(fields), sortedKeys(verdicts)
	if !slices.Equal(got, want) {
		return fmt.Errorf("Container declares %v, the ledger dispositions %v", got, want)
	}
	for name, typ := range fields {
		switch verdicts[name] {
		case "lock":
			if typ != "sync.Mutex" {
				return fmt.Errorf("%s is %s, the one lock must be sync.Mutex", name, typ)
			}
		case "atomic":
			if typ != "atomic.Int32" {
				return fmt.Errorf("%s is %s, the one atomic must be atomic.Int32", name, typ)
			}
		case "flag":
			if typ != "bool" {
				return fmt.Errorf("%s is %s, a flag must be bool", name, typ)
			}
		case "owner", "seam", "options", "public":
		default:
			return fmt.Errorf("%s has unknown verdict %q", name, verdicts[name])
		}
		// Only the lock and the atomic may be a sync primitive; a component
		// owns any other lock (TestNativeStateAddsNoMutex counts those).
		if (strings.HasPrefix(typ, "sync.") || strings.HasPrefix(typ, "atomic.")) && verdicts[name] != "lock" && verdicts[name] != "atomic" {
			return fmt.Errorf("%s (%s) is a sync primitive outside the lock and the atomic", name, typ)
		}
	}
	return nil
}

// TestContainerResidualFieldsAreAllDispositioned pins the full field set of
// Container: each field has a verdict, and Container itself declares exactly
// one mutex (mu) and one atomic (sharing).
func TestContainerResidualFieldsAreAllDispositioned(t *testing.T) {
	fields := containerFieldTypes(t, readContainerSource(t), "Container")
	if err := checkResidualFields(fields, residualFieldVerdicts); err != nil {
		t.Fatal(err)
	}
}

func TestContainerResidualFieldCheckRejectsDrift(t *testing.T) {
	base := containerFieldTypes(t, readContainerSource(t), "Container")
	with := func(edit func(map[string]string)) map[string]string {
		out := map[string]string{}
		for k, v := range base {
			out[k] = v
		}
		edit(out)
		return out
	}
	cases := map[string]map[string]string{
		"a new field":           with(func(m map[string]string) { m["extra"] = "int" }),
		"a removed field":       with(func(m map[string]string) { delete(m, "roster") }),
		"a second mutex":        with(func(m map[string]string) { m["controlPlane"] = "sync.RWMutex" }),
		"mu as an RWMutex":      with(func(m map[string]string) { m["mu"] = "sync.RWMutex" }),
		"the flag as an atomic": with(func(m map[string]string) { m["closed"] = "atomic.Bool" }),
		"sharing as a mutex":    with(func(m map[string]string) { m["sharing"] = "sync.Mutex" }),
	}
	for name, fields := range cases {
		if checkResidualFields(fields, residualFieldVerdicts) == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if checkResidualFields(base, map[string]string{}) == nil {
		t.Error("an empty ledger was accepted")
	}
}

// TestContainerResidualFieldsAreInTheDesignNote requires one table row per
// field in the residual-field record. The record is found by name in the
// decomposition directory; its absence fails the test.
func TestContainerResidualFieldsAreInTheDesignNote(t *testing.T) {
	got, err := residualRecordFields(repoFS())
	if err != nil {
		t.Fatal(err)
	}
	want := sortedKeys(residualFieldVerdicts)
	if !slices.Equal(got, want) {
		t.Fatalf("the residual-field record's verdict table names %v, the ledger %v", got, want)
	}
}

func TestResidualDocFieldsReadsTheVerdictRows(t *testing.T) {
	doc := "# Residual fields\n| `mu` | y |\n| `closed` | z |\nprose | `x` |\n| Measure | Value |\n"
	if got := residualDocFields(doc); !slices.Equal(got, []string{"closed", "mu"}) {
		t.Fatalf("rows = %v", got)
	}
	if residualDocFields("no rows") != nil {
		t.Fatal("a document without rows yielded rows")
	}
}

// lifecycleAccess is one use of the closed or monitorTerminated flag.
type lifecycleAccess struct {
	field string
	fn    string // enclosing function, "Container.name" for a method
	write bool
}

// lifecycleAccesses lists the selector uses of the two flags in the given
// parsed files, keyed by file, and the functions that set them in a composite
// literal of Container.
func lifecycleAccesses(files map[string]*ast.File) (accesses []lifecycleAccess, literalKeys []string) {
	flags := map[string]bool{"closed": true, "monitorTerminated": true}
	for file, parsed := range files {
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if recv := receiverTypeName(fn); recv != "" {
				name = recv + "." + name
			}
			if recv := receiverTypeName(fn); recv != "Container" {
				// Other types have their own closed fields. Only the
				// composite literals of Container matter here.
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					lit, ok := n.(*ast.CompositeLit)
					if !ok {
						return true
					}
					if id, ok := lit.Type.(*ast.Ident); ok && id.Name == "Container" {
						for _, el := range lit.Elts {
							if kv, ok := el.(*ast.KeyValueExpr); ok {
								if k, ok := kv.Key.(*ast.Ident); ok && flags[k.Name] {
									literalKeys = append(literalKeys, file+":"+name+":"+k.Name)
								}
							}
						}
					}
					return true
				})
				continue
			}
			written := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && flags[sel.Sel.Name] {
							written[sel] = true
						}
					}
				case *ast.UnaryExpr:
					if sel, ok := n.X.(*ast.SelectorExpr); ok && n.Op == token.AND && flags[sel.Sel.Name] {
						written[sel] = true
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && flags[sel.Sel.Name] {
					if id, ok := sel.X.(*ast.Ident); ok && (id.Name == "c" || id.Name == "cluster") {
						accesses = append(accesses, lifecycleAccess{sel.Sel.Name, name, written[sel]})
					}
				}
				return true
			})
		}
	}
	return accesses, literalKeys
}

// lifecycleWriters are the only functions that assign the two flags. closed is
// set once, at the top of terminate, under mu (ceph/ceph.go:957);
// monitorTerminated is set by terminate when it removes the first MON
// (ceph/ceph.go:1024) and by removeMonitor when the first MON is removed
// (ceph/topology.go:335).
var lifecycleWriters = []string{
	"closed:Container.terminate",
	"monitorTerminated:Container.removeMonitor",
	"monitorTerminated:Container.terminate",
}

// monitorTerminatedReaders are the Container methods that read the flag, each
// to learn whether the embedded first MON still stands. A new reader needs a
// review of whether it holds mu. removeMonitor only writes the flag, so it is
// in lifecycleWriters and not here.
var monitorTerminatedReaders = []string{
	"Container.AddMonitor",
	"Container.currentMonitor",
	"Container.diagnosticsSnapshot",
	"Container.monitorConfigTargets",
	"Container.monitorsLocked",
	"Container.ownsDaemon",
	"Container.sharedDaemons",
	"Container.terminate",
}

// unitClusterLiteral is the only Container literal that names a flag; the
// other Container literal is Run's, which leaves both false.
var unitClusterLiteral = []string{"unit_access.go:unitAccess.UnitCluster:closed"}

func summarizeLifecycle(accesses []lifecycleAccess) (writers, readers []string) {
	for _, a := range accesses {
		switch {
		case a.write:
			writers = append(writers, a.field+":"+a.fn)
		case a.field == "monitorTerminated":
			readers = append(readers, a.fn)
		}
	}
	slices.Sort(writers)
	slices.Sort(readers)
	return slices.Compact(writers), slices.Compact(readers)
}

// TestLifecycleFlagsHaveAuditedWritersAndReaders pins who assigns closed and
// monitorTerminated and who reads monitorTerminated. closed has 44 reader uses
// in 27 files by design (it is the admission flag) and is not enumerated.
func TestLifecycleFlagsHaveAuditedWritersAndReaders(t *testing.T) {
	accesses, literals := lifecycleAccesses(parseProductionFiles(t))
	writers, readers := summarizeLifecycle(accesses)
	if !slices.Equal(writers, lifecycleWriters) {
		t.Errorf("flag writers = %v, audited %v", writers, lifecycleWriters)
	}
	if !slices.Equal(readers, monitorTerminatedReaders) {
		t.Errorf("monitorTerminated readers = %v, audited %v", readers, monitorTerminatedReaders)
	}
	slices.Sort(literals)
	if !slices.Equal(literals, unitClusterLiteral) {
		t.Errorf("Container literals naming a flag = %v, audited %v", literals, unitClusterLiteral)
	}
}

func parseSnippet(t *testing.T, src string) map[string]*ast.File {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), "snippet.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*ast.File{"snippet.go": parsed}
}

func TestLifecycleAccessScannerSeesWritersReadersAndLiterals(t *testing.T) {
	src := `package ceph
type Container struct{ closed, monitorTerminated bool }
type other struct{ closed bool }
func (c *Container) terminate() { c.closed = true; c.monitorTerminated = true }
func (c *Container) peek() bool { return !c.monitorTerminated }
func (c *Container) addr() *bool { return &c.monitorTerminated }
func (o *other) close() { o.closed = true }
func build() *Container { return &Container{closed: true} }
`
	accesses, literals := lifecycleAccesses(parseSnippet(t, src))
	writers, readers := summarizeLifecycle(accesses)
	if want := []string{"closed:Container.terminate", "monitorTerminated:Container.addr", "monitorTerminated:Container.terminate"}; !slices.Equal(writers, want) {
		t.Errorf("writers = %v, want %v", writers, want)
	}
	if want := []string{"Container.peek"}; !slices.Equal(readers, want) {
		t.Errorf("readers = %v, want %v", readers, want)
	}
	if want := []string{"snippet.go:build:closed"}; !slices.Equal(literals, want) {
		t.Errorf("literals = %v, want %v", literals, want)
	}
}
