package ceph

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// This file keeps the inventory section of docs/design/shared-harness-boundary.md
// equal to the source. The note freezes nothing: it records what ceph/harness
// carries after the public move of issue 155 and what still names the old
// qualified ceph names, and a count that is copied by hand into a design note
// rots with the next refactor. Every row of the section is measured here,
// from the files, and compared with the table. The test runs without Docker.
//
// Refresh the table with
//
//	env -u GOROOT go test ./ceph -run '^TestSharedHarnessInventoryMatchesTheNote$' -v
//
// which logs each measured row, then edit the pinned commit of the section.

// harnessPackage is the directory of the shared-cluster and tenant harness,
// relative to the module root.
const harnessPackage = "ceph/harness"

// declaredNames returns the package-level names that src declares, and the
// exported declarations by kind. A method is counted as "<Receiver> method".
func declaredNames(t *testing.T, name, src string) (pkgLevel []string, exported map[string]int) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	exported = map[string]int{}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				pkgLevel = append(pkgLevel, d.Name.Name)
				if d.Name.IsExported() {
					exported["function"]++
				}
				continue
			}
			if !d.Name.IsExported() {
				continue
			}
			recv := d.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			if id, ok := recv.(*ast.Ident); ok {
				exported[id.Name+" method"]++
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					pkgLevel = append(pkgLevel, s.Name.Name)
					if s.Name.IsExported() {
						exported["type"]++
					}
				case *ast.ValueSpec:
					for _, n := range s.Names {
						pkgLevel = append(pkgLevel, n.Name)
						if n.IsExported() {
							exported["constant or variable"]++
						}
					}
				}
			}
		}
	}
	return pkgLevel, exported
}

// unresolvedNames returns the identifiers of src that the file itself does
// not declare: package-level names of other files, imports and the universe.
// A local variable that shadows a package-level name is resolved by the
// parser, so it is not reported.
func unresolvedNames(t *testing.T, name, src string) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, id := range file.Unresolved {
		out[id.Name] = true
	}
	return out
}

// privateReach returns the unexported package-level names of the other files of
// the package that harness files name, sorted. Exported names are public API
// that ceph/harness may use; the unexported ones need a registration, a port
// method or equal code in the new package.
func privateReach(t *testing.T, harness map[string]string, others map[string]string) []string {
	t.Helper()
	declaredElsewhere := map[string]bool{}
	for name, src := range others {
		names, _ := declaredNames(t, name, src)
		for _, n := range names {
			declaredElsewhere[n] = true
		}
	}
	reach := map[string]bool{}
	for name, src := range harness {
		for n := range unresolvedNames(t, name, src) {
			if declaredElsewhere[n] && !ast.IsExported(n) {
				reach[n] = true
			}
		}
	}
	return sortedKeys(reach)
}

// movedNameReference matches a qualified use of one of the 16 package-level
// names that ceph/harness took over, under their old package. After the move no
// file may use one, because the names were removed without an alias.
var movedNameReference = regexp.MustCompile(`\bceph\.(SharedCluster|NewShared|RunTests|SharedOption|LeaseOption|WithSharedRunOptions|WithMaxTenants|WithMaxRecycles|WithKeepOnTenantFailure|SharedStats|Tenant|TenantAccess|TenantReadOnly|TenantReadWrite|SharedPool|ObserveTenant)\b`)

// countReferences counts, in the Go files under root that match want, the
// matches of re and the files that hold at least one.
func countReferences(t *testing.T, root string, want func(path string) bool, re *regexp.Regexp) (matches, files int) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || !want(filepath.ToSlash(path)) {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		n := len(re.FindAll(b, -1))
		matches += n
		if n > 0 {
			files++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return matches, files
}

// moduleImportsOf returns the module-internal import paths (without the module
// prefix) of the non-test files of dir, sorted.
func moduleImportsOf(t *testing.T, dir string) []string {
	t.Helper()
	set := map[string]bool{}
	for name, src := range nonTestSources(t, dir) {
		f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if rest, ok := strings.CutPrefix(path, modulePath+"/"); ok {
				set[rest] = true
			}
		}
	}
	return sortedKeys(set)
}

// measureHarnessInventory returns the rows of the inventory table, measured at
// repoRoot (the module root).
func measureHarnessInventory(t *testing.T, repoRoot string) map[string]string {
	t.Helper()
	harnessDir := filepath.Join(repoRoot, filepath.FromSlash(harnessPackage))
	rows := map[string]string{}

	exported := map[string]int{}
	for name, src := range nonTestSources(t, harnessDir) {
		_, e := declaredNames(t, name, src)
		for k, v := range e {
			exported[k] += v
		}
	}
	total := 0
	for _, kind := range []struct{ key, row string }{
		{"type", "exported types"},
		{"constant or variable", "exported constants"},
		{"function", "exported package functions"},
		{"SharedCluster method", "exported SharedCluster methods"},
		{"Tenant method", "exported Tenant methods"},
	} {
		rows[kind.row+" in ceph/harness"] = strconv.Itoa(exported[kind.key])
		total += exported[kind.key]
	}
	rows["exported declarations in ceph/harness"] = strconv.Itoa(total)

	rows["module packages that ceph/harness imports"] = strings.Join(moduleImportsOf(t, harnessDir), ", ")

	var importers []string
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") && path != repoRoot {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), "\""+modulePath+"/"+harnessPackage+"\"") {
			rel, _ := filepath.Rel(repoRoot, filepath.Dir(path))
			if rel = filepath.ToSlash(rel); !slices.Contains(importers, rel) {
				importers = append(importers, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(importers)
	rows["module packages that import ceph/harness"] = strings.Join(importers, ", ")

	for _, service := range []string{"rbd", "cephfs", "rgw"} {
		adapter := map[string]string{"tenant.go": nonTestSources(t, filepath.Join(repoRoot, service, "harness"))["tenant.go"]}
		rest := nonTestSources(t, filepath.Join(repoRoot, service))
		rows["private names of package "+service+" that its harness tenant.go names"] = strings.Join(privateReach(t, adapter, rest), ", ")
	}

	under := func(prefix string) func(string) bool {
		return func(path string) bool { return strings.Contains(path, prefix) }
	}
	for _, dir := range []string{"ceph", "rbd", "cephfs", "rgw", "internal/servicehook", "internal/integration"} {
		matches, files := countReferences(t, filepath.Join(repoRoot, filepath.FromSlash(dir)), under(dir+"/"), movedNameReference)
		rows["references to moved names under the old package in "+dir] = fmt.Sprintf("%d in %d files", matches, files)
	}

	tenantCluster := regexp.MustCompile(`\btesthelper\.TestTenantCluster\(`)
	matches, files := countReferences(t, filepath.Join(repoRoot, "internal", "integration"), func(string) bool { return true }, tenantCluster)
	rows["testhelper.TestTenantCluster calls in internal/integration"] = fmt.Sprintf("%d in %d files", matches, files)

	nonTest := func(path string) bool { return !strings.HasSuffix(path, "_test.go") }
	count := func(re *regexp.Regexp, dirs ...string) string {
		total := 0
		for _, dir := range dirs {
			n, _ := countReferences(t, filepath.Join(repoRoot, filepath.FromSlash(dir)), nonTest, re)
			total += n
		}
		return strconv.Itoa(total)
	}
	rows["servicehook.RegisterSharedService calls in rbd, cephfs and rgw"] = count(regexp.MustCompile(`\bRegisterSharedService\(`), "rbd", "cephfs", "rgw", "internal", "ceph")
	rows["servicehook.Support calls in rbd, cephfs and rgw harness packages"] = count(regexp.MustCompile(`\bservicehook\.Support\(`), "rbd/harness", "cephfs/harness", "rgw/harness")
	rows["port RequireService calls in rbd, cephfs and rgw harness packages"] = count(regexp.MustCompile(`\.RequireService\(`), "rbd/harness", "cephfs/harness", "rgw/harness")
	return rows
}

// inventoryRows returns the rows of the inventory section of the note as
// key -> value. A row is "| `key` | `value` |" or "| `key` | value |"; the
// section ends at the next level-2 heading.
func inventoryRows(doc string) (map[string]string, error) {
	const heading = "## Inventory at `"
	start := strings.Index(doc, "\n"+heading)
	if start < 0 {
		return nil, fmt.Errorf("the note has no section %q", heading)
	}
	section := doc[start+1+len(heading):]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	row := regexp.MustCompile(`(?m)^\| ([^|]+?) \| ([^|]*?) \|$`)
	rows := map[string]string{}
	for _, m := range row.FindAllStringSubmatch(section, -1) {
		key := strings.Trim(m[1], " ")
		if key == "Measure" || strings.HasPrefix(key, "---") {
			continue
		}
		if _, dup := rows[key]; dup {
			return nil, fmt.Errorf("the inventory names %q twice", key)
		}
		rows[key] = strings.Trim(m[2], " `")
	}
	return rows, nil
}

func compareInventory(measured, documented map[string]string) error {
	var problems []string
	for _, key := range sortedKeys(measured) {
		got, ok := documented[key]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("row %q is missing (measured %q)", key, measured[key]))
		case got != measured[key]:
			problems = append(problems, fmt.Sprintf("row %q says %q, measured %q", key, got, measured[key]))
		}
	}
	for _, key := range sortedKeys(documented) {
		if _, ok := measured[key]; !ok {
			problems = append(problems, fmt.Sprintf("row %q is not measured", key))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "\n"))
	}
	return nil
}

func TestSharedHarnessInventoryMatchesTheNote(t *testing.T) {
	measured := measureHarnessInventory(t, "..")
	for _, key := range sortedKeys(measured) {
		t.Logf("| %s | %s |", key, measured[key])
	}
	b, err := os.ReadFile(filepath.Join("..", "docs", "design", "shared-harness-boundary.md"))
	if err != nil {
		t.Fatal(err)
	}
	documented, err := inventoryRows(string(b))
	if err != nil {
		t.Fatal(err)
	}
	if err := compareInventory(measured, documented); err != nil {
		t.Fatalf("docs/design/shared-harness-boundary.md, section \"Inventory at\", differs from the source:\n%v\nrefresh the table and its pinned commit", err)
	}
}

// Negative controls: each measurement reports the drift it exists to catch.

func TestHarnessInventoryCountsExportedDeclarationsByKind(t *testing.T) {
	src := `package ceph
type Tenant struct{}
type hidden struct{}
const Pool = "x"
func Run() {}
func helper() {}
func (t *Tenant) ID() string { return "" }
func (t Tenant) other() {}
func (s *SharedCluster) Lease() {}
`
	names, exported := declaredNames(t, "x.go", src)
	want := map[string]int{"type": 1, "constant or variable": 1, "function": 1, "Tenant method": 1, "SharedCluster method": 1}
	if fmt.Sprint(exported) != fmt.Sprint(want) {
		t.Fatalf("exported = %v, want %v", exported, want)
	}
	if !slices.Contains(names, "hidden") || !slices.Contains(names, "helper") || slices.Contains(names, "ID") {
		t.Fatalf("package-level names = %v", names)
	}
}

func TestHarnessPrivateReachIgnoresShadowsAndExportedNames(t *testing.T) {
	harness := map[string]string{"h.go": `package ceph
func use() error {
	command := 1
	_ = command
	_ = ClientConfig{}
	return invalidRequest("x")
}
`}
	others := map[string]string{"o.go": `package ceph
func invalidRequest(string) error { return nil }
func command() {}
type ClientConfig struct{}
`}
	got := privateReach(t, harness, others)
	if want := []string{"invalidRequest"}; !slices.Equal(got, want) {
		t.Fatalf("reach = %v, want %v (a shadowing local and an exported type are not private reach)", got, want)
	}
	harness["h.go"] = strings.Replace(harness["h.go"], "command := 1\n\t_ = command", "command()", 1)
	if got := privateReach(t, harness, others); !slices.Equal(got, []string{"command", "invalidRequest"}) {
		t.Fatalf("reach = %v, want command and invalidRequest", got)
	}
}

func TestHarnessInventoryComparisonReportsEveryDrift(t *testing.T) {
	measured := map[string]string{"a": "1", "b": "2"}
	for _, tc := range []struct {
		name       string
		documented map[string]string
		want       string
	}{
		{"equal", map[string]string{"a": "1", "b": "2"}, ""},
		{"changed value", map[string]string{"a": "1", "b": "3"}, `row "b" says "3", measured "2"`},
		{"missing row", map[string]string{"a": "1"}, `row "b" is missing`},
		{"extra row", map[string]string{"a": "1", "b": "2", "c": "9"}, `row "c" is not measured`},
	} {
		err := compareInventory(measured, tc.documented)
		if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}

func TestInventoryRowsReadsOnlyTheInventorySection(t *testing.T) {
	doc := "# T\n\n## Before\n\n| x | 1 |\n\n## Inventory at `abc`\n\n| Measure | Value |\n| --- | --- |\n| row one | `a, b` |\n| row two | 3 in 2 files |\n\n## After\n\n| y | 2 |\n"
	rows, err := inventoryRows(doc)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rows) != fmt.Sprint(map[string]string{"row one": "a, b", "row two": "3 in 2 files"}) {
		t.Fatalf("rows = %v", rows)
	}
	if _, err := inventoryRows("# T\n"); err == nil {
		t.Fatal("a note without the section was accepted")
	}
	if _, err := inventoryRows(strings.Replace(doc, "| row two |", "| row one |", 1)); err == nil {
		t.Fatal("a duplicated row was accepted")
	}
}
