// Command testdoublecheck keeps docs/contributing/test-double-inventory.md in
// sync with the test-only code it describes, without Docker. It reads Go
// syntax only (no type information), so it states what it can see and no more:
//
//   - every exported top-level declaration of a package listed in the
//     testsupportcheck manifest's test_only_packages has a row in the
//     inventory's "Shared test-support packages" table;
//   - every non-test declaration outside those packages whose comment says it
//     exists for tests (a type, struct field, interface method, function or
//     variable whose doc or trailing comment says, for example, "replaced in
//     tests", "tests replace it" or "test-only"; the pattern is testComment) has a row in the "Runtime injection seams" table. A seam
//     without such a comment is not found, and the document says so;
//   - every symbol an inventory row names still exists, in the file the row
//     names;
//   - no non-test file outside the test-only packages declares a type or
//     function whose name contains fake, mock, stub or spy (case-insensitive):
//     such doubles are _test.go declarations or live in a test-only package.
//
// It does not enumerate package-local doubles in _test.go files: those are
// described by family, and a per-name list would only restate the source.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const (
	defaultInventory = "docs/contributing/test-double-inventory.md"
	defaultManifest  = "internal/tools/testsupportcheck/manifest.json"
	sharedHeading    = "Shared test-support packages"
	seamHeading      = "Runtime injection seams"
)

var (
	// testComment matches the phrases the module uses for an injection seam:
	// "replaced in tests", "tests replace it", "unit tests replace", "test
	// seam(s)", "test-only" and "unit-test hook". A mention of tests inside a longer phrase ("for tests that
	// check ...") does not match.
	testComment = regexp.MustCompile(`(?i)\b(in|for the|for) (unit )?tests([.,;)]|\s*$)|\b(unit )?tests replace\b|\btest[- ]seams?\b|\btest-only\b|\bunit-test hook\b`)
	doubleName  = regexp.MustCompile(`(?i)fake|mock|stub|spy`)
	// rowSymbol is a code span of the form path/to/file.go#Symbol.
	rowSymbol = regexp.MustCompile("`([A-Za-z0-9_./-]+\\.go)#([A-Za-z0-9_.]+)`")
)

func main() {
	root := flag.String("root", ".", "module root")
	inventory := flag.String("inventory", "", "inventory file (default: "+defaultInventory+" under the root)")
	manifest := flag.String("manifest", "", "testsupportcheck manifest (default: "+defaultManifest+" under the root)")
	flag.Parse()
	if *inventory == "" {
		*inventory = filepath.Join(*root, filepath.FromSlash(defaultInventory))
	}
	if *manifest == "" {
		*manifest = filepath.Join(*root, filepath.FromSlash(defaultManifest))
	}
	problems, err := Check(*root, *inventory, *manifest)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testdoublecheck:", err)
		os.Exit(2)
	}
	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "FAIL", p)
		}
		os.Exit(1)
	}
	fmt.Println("test-double inventory matches the source")
}

// symbol is one declaration site: a module-relative file and a name.
type symbol struct{ file, name string }

func (s symbol) String() string { return s.file + "#" + s.name }

// Check returns the differences between the inventory and the source under
// root, or an error when the check could not run.
func Check(root, inventoryPath, manifestPath string) ([]string, error) {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	var manifest struct {
		TestOnlyPackages []string `json:"test_only_packages"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("manifest: %v", err)
	}
	if len(manifest.TestOnlyPackages) == 0 {
		return nil, fmt.Errorf("manifest: test_only_packages is empty")
	}
	doc, err := os.ReadFile(inventoryPath)
	if err != nil {
		return nil, err
	}
	shared, seams, err := parseInventory(string(doc))
	if err != nil {
		return nil, err
	}

	var problems []string
	declared := map[symbol]bool{}  // every symbol of every non-test file
	exported := map[symbol]bool{}  // exported top-level declarations of test-only packages
	annotated := map[symbol]bool{} // test-purpose declarations outside them
	files, err := goFiles(root)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		dir := path.Dir(file)
		helper := slices.Contains(manifest.TestOnlyPackages, dir)
		found, err := scanFile(filepath.Join(root, filepath.FromSlash(file)), file)
		if err != nil {
			return nil, err
		}
		for _, s := range found.all {
			declared[s] = true
		}
		if helper {
			for _, s := range found.exported {
				exported[s] = true
			}
			continue
		}
		for _, s := range found.annotated {
			annotated[s] = true
		}
		for _, s := range found.doubles {
			problems = append(problems, fmt.Sprintf("%s is named like a test double but is declared in a non-test file outside the test-only packages", s))
		}
	}
	for _, s := range sortedKeys(exported) {
		if !shared[s] {
			problems = append(problems, fmt.Sprintf("%s is exported by a test-only package but has no row in the %q table", s, sharedHeading))
		}
	}
	for _, s := range sortedKeys(annotated) {
		if !seams[s] {
			problems = append(problems, fmt.Sprintf("%s says it exists for tests but has no row in the %q table", s, seamHeading))
		}
	}
	for table, rows := range map[string]map[symbol]bool{sharedHeading: shared, seamHeading: seams} {
		for _, s := range sortedKeys(rows) {
			if !declared[s] {
				problems = append(problems, fmt.Sprintf("the %q table names %s, which no non-test file declares", table, s))
			}
		}
	}
	sort.Strings(problems)
	return problems, nil
}

func sortedKeys(m map[symbol]bool) []symbol {
	keys := make([]symbol, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b symbol) int { return strings.Compare(a.String(), b.String()) })
	return keys
}

// parseInventory returns the symbols named in the first column of each table.
// A table belongs to the "##" heading above it.
func parseInventory(doc string) (shared, seams map[symbol]bool, err error) {
	shared, seams = map[symbol]bool{}, map[symbol]bool{}
	var current map[symbol]bool
	seen := map[string]bool{}
	for _, line := range strings.Split(doc, "\n") {
		if heading, ok := strings.CutPrefix(line, "## "); ok {
			current = nil
			switch strings.TrimSpace(heading) {
			case sharedHeading:
				current, seen[sharedHeading] = shared, true
			case seamHeading:
				current, seen[seamHeading] = seams, true
			}
			continue
		}
		if current == nil || !strings.HasPrefix(line, "|") {
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) < 3 {
			continue
		}
		for _, m := range rowSymbol.FindAllStringSubmatch(cells[1], -1) {
			current[symbol{m[1], m[2]}] = true
		}
	}
	for _, heading := range []string{sharedHeading, seamHeading} {
		if !seen[heading] {
			return nil, nil, fmt.Errorf("inventory has no %q section", heading)
		}
	}
	if len(shared) == 0 || len(seams) == 0 {
		return nil, nil, fmt.Errorf("inventory tables name no symbols: the rule would be vacuous")
	}
	return shared, seams, nil
}

// goFiles lists the module-relative non-test Go files outside internal/tools
// (command-line programs that describe test code in their own words), testdata
// and hidden directories.
func goFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			name := d.Name()
			if rel != "." && (strings.HasPrefix(name, ".") || name == "testdata" || name == "docs") || rel == "internal/tools" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(rel, ".go") && !strings.HasSuffix(rel, "_test.go") {
			files = append(files, rel)
		}
		return nil
	})
	return files, err
}

type scanned struct {
	all       []symbol
	exported  []symbol
	annotated []symbol
	doubles   []symbol
}

func hasTestComment(groups ...*ast.CommentGroup) bool {
	for _, g := range groups {
		if g != nil && testComment.MatchString(g.Text()) {
			return true
		}
	}
	return false
}

func scanFile(abs, rel string) (scanned, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), abs, nil, parser.ParseComments)
	if err != nil {
		return scanned{}, err
	}
	var out scanned
	add := func(name string, annotated bool, exportedTop bool) {
		s := symbol{rel, name}
		out.all = append(out.all, s)
		if annotated {
			out.annotated = append(out.annotated, s)
		}
		if exportedTop {
			out.exported = append(out.exported, s)
		}
	}
	topDouble := func(name string) {
		if doubleName.MatchString(name) {
			out.doubles = append(out.doubles, symbol{rel, name})
		}
	}
	for _, decl := range parsed.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			name := decl.Name.Name
			if decl.Recv != nil {
				name = recvName(decl.Recv) + "." + name
				add(name, hasTestComment(decl.Doc), false)
				continue
			}
			add(name, hasTestComment(decl.Doc), decl.Name.IsExported())
			topDouble(name)
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					name := spec.Name.Name
					add(name, hasTestComment(spec.Doc, spec.Comment) || (len(decl.Specs) == 1 && hasTestComment(decl.Doc)), spec.Name.IsExported())
					topDouble(name)
					members(spec, name, rel, &out)
				case *ast.ValueSpec:
					for _, id := range spec.Names {
						if id.Name == "_" {
							continue
						}
						add(id.Name, hasTestComment(spec.Doc, spec.Comment) || (len(decl.Specs) == 1 && hasTestComment(decl.Doc)), id.IsExported())
					}
				}
			}
		}
	}
	return out, nil
}

// members records the fields of a struct and the methods of an interface as
// Type.Name.
func members(spec *ast.TypeSpec, typeName, rel string, out *scanned) {
	var fields *ast.FieldList
	switch t := spec.Type.(type) {
	case *ast.StructType:
		fields = t.Fields
	case *ast.InterfaceType:
		fields = t.Methods
	}
	if fields == nil {
		return
	}
	for _, f := range fields.List {
		for _, id := range f.Names {
			s := symbol{rel, typeName + "." + id.Name}
			out.all = append(out.all, s)
			if hasTestComment(f.Doc, f.Comment) {
				out.annotated = append(out.annotated, s)
			}
		}
	}
}

func recvName(recv *ast.FieldList) string {
	if len(recv.List) == 0 {
		return ""
	}
	expr := recv.List[0].Type
	for {
		switch t := expr.(type) {
		case *ast.StarExpr:
			expr = t.X
		case *ast.IndexExpr:
			expr = t.X
		case *ast.IndexListExpr:
			expr = t.X
		case *ast.Ident:
			return t.Name
		default:
			return "?"
		}
	}
}
