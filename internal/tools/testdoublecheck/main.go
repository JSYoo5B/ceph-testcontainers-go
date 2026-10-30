// Command testdoublecheck checks, without Docker, that every test double and
// test-only access point of the module says what it is, next to its
// declaration. It reads Go syntax only (no type information), so it states
// what it can see and no more:
//
//   - every exported top-level declaration of a package listed in the
//     testsupportcheck manifest's test_only_packages carries a
//     "//testdouble:<class>" directive in its doc comment;
//   - every non-test declaration outside those packages whose comment says it
//     exists for tests (a type, struct field, interface method, function or
//     variable whose doc or trailing comment says, for example, "replaced in
//     tests", "tests replace it" or "test-only"; the pattern is testComment)
//     carries the directive. A seam without such a comment and without the
//     directive is not found, and docs/contributing/test-double-inventory.md
//     says so;
//   - every directive names a class of docs/contributing/test-double-inventory.md,
//     once, in the doc comment of a declaration;
//   - no non-test file outside the test-only packages declares a type or
//     function whose name contains fake, mock, stub or spy (case-insensitive):
//     such doubles are _test.go declarations or live in a test-only package.
//
// The harness_packages of the manifest hold integration test code that is
// outside _test.go files only so that several test packages can share it;
// they are read like _test.go files and not checked.
//
// The class lives next to the declaration rather than in a table, so changes
// that add doubles in different packages do not edit a shared file. Package-
// local doubles in _test.go files are not checked: they are described by
// family in the inventory page.
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

const defaultManifest = "internal/tools/testsupportcheck/manifest.json"

// classes are the test-double classes of
// docs/contributing/test-double-inventory.md, in directive spelling.
var classes = []string{
	"shared-double", "shared-assertion", "unit-seam", "unit-seam-client",
	"dependency-port", "nil-default-hook", "wired-operation", "test-mutator",
}

var (
	// testComment matches the phrases the module uses for an injection seam:
	// "replaced in tests", "tests replace it", "unit tests replace", "test
	// seam(s)", "test-only" and "unit-test hook". A mention of tests inside a longer phrase ("for tests that
	// check ...") does not match.
	testComment = regexp.MustCompile(`(?i)\b(in|for the|for) (unit )?tests([.,;)]|\s*$)|\b(unit )?tests replace\b|\btest[- ]seams?\b|\btest-only\b|\bunit-test hook\b`)
	doubleName  = regexp.MustCompile(`(?i)fake|mock|stub|spy`)
	// directive is the spelling of a class directive; mention is anything
	// that tries to be one.
	directive = regexp.MustCompile(`^//testdouble:([a-z-]+)$`)
	mention   = regexp.MustCompile(`^//\s*testdouble\s*:`)
)

func main() {
	root := flag.String("root", ".", "module root")
	manifest := flag.String("manifest", "", "testsupportcheck manifest (default: "+defaultManifest+" under the root)")
	flag.Parse()
	if *manifest == "" {
		*manifest = filepath.Join(*root, filepath.FromSlash(defaultManifest))
	}
	problems, err := Check(*root, *manifest)
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
	fmt.Println("every test double declares its class")
}

// symbol is one declaration site: a module-relative file and a name.
type symbol struct{ file, name string }

func (s symbol) String() string { return s.file + "#" + s.name }

// Check returns the test-double problems of the source under root, or an
// error when the check could not run.
func Check(root, manifestPath string) ([]string, error) {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	var manifest struct {
		TestOnlyPackages []string `json:"test_only_packages"`
		HarnessPackages  []string `json:"harness_packages"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("manifest: %v", err)
	}
	if len(manifest.TestOnlyPackages) == 0 {
		return nil, fmt.Errorf("manifest: test_only_packages is empty")
	}

	var problems []string
	exported := map[symbol]bool{}   // exported top-level declarations of test-only packages
	annotated := map[symbol]bool{}  // test-purpose declarations outside them
	classified := map[symbol]bool{} // declarations with a class directive
	files, err := goFiles(root)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		dir := path.Dir(file)
		if slices.Contains(manifest.HarnessPackages, dir) {
			// Integration test code outside _test.go files only because
			// other test packages share it; it is read like a _test.go file.
			continue
		}
		helper := slices.Contains(manifest.TestOnlyPackages, dir)
		found, err := scanFile(filepath.Join(root, filepath.FromSlash(file)), file)
		if err != nil {
			return nil, err
		}
		problems = append(problems, found.problems...)
		for _, s := range found.classified {
			classified[s] = true
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
		if !classified[s] {
			problems = append(problems, fmt.Sprintf("%s is exported by a test-only package but has no //testdouble:<class> directive", s))
		}
	}
	for _, s := range sortedKeys(annotated) {
		if !classified[s] {
			problems = append(problems, fmt.Sprintf("%s says it exists for tests but has no //testdouble:<class> directive", s))
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
	exported   []symbol
	annotated  []symbol
	classified []symbol
	doubles    []symbol
	problems   []string
}

// classify records the class directives of the doc comments of one
// declaration and marks them as owned.
func (out *scanned) classify(s symbol, fset *token.FileSet, owned map[*ast.Comment]bool, docs ...*ast.CommentGroup) {
	var found []string
	for _, doc := range docs {
		if doc == nil {
			continue
		}
		for _, c := range doc.List {
			if !mention.MatchString(c.Text) {
				continue
			}
			owned[c] = true
			m := directive.FindStringSubmatch(c.Text)
			if m == nil || !slices.Contains(classes, m[1]) {
				out.problems = append(out.problems, fmt.Sprintf("%s: %s: %q is not //testdouble:<class> with a class of %v", fset.Position(c.Pos()), s, c.Text, classes))
				continue
			}
			found = append(found, m[1])
		}
	}
	switch {
	case len(found) == 1:
		out.classified = append(out.classified, s)
	case len(found) > 1:
		out.problems = append(out.problems, fmt.Sprintf("%s has %d //testdouble directives; give it one class", s, len(found)))
	}
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
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, abs, nil, parser.ParseComments)
	if err != nil {
		return scanned{}, err
	}
	var out scanned
	owned := map[*ast.Comment]bool{}
	add := func(name string, annotated bool, exportedTop bool, docs ...*ast.CommentGroup) {
		s := symbol{rel, name}
		out.classify(s, fset, owned, docs...)
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
				add(name, hasTestComment(decl.Doc), false, decl.Doc)
				continue
			}
			add(name, hasTestComment(decl.Doc), decl.Name.IsExported(), decl.Doc)
			topDouble(name)
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					name := spec.Name.Name
					add(name, hasTestComment(spec.Doc, spec.Comment) || (len(decl.Specs) == 1 && hasTestComment(decl.Doc)), spec.Name.IsExported(), specDocs(decl, spec.Doc)...)
					topDouble(name)
					members(spec, name, rel, fset, owned, &out)
				case *ast.ValueSpec:
					for _, id := range spec.Names {
						if id.Name == "_" {
							continue
						}
						add(id.Name, hasTestComment(spec.Doc, spec.Comment) || (len(decl.Specs) == 1 && hasTestComment(decl.Doc)), id.IsExported(), specDocs(decl, spec.Doc)...)
					}
				}
			}
		}
	}
	for _, group := range parsed.Comments {
		for _, c := range group.List {
			if mention.MatchString(c.Text) && !owned[c] {
				out.problems = append(out.problems, fmt.Sprintf("%s: %q is not in the doc comment of a declaration", fset.Position(c.Pos()), c.Text))
			}
		}
	}
	return out, nil
}

// specDocs returns the doc comments a directive of one spec may be in: its
// own, and the declaration's when the declaration has only that spec.
func specDocs(decl *ast.GenDecl, doc *ast.CommentGroup) []*ast.CommentGroup {
	if len(decl.Specs) == 1 {
		return []*ast.CommentGroup{doc, decl.Doc}
	}
	return []*ast.CommentGroup{doc}
}

// members records the fields of a struct and the methods of an interface as
// Type.Name.
func members(spec *ast.TypeSpec, typeName, rel string, fset *token.FileSet, owned map[*ast.Comment]bool, out *scanned) {
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
			out.classify(s, fset, owned, f.Doc)
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
