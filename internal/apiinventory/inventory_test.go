// Package apiinventory checks that docs/API_CAPABILITIES.md lists the exact
// public surface of the ceph, cephfs, rgw and rbd packages. Public types are
// aliases of implementation types, so methods and line anchors refer to the
// implementation in internal/cluster and internal/multicluster.
package apiinventory_test

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const (
	root     = "../.."
	document = "docs/API_CAPABILITIES.md"
)

// Public packages in summary-table column order.
var packages = []string{"ceph", "cephfs", "rgw", "rbd"}

var implementations = map[string]string{"cluster": "internal/cluster", "multicluster": "internal/multicluster"}

// Run "go test ./internal/apiinventory -update" after moving declarations to
// rewrite stale line anchors. Missing or extra entries still need editing.
var update = flag.Bool("update", false, "rewrite stale line anchors in "+document)

type declaration struct {
	file string
	line int
}

type implementation struct {
	funcs, types map[string]declaration            // name -> source
	methods      map[string]map[string]declaration // receiver -> method -> source
}

func parseDir(t *testing.T, dir string, visit func(path string, fset *token.FileSet, file *ast.File)) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list %s sources: %v", dir, err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		visit(dir+"/"+filepath.Base(path), fset, file)
	}
}

func receiverName(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.StarExpr:
		return receiverName(expr.X)
	case *ast.IndexExpr:
		return receiverName(expr.X)
	case *ast.IndexListExpr:
		return receiverName(expr.X)
	case *ast.Ident:
		return expr.Name
	}
	return ""
}

func loadImplementation(t *testing.T, dir string) implementation {
	impl := implementation{funcs: map[string]declaration{}, types: map[string]declaration{}, methods: map[string]map[string]declaration{}}
	parseDir(t, dir, func(path string, fset *token.FileSet, file *ast.File) {
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if !decl.Name.IsExported() {
					continue
				}
				at := declaration{path, fset.Position(decl.Pos()).Line}
				if decl.Recv == nil {
					impl.funcs[decl.Name.Name] = at
					continue
				}
				receiver := receiverName(decl.Recv.List[0].Type)
				if !ast.IsExported(receiver) {
					continue
				}
				if impl.methods[receiver] == nil {
					impl.methods[receiver] = map[string]declaration{}
				}
				impl.methods[receiver][decl.Name.Name] = at
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					if spec, ok := spec.(*ast.TypeSpec); ok && spec.Name.IsExported() {
						impl.types[spec.Name.Name] = declaration{path, fset.Position(spec.Pos()).Line}
					}
				}
			}
		}
	})
	return impl
}

// declarations maps "package.Name" or "package.Type.Method" to the source that
// implements it. Wrapper functions point at the implementation they call;
// functions written in a public package point at themselves.
func declarations(t *testing.T) (callables, types map[string]declaration) {
	t.Helper()
	impls := map[string]implementation{}
	for name, dir := range implementations {
		impls[name] = loadImplementation(t, dir)
	}
	callables, types = map[string]declaration{}, map[string]declaration{}
	for _, pkg := range packages {
		parseDir(t, pkg, func(path string, fset *token.FileSet, file *ast.File) {
			for _, decl := range file.Decls {
				switch decl := decl.(type) {
				case *ast.FuncDecl:
					if !decl.Name.IsExported() || decl.Recv != nil {
						continue
					}
					at := declaration{path, fset.Position(decl.Pos()).Line}
					if target, ok := wrapped(decl); ok {
						source, exists := impls[target[0]].funcs[target[1]]
						if !exists {
							t.Fatalf("%s.%s wraps unknown %s.%s", pkg, decl.Name.Name, target[0], target[1])
						}
						at = source
					}
					callables[pkg+"."+decl.Name.Name] = at
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						spec, ok := spec.(*ast.TypeSpec)
						if !ok || !spec.Name.IsExported() {
							continue
						}
						selector, ok := spec.Type.(*ast.SelectorExpr)
						if !ok || !spec.Assign.IsValid() {
							t.Fatalf("%s.%s must alias an implementation type", pkg, spec.Name.Name)
						}
						implPkg := selector.X.(*ast.Ident).Name
						impl, exists := impls[implPkg].types[selector.Sel.Name]
						if !exists {
							t.Fatalf("%s.%s aliases unknown %s.%s", pkg, spec.Name.Name, implPkg, selector.Sel.Name)
						}
						types[pkg+"."+spec.Name.Name] = impl
						for method, source := range impls[implPkg].methods[selector.Sel.Name] {
							callables[pkg+"."+spec.Name.Name+"."+method] = source
						}
					}
				}
			}
		})
	}
	return callables, types
}

// wrapped reports the implementation function a generated wrapper calls.
func wrapped(decl *ast.FuncDecl) ([2]string, bool) {
	if decl.Body == nil || len(decl.Body.List) != 1 {
		return [2]string{}, false
	}
	var call ast.Expr
	switch statement := decl.Body.List[0].(type) {
	case *ast.ReturnStmt:
		if len(statement.Results) == 1 {
			call = statement.Results[0]
		}
	case *ast.ExprStmt:
		call = statement.X
	}
	expr, ok := call.(*ast.CallExpr)
	if !ok {
		return [2]string{}, false
	}
	selector, ok := expr.Fun.(*ast.SelectorExpr)
	if !ok {
		return [2]string{}, false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	if !ok || implementations[qualifier.Name] == "" {
		return [2]string{}, false
	}
	return [2]string{qualifier.Name, selector.Sel.Name}, true
}

type entry struct {
	key, file string
	line      int
}

type section struct {
	title   string
	count   int
	entries []entry
}

var (
	rowSource = regexp.MustCompile(`^\| \[([a-z/]+/[a-z0-9_]+\.go)\]\(\.\./([a-z/]+/[a-z0-9_]+\.go)\) \| (.*) \|$`)
	rowLink   = regexp.MustCompile(`\[((?:ceph|cephfs|rgw|rbd)\.[A-Za-z0-9_.]+)\]\(\.\./([a-z/]+/[a-z0-9_]+\.go)(?:#L([0-9]+))?\)`)
	heading   = regexp.MustCompile(`^### (.+) \(([0-9]+)개\)$`)
)

func readDocument(t *testing.T) string {
	t.Helper()
	path := filepath.Join(root, document)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !*update {
		return string(data)
	}
	callables, types := declarations(t)
	text := rowLink.ReplaceAllStringFunc(string(data), func(link string) string {
		match := rowLink.FindStringSubmatch(link)
		if match[3] == "" {
			return link
		}
		source, exists := callables[match[1]]
		if !exists {
			source, exists = types[match[1]]
		}
		if !exists || source.file != match[2] {
			return link
		}
		return "[" + match[1] + "](../" + match[2] + "#L" + strconv.Itoa(source.line) + ")"
	})
	if text != string(data) {
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return text
}

func block(t *testing.T, text, name string) []string {
	t.Helper()
	begin, end := "<!-- "+name+":begin -->", "<!-- "+name+":end -->"
	start, stop := strings.Index(text, begin), strings.Index(text, end)
	if start < 0 || stop < start || strings.Count(text, begin) != 1 || strings.Count(text, end) != 1 {
		t.Fatalf("%s must contain exactly one %s block", document, name)
	}
	return strings.Split(text[start+len(begin):stop], "\n")
}

// parseRows reads "| [source](../source) | [pkg.Name](../source#Lnn) · ... |"
// rows. Every link in a row must point at that row's source file.
func parseRows(t *testing.T, lines []string, onSection func(string, int), onEntry func(entry)) {
	t.Helper()
	for number, line := range lines {
		if match := heading.FindStringSubmatch(line); match != nil {
			count, _ := strconv.Atoi(match[2])
			onSection(match[1], count)
			continue
		}
		match := rowSource.FindStringSubmatch(line)
		if match == nil {
			if strings.HasPrefix(line, "| [") {
				t.Errorf("unparsed inventory row %d: %s", number, line)
			}
			continue
		}
		if match[1] != match[2] {
			t.Errorf("row label %s links to %s", match[1], match[2])
		}
		links := rowLink.FindAllStringSubmatch(match[3], -1)
		if len(links) == 0 {
			t.Errorf("row for %s lists nothing", match[1])
		}
		for _, link := range links {
			if link[2] != match[1] {
				t.Errorf("%s is listed under %s but links to %s", link[1], match[1], link[2])
			}
			line := 0
			if link[3] != "" {
				line, _ = strconv.Atoi(link[3])
			}
			onEntry(entry{key: link[1], file: link[2], line: line})
		}
	}
}

// compare requires one listing per declaration in its implementing file. Line
// anchors are optional, but a present anchor must reach the declaration.
func compare(t *testing.T, kind string, declared map[string]declaration, listed []entry) {
	t.Helper()
	seen := map[string]bool{}
	for _, item := range listed {
		if seen[item.key] {
			t.Errorf("%s %s is listed more than once", kind, item.key)
		}
		seen[item.key] = true
		source, exists := declared[item.key]
		switch {
		case !exists:
			t.Errorf("%s %s is listed but not public", kind, item.key)
		case source.file != item.file:
			t.Errorf("%s %s is listed under %s but declared in %s", kind, item.key, item.file, source.file)
		case item.line != 0 && item.line != source.line:
			t.Errorf("%s %s links to %s#L%d; declared at line %d", kind, item.key, item.file, item.line, source.line)
		}
	}
	var missing []string
	for key := range declared {
		if !seen[key] {
			missing = append(missing, key)
		}
	}
	slices.Sort(missing)
	for _, key := range missing {
		t.Errorf("%s %s (%s) is not listed in %s", kind, key, declared[key].file, document)
	}
}

func publicPackage(key string) string {
	pkg, _, _ := strings.Cut(key, ".")
	return pkg
}

func TestCapabilityDocumentListsEveryPublicCallable(t *testing.T) {
	callables, _ := declarations(t)
	text := readDocument(t)
	var sections []*section
	var listed []entry
	parseRows(t, block(t, text, "callables"), func(title string, count int) {
		sections = append(sections, &section{title: title, count: count})
	}, func(item entry) {
		if len(sections) == 0 {
			t.Fatalf("%s appears before any category heading", item.key)
		}
		current := sections[len(sections)-1]
		current.entries = append(current.entries, item)
		listed = append(listed, item)
	})
	compare(t, "callable", callables, listed)

	// Summary rows group headings by the text before a colon, so both
	// "Check: ..." headings add up to the single Check row.
	categories := map[string]map[string]int{}
	for _, current := range sections {
		if current.count != len(current.entries) {
			t.Errorf("heading %q says %d but lists %d", current.title, current.count, len(current.entries))
		}
		key, _, _ := strings.Cut(current.title, ":")
		if categories[key] == nil {
			categories[key] = map[string]int{}
		}
		for _, item := range current.entries {
			categories[key][publicPackage(item.key)]++
		}
	}
	start := strings.Index(text, "## 현재 공개 callable 수")
	end := strings.Index(text, "<!-- callables:begin -->")
	if start < 0 || end < start {
		t.Fatal("callable summary table is missing")
	}
	columns := strings.Repeat(` \| ([0-9]+)`, len(packages))
	summary := regexp.MustCompile(`(?m)^\| ([^|]+?)` + columns + ` \| \**([0-9]+)\**` + ` \|$`)
	totals := map[string]int{}
	for _, item := range listed {
		totals[publicPackage(item.key)]++
	}
	matched := map[string]bool{}
	sawTotal := false
	for _, row := range summary.FindAllStringSubmatch(text[start:end], -1) {
		values := make([]int, len(packages)+1)
		for i := range values {
			values[i], _ = strconv.Atoi(row[i+2])
		}
		sum := 0
		for _, value := range values[:len(packages)] {
			sum += value
		}
		if sum != values[len(packages)] {
			t.Errorf("summary row %q does not add up", row[1])
		}
		want := totals
		if row[1] == "전체" {
			sawTotal = true
		} else {
			key, _, _ := strings.Cut(row[1], ":")
			if categories[key] == nil {
				t.Errorf("summary row %q has no matching heading", row[1])
				continue
			}
			matched[key] = true
			want = categories[key]
		}
		for i, pkg := range packages {
			if values[i] != want[pkg] {
				t.Errorf("summary row %q says %s=%d but the list has %d", row[1], pkg, values[i], want[pkg])
			}
		}
	}
	for key := range categories {
		if !matched[key] {
			t.Errorf("heading group %q has no summary row", key)
		}
	}
	if !sawTotal {
		t.Error("summary total row is missing")
	}
	prose := regexp.MustCompile(`이 ([0-9]+)개에 포함하지`).FindStringSubmatch(text)
	if prose == nil || prose[1] != strconv.Itoa(len(listed)) {
		t.Errorf("summary prose total %v differs from %d", prose, len(listed))
	}
}

func TestCapabilityDocumentListsEveryPublicType(t *testing.T) {
	_, types := declarations(t)
	text := readDocument(t)
	var listed []entry
	parseRows(t, block(t, text, "schemas"), func(title string, _ int) {
		t.Errorf("unexpected heading %q in the schema block", title)
	}, func(item entry) {
		listed = append(listed, item)
	})
	compare(t, "type", types, listed)
	counts := map[string]int{}
	for _, item := range listed {
		counts[publicPackage(item.key)]++
	}
	sentence := regexp.MustCompile("공개 타입은 `ceph` ([0-9]+)개, `cephfs` ([0-9]+)개, `rgw` ([0-9]+)개, `rbd` ([0-9]+)개").FindStringSubmatch(text)
	if sentence == nil {
		t.Fatal("public type count sentence is missing")
	}
	for i, pkg := range packages {
		if sentence[i+1] != strconv.Itoa(counts[pkg]) {
			t.Errorf("%s type count says %s but lists %d", pkg, sentence[i+1], counts[pkg])
		}
	}
}
