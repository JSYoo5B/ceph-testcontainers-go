// Package apiinventory checks that docs/API_CAPABILITIES.md lists the exact
// public surface of the ceph and multicluster packages.
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

var packages = []string{"ceph", "multicluster"}

// Run "go test ./internal/apiinventory -update" after moving declarations to
// rewrite stale line anchors. Missing or extra entries still need editing.
var update = flag.Bool("update", false, "rewrite stale line anchors in "+document)

type declaration struct {
	file string
	line int
}

// declarations maps "package:Name" or "package:Receiver.Method" to its source.
// Methods count only when both the method and its receiver type are exported.
func declarations(t *testing.T) (callables, types map[string]declaration) {
	t.Helper()
	callables, types = map[string]declaration{}, map[string]declaration{}
	for _, pkg := range packages {
		files, err := filepath.Glob(filepath.Join(root, pkg, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("list %s sources: %v", pkg, err)
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
			source := pkg + "/" + filepath.Base(path)
			for _, decl := range file.Decls {
				switch decl := decl.(type) {
				case *ast.FuncDecl:
					if !decl.Name.IsExported() {
						continue
					}
					name := decl.Name.Name
					if decl.Recv != nil {
						receiver := receiverName(decl.Recv.List[0].Type)
						if !ast.IsExported(receiver) {
							continue
						}
						name = receiver + "." + name
					}
					callables[pkg+":"+name] = declaration{source, fset.Position(decl.Pos()).Line}
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						if spec, ok := spec.(*ast.TypeSpec); ok && spec.Name.IsExported() {
							// A grouped declaration links to the type's own line.
							types[pkg+":"+spec.Name.Name] = declaration{source, fset.Position(spec.Pos()).Line}
						}
					}
				}
			}
		}
	}
	return callables, types
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
	rowSource = regexp.MustCompile(`^\| \[((?:ceph|multicluster)/[a-z0-9_]+\.go)\]\(\.\./((?:ceph|multicluster)/[a-z0-9_]+\.go)\) \| (.*) \|$`)
	rowLink   = regexp.MustCompile(`\[([A-Za-z0-9_.]+)\]\(\.\./((?:ceph|multicluster)/[a-z0-9_]+\.go)(?:#L([0-9]+))?\)`)
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
		pkg, _, _ := strings.Cut(match[2], "/")
		source, exists := callables[pkg+":"+match[1]]
		if !exists {
			source, exists = types[pkg+":"+match[1]]
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

// parseRows reads "| [source](../source) | [Name](../source#Lnn) · ... |" rows.
// Every link in a row must point at that row's source file.
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
		pkg, _, _ := strings.Cut(match[1], "/")
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
			onEntry(entry{key: pkg + ":" + link[1], file: link[2], line: line})
		}
	}
}

// compare requires one listing per declaration in its declaring file. Line
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
			t.Errorf("%s %s is listed but not declared", kind, item.key)
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
	type totals struct{ ceph, multicluster int }
	categories := map[string]*totals{}
	for _, current := range sections {
		if current.count != len(current.entries) {
			t.Errorf("heading %q says %d but lists %d", current.title, current.count, len(current.entries))
		}
		key, _, _ := strings.Cut(current.title, ":")
		if categories[key] == nil {
			categories[key] = &totals{}
		}
		for _, item := range current.entries {
			if strings.HasPrefix(item.key, "ceph:") {
				categories[key].ceph++
			} else {
				categories[key].multicluster++
			}
		}
	}
	summary := regexp.MustCompile(`(?m)^\| ([^|]+?) \| ([0-9]+) \| ([0-9]+) \| \**([0-9]+)\** \|$`)
	start := strings.Index(text, "## 현재 공개 callable 수")
	end := strings.Index(text, "<!-- callables:begin -->")
	if start < 0 || end < start {
		t.Fatal("callable summary table is missing")
	}
	rows := summary.FindAllStringSubmatch(text[start:end], -1)
	var all totals
	matched := map[string]bool{}
	for _, row := range rows {
		values := [3]int{}
		for i := range values {
			values[i], _ = strconv.Atoi(row[i+2])
		}
		if values[0]+values[1] != values[2] {
			t.Errorf("summary row %q does not add up", row[1])
		}
		if row[1] == "전체" {
			if values[0] != len(filter(listed, "ceph:")) || values[1] != len(filter(listed, "multicluster:")) {
				t.Errorf("summary total %d/%d differs from listed %d/%d", values[0], values[1], len(filter(listed, "ceph:")), len(filter(listed, "multicluster:")))
			}
			all = totals{values[0], values[1]}
			continue
		}
		key, _, _ := strings.Cut(row[1], ":")
		counted := categories[key]
		if counted == nil {
			t.Errorf("summary row %q has no matching heading", row[1])
			continue
		}
		matched[key] = true
		if counted.ceph != values[0] || counted.multicluster != values[1] {
			t.Errorf("summary row %q says %d/%d but headings list %d/%d", row[1], values[0], values[1], counted.ceph, counted.multicluster)
		}
	}
	for key := range categories {
		if !matched[key] {
			t.Errorf("heading group %q has no summary row", key)
		}
	}
	if all == (totals{}) {
		t.Error("summary total row is missing")
	}
	prose := regexp.MustCompile(`이 ([0-9]+)개에 포함하지`).FindStringSubmatch(text)
	if prose == nil || prose[1] != strconv.Itoa(all.ceph+all.multicluster) {
		t.Errorf("summary prose total %v differs from %d", prose, all.ceph+all.multicluster)
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
	counts := regexp.MustCompile("공개 타입은 `ceph` ([0-9]+)개, `multicluster` ([0-9]+)개").FindStringSubmatch(text)
	if counts == nil {
		t.Fatal("public type count sentence is missing")
	}
	for i, pkg := range packages {
		if counts[i+1] != strconv.Itoa(len(filter(listed, pkg+":"))) {
			t.Errorf("%s type count says %s but lists %d", pkg, counts[i+1], len(filter(listed, pkg+":")))
		}
	}
}

func filter(entries []entry, prefix string) []entry {
	var result []entry
	for _, item := range entries {
		if strings.HasPrefix(item.key, prefix) {
			result = append(result, item)
		}
	}
	return result
}
