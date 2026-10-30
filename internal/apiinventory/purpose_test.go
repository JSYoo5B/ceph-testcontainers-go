package apiinventory_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// purposeTags are the purpose tags of docs/guides/api-purpose.md, in the
// canonical order a directive lists them.
var purposeTags = []string{"construction", "condition", "observation", "handoff", "native", "evidence", "lifetime"}

// purposeExempt are methods whose contract is an interface they implement:
// the standard ones of interfaceMethods, and Customize, which makes a
// configuration value a testcontainers.ContainerCustomizer.
func purposeExempt(name string) bool {
	return interfaceMethods[name] || name == "Customize"
}

var (
	purposeDirective = regexp.MustCompile(`^//purpose:([a-z]+(?:,[a-z]+)*)$`)
	purposeMention   = regexp.MustCompile(`^//\s*purpose\s*:`)
)

// Every exported function, and every exported method of an exported type
// except purposeExempt, ends its doc comment with exactly one directive
// "//purpose:<tag>[,<tag>...]" naming known purpose tags, each once, in the
// canonical order. go doc hides directives, so the tag stays next to the API
// without appearing in its documentation. A purpose directive anywhere else
// (another comment, an unexported or exempt declaration, a malformed
// spelling) is refused.
func TestExportsDeclarePurposeTags(t *testing.T) {
	for _, pkg := range publicPackages(t) {
		matches, err := filepath.Glob(filepath.Join(root, pkg, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range matches {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			for _, problem := range purposeProblems(fset, file) {
				t.Error(problem)
			}
		}
	}
}

// purposeProblems reports every purpose-directive violation of one file.
func purposeProblems(fset *token.FileSet, file *ast.File) []string {
	var problems []string
	owned := map[*ast.Comment]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !purposeInScope(fn) {
			continue
		}
		where := fset.Position(fn.Pos()).String() + ": " + funcName(fn)
		if fn.Doc == nil {
			problems = append(problems, where+": no doc comment to carry a //purpose: directive")
			continue
		}
		var found []*ast.Comment
		for _, line := range fn.Doc.List {
			if purposeMention.MatchString(line.Text) {
				found = append(found, line)
				owned[line] = true
			}
		}
		switch {
		case len(found) == 0:
			problems = append(problems, where+": missing //purpose: directive")
			continue
		case len(found) > 1:
			problems = append(problems, where+": more than one //purpose: directive")
			continue
		}
		if found[0] != fn.Doc.List[len(fn.Doc.List)-1] {
			problems = append(problems, where+": //purpose: is not the last line of the doc comment")
		}
		problems = append(problems, purposeValueProblems(where, found[0].Text)...)
	}
	for _, group := range file.Comments {
		for _, line := range group.List {
			if purposeMention.MatchString(line.Text) && !owned[line] {
				problems = append(problems, fset.Position(line.Pos()).String()+": //purpose: outside the doc comment of an exported function or method")
			}
		}
	}
	return problems
}

func purposeValueProblems(where, text string) []string {
	match := purposeDirective.FindStringSubmatch(text)
	if match == nil {
		return []string{where + ": malformed directive " + text + ` (want "//purpose:tag[,tag...]", lowercase, no spaces)`}
	}
	var problems []string
	last := -1
	seen := map[string]bool{}
	for _, tag := range strings.Split(match[1], ",") {
		index := slices.Index(purposeTags, tag)
		switch {
		case index < 0:
			problems = append(problems, where+": unknown purpose tag "+tag)
		case seen[tag]:
			problems = append(problems, where+": purpose tag "+tag+" repeated")
		case index < last:
			problems = append(problems, where+": purpose tags out of order; want "+strings.Join(purposeTags, ","))
		}
		seen[tag] = true
		if index > last {
			last = index
		}
	}
	return problems
}

// purposeInScope reports whether a function needs a purpose directive: an
// exported function, or an exported method of an exported type that is not
// purposeExempt.
func purposeInScope(fn *ast.FuncDecl) bool {
	if !fn.Name.IsExported() {
		return false
	}
	if fn.Recv == nil {
		return true
	}
	receiver := receiverName(fn)
	return receiver != "" && ast.IsExported(receiver) && !purposeExempt(fn.Name.Name)
}

func receiverName(fn *ast.FuncDecl) string {
	receiver := fn.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if index, ok := receiver.(*ast.IndexExpr); ok {
		receiver = index.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil {
		return fn.Name.Name
	}
	return receiverName(fn) + "." + fn.Name.Name
}

// The purpose check refuses each kind of violation and accepts the
// canonical form.
func TestPurposeProblemsRefuseViolations(t *testing.T) {
	const valid = `package p

// Run starts it.
//
//purpose:construction
func Run() {}

// Value is a value.
type Value struct{}

// Remove removes a member.
//
//purpose:condition,lifetime
func (Value) Remove() {}

// String formats it.
func (Value) String() string { return "" }

// Customize applies nothing.
func (Value) Customize() error { return nil }

func helper() {}
`
	if problems := purposeOf(t, valid); len(problems) != 0 {
		t.Fatalf("canonical source refused: %v", problems)
	}
	for name, source := range map[string]string{
		"missing":         "package p\n\n// Run starts it.\nfunc Run() {}\n",
		"no doc":          "package p\n\nfunc Run() {}\n",
		"two directives":  "package p\n\n// Run starts it.\n//\n//purpose:construction\n//purpose:construction\nfunc Run() {}\n",
		"unknown tag":     "package p\n\n// Run starts it.\n//\n//purpose:convenience\nfunc Run() {}\n",
		"repeated tag":    "package p\n\n// Run starts it.\n//\n//purpose:condition,condition\nfunc Run() {}\n",
		"out of order":    "package p\n\n// Run starts it.\n//\n//purpose:lifetime,condition\nfunc Run() {}\n",
		"empty":           "package p\n\n// Run starts it.\n//\n//purpose:\nfunc Run() {}\n",
		"space form":      "package p\n\n// Run starts it.\n//\n// purpose:construction\nfunc Run() {}\n",
		"uppercase":       "package p\n\n// Run starts it.\n//\n//purpose:Construction\nfunc Run() {}\n",
		"spaced list":     "package p\n\n// Run starts it.\n//\n//purpose:construction, condition\nfunc Run() {}\n",
		"not last":        "package p\n\n// Run starts it.\n//purpose:construction\n//go:noinline\nfunc Run() {}\n",
		"unexported func": "package p\n\n// run starts it.\n//\n//purpose:construction\nfunc run() {}\n",
		"exempt method":   "package p\n\n// Value is a value.\ntype Value struct{}\n\n// String formats it.\n//\n//purpose:observation\nfunc (Value) String() string { return \"\" }\n",
		"stray comment":   "package p\n\n// Run starts it.\n//\n//purpose:construction\nfunc Run() {\n\t//purpose:construction\n}\n",
		"on a type":       "package p\n\n// Value is a value.\n//\n//purpose:construction\ntype Value struct{}\n",
	} {
		if problems := purposeOf(t, source); len(problems) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
}

func purposeOf(t *testing.T, source string) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "p.go", source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	return purposeProblems(fset, file)
}

// The doc syntax check refuses each Markdown or list misuse and accepts what
// go doc renders as intended.
func TestDocSyntaxProblemsRefuseMarkdown(t *testing.T) {
	const valid = `// Run starts it.
//
// It does this:
//   - one thing
//   - another thing,
//     continued
//
// A code block:
//
//	| not | a | table |
//	**literal**
//
// See [Other] and [the guide].
//
// [the guide]: https://example.com
//
//purpose:construction
func Run() {}
`
	if problems := docSyntaxOf(t, valid, false); len(problems) != 0 {
		t.Fatalf("valid comment refused: %v", problems)
	}
	if problems := docSyntaxOf(t, "// Package p does it.\n//\n// # Usage\n//\n// Text.\npackage p\n", true); len(problems) != 0 {
		t.Fatalf("package heading refused: %v", problems)
	}
	for name, doc := range map[string]string{
		"unindented bullets": "// Run does:\n// - one\n// - two\nfunc Run() {}\n",
		"unindented numbers": "// Run does:\n// 1. one\n// 2. two\nfunc Run() {}\n",
		"table":              "// Run does it.\n//\n// | a | b |\n// | --- | --- |\nfunc Run() {}\n",
		"bold":               "// Run does it **now**.\nfunc Run() {}\n",
		"fence":              "// Run does it.\n//\n// ```\n// x\n// ```\nfunc Run() {}\n",
		"deep heading":       "// Run does it.\n//\n// ## Details\nfunc Run() {}\n",
		"heading on func":    "// Run does it.\n//\n// # Details\n//\n// More.\nfunc Run() {}\n",
		"nested item":        "// Run does:\n//   - one\n//     - nested\nfunc Run() {}\n",
		"bold in list item":  "// Run does:\n//   - **one**\nfunc Run() {}\n",
	} {
		if problems := docSyntaxOf(t, doc, false); len(problems) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
}

func docSyntaxOf(t *testing.T, doc string, packageDoc bool) []string {
	t.Helper()
	source := doc
	if !packageDoc {
		source = "package p\n\n" + doc
	}
	file, err := parser.ParseFile(token.NewFileSet(), "p.go", source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	group := file.Doc
	if !packageDoc {
		group = file.Decls[0].(*ast.FuncDecl).Doc
	}
	return docSyntaxProblems(group, packageDoc)
}
