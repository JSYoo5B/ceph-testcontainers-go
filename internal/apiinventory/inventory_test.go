// Package apiinventory checks the package-scoped API catalogues under
// internal/apiinventory/catalogues, that each public package keeps its usage
// page, and that the godoc documents every identifier the package exports:
// functions, types, constants, variables and the methods of its types.
//
// The public package list is discovered from the source tree; the page of a
// package is named by its reviewed receipt.
package apiinventory_test

import (
	"go/ast"
	"go/doc/comment"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

const root = "../.."

// The package README is a usage page (purpose, order of use, concepts and
// limits); the API itself is documented in godoc, which go doc and
// pkg.go.dev show. Every page must still exist.
func TestPackagePagesExist(t *testing.T) {
	for _, pkg := range publicPackages(t) {
		receipt, err := catalogue.LoadReceipt(root, pkg)
		if err != nil {
			t.Errorf("%s: %v", pkg, err)
			continue
		}
		if _, err := os.Stat(filepath.Join(root, receipt.Page)); err != nil {
			t.Errorf("%s: %v", pkg, err)
		}
	}
}

// interfaceMethods are the methods whose contract is the standard interface
// they implement (fmt.Stringer, fmt.GoStringer, fmt.Formatter, error,
// errors.Unwrap and errors.Is/As), so they need no doc comment of their own.
var interfaceMethods = map[string]bool{"String": true, "GoString": true, "Format": true, "Error": true, "Unwrap": true, "Is": true, "As": true}

// Every exported function, type and method of an exported type has a doc
// comment. A constant or variable is documented by its own comment, its
// declaration group's comment, another documented entry of the same group, or
// the doc comment of its declared named type.
func TestGodocDocumentsEveryExport(t *testing.T) {
	for _, pkg := range publicPackages(t) {
		dir := filepath.Join(root, pkg)
		fset := token.NewFileSet()
		matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		var files []*ast.File
		documentedTypes := map[string]bool{}
		for _, path := range matches {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, file)
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.TYPE {
					continue
				}
				for _, spec := range gen.Specs {
					spec := spec.(*ast.TypeSpec)
					if spec.Doc != nil || gen.Doc != nil {
						documentedTypes[spec.Name.Name] = true
					}
				}
			}
		}
		if len(files) == 0 {
			t.Errorf("%s: no Go files", pkg)
		}
		for _, file := range files {
			for _, decl := range file.Decls {
				for _, name := range undocumented(decl, documentedTypes) {
					t.Errorf("%s: %s %s has no doc comment", pkg, fset.Position(decl.Pos()), name)
				}
			}
		}
	}
}

func undocumented(decl ast.Decl, documentedTypes map[string]bool) []string {
	switch decl := decl.(type) {
	case *ast.FuncDecl:
		if !decl.Name.IsExported() || decl.Doc != nil {
			return nil
		}
		if decl.Recv == nil {
			return []string{decl.Name.Name}
		}
		receiver := decl.Recv.List[0].Type
		if star, ok := receiver.(*ast.StarExpr); ok {
			receiver = star.X
		}
		if index, ok := receiver.(*ast.IndexExpr); ok {
			receiver = index.X
		}
		ident, ok := receiver.(*ast.Ident)
		if !ok || !ident.IsExported() || interfaceMethods[decl.Name.Name] {
			return nil
		}
		return []string{ident.Name + "." + decl.Name.Name}
	case *ast.GenDecl:
		var missing []string
		groupDocumented := decl.Doc != nil
		for _, spec := range decl.Specs {
			if value, ok := spec.(*ast.ValueSpec); ok && (value.Doc != nil || value.Comment != nil) {
				groupDocumented = true
			}
		}
		for _, spec := range decl.Specs {
			switch spec := spec.(type) {
			case *ast.TypeSpec:
				if spec.Name.IsExported() && spec.Doc == nil && decl.Doc == nil {
					missing = append(missing, spec.Name.Name)
				}
			case *ast.ValueSpec:
				typed := false
				if ident, ok := spec.Type.(*ast.Ident); ok && documentedTypes[ident.Name] {
					typed = true
				}
				for _, name := range spec.Names {
					if name.IsExported() && !groupDocumented && !typed {
						missing = append(missing, name.Name)
					}
				}
			}
		}
		return missing
	}
	return nil
}

// Doc comments use Go doc comment syntax (go.dev/doc/comment), which go doc and
// pkg.go.dev render: paragraphs, "# Heading" lines in a package doc only,
// lists indented as gofmt writes them ("//   - item"), indented code blocks
// and [links]. Markdown leaks in easily when a README section moves into
// godoc, and go doc then shows it as literal text. Each doc comment is parsed
// with go/doc/comment, as go doc does (directives such as //purpose: are left
// out), and docSyntaxProblems judges the blocks the parser produced.
func TestDocCommentsUseGoDocSyntax(t *testing.T) {
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
			for _, group := range docComments(file) {
				for _, problem := range docSyntaxProblems(group, group == file.Doc) {
					t.Errorf("%s: %s", fset.Position(group.Pos()), problem)
				}
			}
		}
	}
}

var (
	listMarkerLine = regexp.MustCompile(`^([-*+\x{2022}]|[0-9]+[.)])\s`)
	tableRowLine   = regexp.MustCompile(`^\|.*\|$`)
	markdownHead   = regexp.MustCompile(`^#{2,6}(\s|$)`)
	nestedItemLine = regexp.MustCompile(`^// {4,}([-*+\x{2022}]|[0-9]+[.)])\s`)
)

// docSyntaxProblems parses one doc comment the way go doc does and reports
// what go doc would not render as intended:
//   - a paragraph line that starts with a list marker: the parser makes a list
//     only of indented items, so an unindented "- item" is joined into the
//     surrounding paragraph;
//   - a paragraph line shaped like a Markdown table row, ** emphasis, a ```
//     fence or a ## heading, which go doc prints literally;
//   - a heading outside a package doc;
//   - a nested list item, which doc comments do not have (detected on the raw
//     lines, since the parser flattens it).
//
// Code blocks are exempt: the parser keeps them verbatim.
func docSyntaxProblems(group *ast.CommentGroup, packageDoc bool) []string {
	var problems []string
	for _, line := range group.List {
		if nestedItemLine.MatchString(line.Text) {
			problems = append(problems, "a nested list item: "+strings.TrimSpace(strings.TrimPrefix(line.Text, "//")))
		}
	}
	var p comment.Parser
	var walk func([]comment.Block)
	walk = func(blocks []comment.Block) {
		for _, block := range blocks {
			switch block := block.(type) {
			case *comment.Heading:
				if !packageDoc {
					problems = append(problems, "a heading outside a package doc: "+plainText(block.Text))
				}
			case *comment.Paragraph:
				for _, line := range strings.Split(plainText(block.Text), "\n") {
					line = strings.TrimSpace(line)
					switch {
					case listMarkerLine.MatchString(line):
						problems = append(problems, "an unindented list item, which go doc joins into the paragraph: "+line)
					case tableRowLine.MatchString(line):
						problems = append(problems, "a Markdown table row: "+line)
					case strings.Contains(line, "**"):
						problems = append(problems, "Markdown ** emphasis: "+line)
					case strings.Contains(line, "```"):
						problems = append(problems, "a Markdown code fence: "+line)
					case markdownHead.MatchString(line):
						problems = append(problems, "a Markdown ## heading: "+line)
					}
				}
			case *comment.List:
				for _, item := range block.Items {
					walk(item.Content)
				}
			}
		}
	}
	walk(p.Parse(group.Text()).Content)
	return problems
}

// plainText joins the text of parsed spans, keeping line breaks.
func plainText(spans []comment.Text) string {
	var b strings.Builder
	for _, span := range spans {
		switch span := span.(type) {
		case comment.Plain:
			b.WriteString(string(span))
		case comment.Italic:
			b.WriteString(string(span))
		case *comment.Link:
			b.WriteString(plainText(span.Text))
		case *comment.DocLink:
			b.WriteString(plainText(span.Text))
		}
	}
	return b.String()
}

// docComments returns the doc comments of a file: the package doc and those of
// its declarations, specs and struct or interface fields.
func docComments(file *ast.File) []*ast.CommentGroup {
	var groups []*ast.CommentGroup
	add := func(group *ast.CommentGroup) {
		if group != nil {
			groups = append(groups, group)
		}
	}
	add(file.Doc)
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncDecl:
			add(node.Doc)
		case *ast.GenDecl:
			add(node.Doc)
		case *ast.TypeSpec:
			add(node.Doc)
		case *ast.ValueSpec:
			add(node.Doc)
		case *ast.Field:
			add(node.Doc)
		}
		return true
	})
	return groups
}
