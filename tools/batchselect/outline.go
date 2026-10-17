package main

import (
	"go/ast"
	"go/token"
	"strings"
)

// lineKind says what a source line outside every top-level declaration is.
type lineKind int

const (
	// lineCode is anything that is not one of the kinds below; a change
	// there cannot be attributed to a declaration.
	lineCode lineKind = iota
	lineBlank
	// lineComment is a comment between declarations that carries no
	// directive, so changing it changes no compiled code.
	lineComment
	lineDirective
	lineHeader
	lineImport
)

// decl is one top-level declaration: a function, a method, or one spec of
// a type, var or const declaration.
type decl struct {
	// keys identify the declaration across two versions of the package:
	// func:Name, method:Recv.Name, type:Name or value:Name, one per name.
	keys    []string
	display string
	// Lines are 1-based and inclusive, doc comment included.
	startLine, endLine int
	// pos and end bound the declaration itself, for object lookup.
	pos, end token.Pos
	// recvEnd ends the receiver of a method; references in it are not
	// edges, so that a method does not reach every method of its type.
	recvPos, recvEnd token.Pos
	recv             string
	isFunc, isType   bool
	isInit, isEntry  bool
	initializer      bool
}

// outline maps every line of a parsed file to its declarations or, outside
// them, to a lineKind.
type outline struct {
	decls []*decl
	// lines[i] are the indexes into decls of line i+1.
	lines [][]int
	kinds []lineKind
}

func line(fset *token.FileSet, pos token.Pos) int {
	return fset.Position(pos).Line
}

// entryName reports a function that go test runs: tests, fuzz targets and
// examples, and TestMain. A helper that only looks like one is kept too;
// that over-selects and never misses.
func entryName(name string) bool {
	return strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Fuzz") || strings.HasPrefix(name, "Example")
}

func receiverName(expr ast.Expr) string {
	for {
		switch e := expr.(type) {
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.Ident:
			return e.Name
		default:
			return ""
		}
	}
}

func directive(group *ast.CommentGroup) bool {
	for _, comment := range group.List {
		if strings.HasPrefix(comment.Text, "//go:") || strings.HasPrefix(comment.Text, "//ci:") ||
			strings.HasPrefix(comment.Text, "//line ") || strings.HasPrefix(comment.Text, "/*line ") {
			return true
		}
	}
	return false
}

func docStart(doc *ast.CommentGroup, pos token.Pos) token.Pos {
	if doc != nil {
		return doc.Pos()
	}
	return pos
}

func newOutline(fset *token.FileSet, file *ast.File, src []byte) *outline {
	text := strings.Split(string(src), "\n")
	o := &outline{lines: make([][]int, len(text)), kinds: make([]lineKind, len(text))}
	mark := func(from, to int, index int) {
		for l := from; l <= to && l <= len(text); l++ {
			o.lines[l-1] = append(o.lines[l-1], index)
		}
	}
	kind := func(from, to int, k lineKind) {
		for l := from; l <= to && l <= len(text); l++ {
			o.kinds[l-1] = k
		}
	}
	for i, t := range text {
		if strings.TrimSpace(t) == "" {
			o.kinds[i] = lineBlank
		}
	}
	for _, group := range file.Comments {
		k := lineComment
		if directive(group) {
			k = lineDirective
		}
		kind(line(fset, group.Pos()), line(fset, group.End()), k)
	}
	kind(1, line(fset, file.Name.End()), lineHeader)
	for _, d := range file.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			item := &decl{pos: d.Pos(), end: d.End(), isFunc: true,
				startLine: line(fset, docStart(d.Doc, d.Pos())), endLine: line(fset, d.End())}
			if d.Recv != nil && len(d.Recv.List) == 1 {
				item.recv = receiverName(d.Recv.List[0].Type)
				item.recvPos, item.recvEnd = d.Recv.Pos(), d.Recv.End()
				item.keys = []string{"method:" + item.recv + "." + d.Name.Name}
				item.display = "method " + item.recv + "." + d.Name.Name
			} else {
				item.keys = []string{"func:" + d.Name.Name}
				item.display = "func " + d.Name.Name
				item.isInit = d.Name.Name == "init"
				item.isEntry = entryName(d.Name.Name)
			}
			o.decls = append(o.decls, item)
			mark(item.startLine, item.endLine, len(o.decls)-1)
		case *ast.GenDecl:
			from, to := line(fset, docStart(d.Doc, d.Pos())), line(fset, d.End())
			if d.Tok == token.IMPORT {
				kind(from, to, lineImport)
				continue
			}
			first := len(o.decls)
			for _, spec := range d.Specs {
				item := &decl{pos: spec.Pos(), end: spec.End()}
				var doc, comment *ast.CommentGroup
				switch s := spec.(type) {
				case *ast.TypeSpec:
					item.isType = true
					item.keys = []string{"type:" + s.Name.Name}
					item.display = "type " + s.Name.Name
					doc, comment = s.Doc, s.Comment
				case *ast.ValueSpec:
					names := []string{}
					for _, name := range s.Names {
						item.keys = append(item.keys, "value:"+name.Name)
						names = append(names, name.Name)
					}
					item.display = d.Tok.String() + " " + strings.Join(names, ", ")
					item.initializer = d.Tok == token.VAR && len(s.Values) > 0
					doc, comment = s.Doc, s.Comment
				}
				item.startLine, item.endLine = line(fset, docStart(doc, spec.Pos())), line(fset, spec.End())
				if comment != nil {
					item.endLine = line(fset, comment.End())
				}
				if !d.Lparen.IsValid() {
					item.startLine, item.endLine = from, to
				}
				o.decls = append(o.decls, item)
				mark(item.startLine, item.endLine, len(o.decls)-1)
			}
			// The lines of a group that belong to no spec, such as
			// "var (" or a comment between specs, belong to all of them.
			for l := from; l <= to && l <= len(text); l++ {
				if len(o.lines[l-1]) == 0 {
					for index := first; index < len(o.decls); index++ {
						o.lines[l-1] = append(o.lines[l-1], index)
					}
				}
			}
		}
	}
	return o
}
