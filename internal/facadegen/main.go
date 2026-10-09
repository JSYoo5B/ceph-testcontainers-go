// Command facadegen writes the public ceph, cephfs, rgw and rbd packages'
// api.go files. They re-export the implementation in internal/cluster and
// internal/multicluster under service-scoped names: types become aliases,
// functions become documented wrappers and constants keep their values.
//
// Run it from the repository root with "go run ./internal/facadegen".
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const module = "github.com/jsyoo5b/ceph-testcontainers-go"

// Facades lists the generated packages in output order.
var Facades = []string{"ceph", "cephfs", "rgw", "rbd"}

type source struct {
	dir, pkg, alias string
}

var sources = []source{
	{"internal/cluster", "cluster", "cluster"},
	{"internal/multicluster", "multicluster", "multicluster"},
}

// Names that the generic prefix rule would not produce, and service entry
// points whose public package differs from their source file.
var special = map[string][2]string{
	"cluster.CephFSContainer":    {"cephfs", "Filesystem"},
	"cluster.MDSContainer":       {"cephfs", "MDS"},
	"cluster.CephFSMDSStatus":    {"cephfs", "FilesystemStatus"},
	"cluster.MDSStatus":          {"cephfs", "MDSStatus"},
	"cluster.RGWContainer":       {"rgw", "Gateway"},
	"cluster.WithCephFS":         {"cephfs", "WithFilesystems"},
	"cluster.WithMDSImage":       {"cephfs", "WithMDSImage"},
	"cluster.WithRGW":            {"rgw", "WithGateways"},
	"cluster.WithRGWImage":       {"rgw", "WithImage"},
	"cluster.StartCephFS":        {"cephfs", "Start"},
	"cluster.Filesystems":        {"cephfs", "Filesystems"},
	"cluster.StartRGW":           {"rgw", "Start"},
	"cluster.RemoveRGW":          {"rgw", "Remove"},
	"cluster.Gateways":           {"rgw", "Gateways"},
	"cluster.GatewaysContext":    {"rgw", "GatewaysContext"},
	"cluster.InitRBDPool":        {"rbd", "InitPool"},
	"cluster.CreateRBDNamespace": {"rbd", "CreateNamespace"},
	"cluster.ListRBDNamespaces":  {"rbd", "ListNamespaces"},
	"cluster.RemoveRBDNamespace": {"rbd", "RemoveNamespace"},
	"cluster.WithRBDPools":       {"rbd", "WithPools"},
}

// Internal helpers used by the hand-written Run functions.
var skipped = map[string]bool{
	"cluster.DefaultCephFS": true, "cluster.DefaultRGW": true, "cluster.DefaultRBDPool": true,
}

// Former Container methods that implementation comments may still mention.
var formerNames = map[string][2]string{
	"StartCephFSWithConfig": {"cephfs", "Start"},
	"StartRGWWithConfig":    {"rgw", "Start"},
}

func target(src source, file, name string) (string, string) {
	key := src.pkg + "." + name
	if mapped, ok := special[key]; ok {
		return mapped[0], mapped[1]
	}
	service := ""
	switch {
	case strings.HasPrefix(file, "cephfs"), file == "no_initial_mds.go":
		service = "cephfs"
	case strings.HasPrefix(file, "rgw"):
		service = "rgw"
	case strings.HasPrefix(file, "rbd"):
		service = "rbd"
	}
	if service == "" {
		if src.pkg == "multicluster" {
			panic("unassigned multicluster declaration " + name + " in " + file)
		}
		return "ceph", name
	}
	stripped := name
	for _, prefix := range []string{"CephFS", "RGW", "RBD"} {
		stripped = strings.Replace(stripped, prefix, "", 1)
	}
	if stripped == "" || !ast.IsExported(stripped) {
		panic("cannot derive a public name for " + key)
	}
	return service, stripped
}

type decl struct {
	src            source
	file, name     string
	pkg, public    string
	kind           string // type, func, const, var
	doc            string
	funcType       *ast.FuncType
	imports        map[string]string // name -> path used by the source file
	order          int
	constGroupHead bool
}

type generator struct {
	decls  []*decl
	byImpl map[string]*decl // "cluster.Name" -> decl
}

func (g *generator) load(root string) error {
	g.byImpl = map[string]*decl{}
	order := 0
	for _, src := range sources {
		files, err := filepath.Glob(filepath.Join(root, src.dir, "*.go"))
		if err != nil {
			return err
		}
		sort.Strings(files)
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			imports := map[string]string{}
			for _, spec := range file.Imports {
				importPath := strings.Trim(spec.Path.Value, `"`)
				name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(importPath), "go-"), "-go")
				if spec.Name != nil {
					name = spec.Name.Name
				}
				imports[name] = importPath
			}
			base := filepath.Base(path)
			add := func(name, kind, doc string, fn *ast.FuncType) {
				if !ast.IsExported(name) || skipped[src.pkg+"."+name] {
					return
				}
				pkg, public := target(src, base, name)
				d := &decl{src: src, file: base, name: name, pkg: pkg, public: public, kind: kind, doc: doc,
					funcType: fn, imports: imports, order: order}
				order++
				g.decls = append(g.decls, d)
				g.byImpl[src.pkg+"."+name] = d
			}
			for _, d := range file.Decls {
				switch d := d.(type) {
				case *ast.FuncDecl:
					if d.Recv == nil {
						add(d.Name.Name, "func", d.Doc.Text(), d.Type)
					}
				case *ast.GenDecl:
					for _, spec := range d.Specs {
						doc := d.Doc.Text()
						switch spec := spec.(type) {
						case *ast.TypeSpec:
							if spec.Doc != nil {
								doc = spec.Doc.Text()
							}
							add(spec.Name.Name, "type", doc, nil)
						case *ast.ValueSpec:
							if spec.Doc != nil {
								doc = spec.Doc.Text()
							} else if len(d.Specs) > 1 {
								doc = ""
							}
							kind := "var"
							if d.Tok == token.CONST {
								kind = "const"
							}
							for _, n := range spec.Names {
								add(n.Name, kind, doc, nil)
							}
						}
					}
				}
			}
		}
	}
	seen := map[string]string{}
	for _, d := range g.decls {
		key := d.pkg + "." + d.public
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("public name %s is produced by both %s and %s.%s", key, previous, d.src.pkg, d.name)
		}
		seen[key] = d.src.pkg + "." + d.name
	}
	return nil
}

// ref returns how package pkg refers to the implementation name, recording
// any public package it must import.
func (g *generator) ref(pkg, impl string, uses map[string]bool) (string, bool) {
	d, ok := g.byImpl[impl]
	if !ok {
		return "", false
	}
	if d.pkg == pkg {
		return d.public, true
	}
	uses[d.pkg] = true
	return d.pkg + "." + d.public, true
}

// rewrite copies a type expression, replacing implementation type names with
// their public names as seen from package pkg.
func (g *generator) rewrite(expr ast.Expr, d *decl, pkg string, uses map[string]bool, imports map[string]string) ast.Expr {
	switch e := expr.(type) {
	case nil:
		return nil
	case *ast.Ident:
		if public, ok := g.ref(pkg, d.src.pkg+"."+e.Name, uses); ok {
			return ast.NewIdent(public)
		}
		return ast.NewIdent(e.Name)
	case *ast.SelectorExpr:
		qualifier := e.X.(*ast.Ident).Name
		path := d.imports[qualifier]
		if path == module+"/internal/cluster" {
			if public, ok := g.ref(pkg, "cluster."+e.Sel.Name, uses); ok {
				return ast.NewIdent(public)
			}
			panic("unexported cluster reference " + e.Sel.Name)
		}
		imports[qualifier] = path
		return &ast.SelectorExpr{X: ast.NewIdent(qualifier), Sel: ast.NewIdent(e.Sel.Name)}
	case *ast.StarExpr:
		return &ast.StarExpr{X: g.rewrite(e.X, d, pkg, uses, imports)}
	case *ast.ArrayType:
		return &ast.ArrayType{Len: e.Len, Elt: g.rewrite(e.Elt, d, pkg, uses, imports)}
	case *ast.MapType:
		return &ast.MapType{Key: g.rewrite(e.Key, d, pkg, uses, imports), Value: g.rewrite(e.Value, d, pkg, uses, imports)}
	case *ast.Ellipsis:
		return &ast.Ellipsis{Elt: g.rewrite(e.Elt, d, pkg, uses, imports)}
	case *ast.ChanType:
		return &ast.ChanType{Dir: e.Dir, Value: g.rewrite(e.Value, d, pkg, uses, imports)}
	case *ast.FuncType:
		return g.rewriteFunc(e, d, pkg, uses, imports, nil)
	case *ast.InterfaceType:
		if len(e.Methods.List) == 0 {
			return &ast.InterfaceType{Methods: &ast.FieldList{}}
		}
	}
	panic(fmt.Sprintf("unsupported type expression %T in %s", expr, d.name))
}

func (g *generator) rewriteFunc(fn *ast.FuncType, d *decl, pkg string, uses map[string]bool, imports map[string]string, names *[]string) *ast.FuncType {
	out := &ast.FuncType{Params: &ast.FieldList{}}
	index := 0
	for _, field := range fn.Params.List {
		copied := &ast.Field{Type: g.rewrite(field.Type, d, pkg, uses, imports)}
		if len(field.Names) == 0 {
			name := fmt.Sprintf("arg%d", index)
			index++
			copied.Names = []*ast.Ident{ast.NewIdent(name)}
			if names != nil {
				*names = append(*names, name)
			}
		}
		for _, n := range field.Names {
			name := n.Name
			if name == "_" {
				name = fmt.Sprintf("arg%d", index)
			}
			index++
			copied.Names = append(copied.Names, ast.NewIdent(name))
			if names != nil {
				*names = append(*names, name)
			}
		}
		out.Params.List = append(out.Params.List, copied)
	}
	if fn.Results != nil {
		out.Results = &ast.FieldList{}
		for _, field := range fn.Results.List {
			copied := &ast.Field{Type: g.rewrite(field.Type, d, pkg, uses, imports)}
			for _, n := range field.Names {
				copied.Names = append(copied.Names, ast.NewIdent(n.Name))
			}
			out.Results.List = append(out.Results.List, copied)
		}
	}
	return out
}

var word = regexp.MustCompile(`\b(?:ceph\.)?[A-Z][A-Za-z0-9]*\b`)

// docFor rewrites implementation names in a comment to the names visible from
// package pkg. Unknown words are left unchanged.
func (g *generator) docFor(text, pkg string) string {
	uses := map[string]bool{}
	return word.ReplaceAllStringFunc(text, func(match string) string {
		name := strings.TrimPrefix(match, "ceph.")
		for _, src := range sources {
			if public, ok := g.ref(pkg, src.pkg+"."+name, uses); ok {
				return public
			}
		}
		if former, ok := formerNames[name]; ok {
			if former[0] == pkg {
				return former[1]
			}
			return former[0] + "." + former[1]
		}
		return match
	})
}

func comment(text string) string {
	text = strings.TrimRight(text, "\n")
	if text == "" {
		return ""
	}
	var b strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			b.WriteString("//\n")
		} else {
			b.WriteString("// " + line + "\n")
		}
	}
	return b.String()
}

func render(node ast.Node) string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return buf.String()
}

// Generate returns the formatted api.go for one facade package.
func (g *generator) Generate(pkg string) ([]byte, error) {
	uses := map[string]bool{}
	imports := map[string]string{}
	internal := map[string]bool{}
	var body strings.Builder
	var consts, vars []*decl
	for _, d := range g.decls {
		if d.pkg != pkg {
			continue
		}
		internal[d.src.pkg] = true
		impl := d.src.alias + "." + d.name
		switch d.kind {
		case "type":
			body.WriteString(comment(g.docFor(d.doc, pkg)))
			fmt.Fprintf(&body, "type %s = %s\n\n", d.public, impl)
		case "func":
			var names []string
			fn := g.rewriteFunc(d.funcType, d, pkg, uses, imports, &names)
			args := make([]string, len(names))
			copy(args, names)
			last := d.funcType.Params.List
			if len(last) > 0 {
				if _, ok := last[len(last)-1].Type.(*ast.Ellipsis); ok {
					args[len(args)-1] += "..."
				}
			}
			signature := strings.TrimPrefix(render(fn), "func")
			call := fmt.Sprintf("%s(%s)", impl, strings.Join(args, ", "))
			body.WriteString(comment(g.docFor(d.doc, pkg)))
			if fn.Results == nil || len(fn.Results.List) == 0 {
				fmt.Fprintf(&body, "func %s%s {\n\t%s\n}\n\n", d.public, signature, call)
			} else {
				fmt.Fprintf(&body, "func %s%s {\n\treturn %s\n}\n\n", d.public, signature, call)
			}
		case "const":
			consts = append(consts, d)
		case "var":
			vars = append(vars, d)
		}
	}
	var values strings.Builder
	for _, group := range []struct {
		keyword string
		decls   []*decl
	}{{"const", consts}, {"var", vars}} {
		if len(group.decls) == 0 {
			continue
		}
		fmt.Fprintf(&values, "%s (\n", group.keyword)
		for _, d := range group.decls {
			for _, line := range strings.Split(strings.TrimRight(comment(g.docFor(d.doc, pkg)), "\n"), "\n") {
				if line != "" {
					values.WriteString("\t" + line + "\n")
				}
			}
			fmt.Fprintf(&values, "\t%s = %s.%s\n", d.public, d.src.alias, d.name)
		}
		values.WriteString(")\n\n")
	}
	var out strings.Builder
	out.WriteString("// Code generated by internal/facadegen. DO NOT EDIT.\n\n")
	fmt.Fprintf(&out, "package %s\n\nimport (\n", pkg)
	var paths []string
	var standard []string
	for name, path := range imports {
		spec := fmt.Sprintf("%q", path)
		if strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "go-"), "-go") != name {
			spec = name + " " + spec
		}
		if strings.Contains(strings.Split(path, "/")[0], ".") {
			paths = append(paths, spec)
		} else {
			standard = append(standard, spec)
		}
	}
	sort.Strings(standard)
	for _, spec := range standard {
		out.WriteString("\t" + spec + "\n")
	}
	if len(standard) > 0 {
		out.WriteString("\n")
	}
	for public := range uses {
		paths = append(paths, fmt.Sprintf("%q", module+"/"+public))
	}
	for src := range internal {
		for _, s := range sources {
			if s.pkg == src {
				paths = append(paths, fmt.Sprintf("%q", module+"/"+s.dir))
			}
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		out.WriteString("\t" + path + "\n")
	}
	out.WriteString(")\n\n")
	out.WriteString(values.String())
	out.WriteString(body.String())
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		return nil, fmt.Errorf("format %s: %w\n%s", pkg, err, out.String())
	}
	return formatted, nil
}

// Load parses the implementation packages below root.
func Load(root string) (*generator, error) {
	g := &generator{}
	return g, g.load(root)
}

func main() {
	g, err := Load(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, pkg := range Facades {
		data, err := g.Generate(pkg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := os.MkdirAll(pkg, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(pkg, "api.go"), data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
