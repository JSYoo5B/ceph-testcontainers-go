package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// listedPackage holds the fields of go list -json that the loader reads.
type listedPackage struct {
	ImportPath string
	Name       string
	Dir        string
	ForTest    string
	Export     string
	GoFiles    []string
	CgoFiles   []string
	Imports    []string
	ImportMap  map[string]string
	Incomplete bool
	Error      *struct{ Err string }
	DepsErrors []*struct{ Err string }
}

// goEnv is the environment of every go command: the CI build settings,
// for the platform that the batches run on.
func goEnv(goos, goarch string) []string {
	return append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=readonly", "GOWORK=off", "GOOS="+goos, "GOARCH="+goarch)
}

func goList(root string, env []string, args ...string) ([]listedPackage, error) {
	cmd := exec.Command("go", append([]string{"list", "-e", "-json"}, args...)...)
	cmd.Dir, cmd.Env = root, env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("go list: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	var packages []listedPackage
	decoder := json.NewDecoder(&stdout)
	for {
		var p listedPackage
		if err := decoder.Decode(&p); errors.Is(err, io.EOF) {
			return packages, nil
		} else if err != nil {
			return nil, err
		}
		packages = append(packages, p)
	}
}

func listError(p listedPackage) error {
	if p.Error != nil {
		return fmt.Errorf("%s: %s", p.ImportPath, p.Error.Err)
	}
	for _, e := range p.DepsErrors {
		return fmt.Errorf("%s: %s", p.ImportPath, e.Err)
	}
	if p.Incomplete {
		return fmt.Errorf("%s: incomplete package", p.ImportPath)
	}
	return nil
}

// testPackage finds the one test package that go test builds for pattern
// with tags: the external test package when there is one, otherwise the
// package with its internal test files.
func testPackage(root string, env []string, pattern, tags string) (listedPackage, error) {
	packages, err := goList(root, env, "-test", "-tags="+tags, pattern)
	if err != nil {
		return listedPackage{}, err
	}
	var plain, internal, external *listedPackage
	for i := range packages {
		p := &packages[i]
		if err := listError(*p); err != nil {
			return listedPackage{}, err
		}
		switch {
		case p.ForTest == "" && !strings.HasSuffix(p.ImportPath, ".test"):
			plain = p
		case strings.HasSuffix(p.ImportPath, "_test ["+p.ForTest+".test]"):
			external = p
		case p.ImportPath == p.ForTest+" ["+p.ForTest+".test]":
			internal = p
		}
	}
	if plain == nil {
		return listedPackage{}, fmt.Errorf("go list found no package %s", pattern)
	}
	if len(plain.CgoFiles) > 0 {
		return listedPackage{}, errors.New("cgo files are not analysed")
	}
	switch {
	case external != nil && (internal != nil || len(plain.GoFiles) > 0):
		return listedPackage{}, errors.New("a package with both its own and external test files is not analysed")
	case external != nil:
		return *external, nil
	case internal != nil:
		return *internal, nil
	}
	return *plain, nil
}

func realPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

// node is one top-level declaration of the analysed package.
type node struct {
	*decl
	file string
}

// graph is the reference graph of one type-checked build of the package.
type graph struct {
	fset  *token.FileSet
	files map[string]*outline // by path relative to the module root
	nodes []node
	// fileNodes[path][i] is the node of the file's i-th outline decl.
	fileNodes map[string][]int
	keys      map[string][]int
	reverse   [][]int
}

// load type-checks the test package of pattern under tags and builds its
// declaration reference graph.
func load(root, pattern, tags, goos, goarch string) (*graph, error) {
	env := goEnv(goos, goarch)
	target, err := testPackage(root, env, pattern, tags)
	if err != nil {
		return nil, err
	}
	imports := []string{}
	for _, path := range target.Imports {
		if strings.Contains(path, " [") {
			return nil, fmt.Errorf("the test package imports the test variant %s", path)
		}
		if path != "unsafe" && path != "C" {
			imports = append(imports, path)
		}
	}
	exports := map[string]string{}
	if len(imports) > 0 {
		deps, err := goList(root, env, append([]string{"-export", "-deps", "-tags=" + tags}, imports...)...)
		if err != nil {
			return nil, err
		}
		for _, p := range deps {
			if err := listError(p); err != nil {
				return nil, err
			}
			exports[p.ImportPath] = p.Export
		}
	}
	fset := token.NewFileSet()
	g := &graph{fset: fset, files: map[string]*outline{}, fileNodes: map[string][]int{}, keys: map[string][]int{}}
	var files []*ast.File
	absRoot, err := realPath(root)
	if err != nil {
		return nil, err
	}
	dir, err := realPath(target.Dir)
	if err != nil {
		return nil, err
	}
	for _, name := range target.GoFiles {
		path := filepath.Join(dir, name)
		src, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil {
			return nil, err
		}
		rel = filepath.ToSlash(rel)
		o := newOutline(fset, f, src)
		g.files[rel] = o
		for _, d := range o.decls {
			g.fileNodes[rel] = append(g.fileNodes[rel], len(g.nodes))
			for _, key := range d.keys {
				g.keys[key] = append(g.keys[key], len(g.nodes))
			}
			g.nodes = append(g.nodes, node{decl: d, file: rel})
		}
		files = append(files, f)
	}
	lookup := func(path string) (io.ReadCloser, error) {
		if mapped, ok := target.ImportMap[path]; ok {
			path = mapped
		}
		export, ok := exports[path]
		if !ok || export == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(export)
	}
	var typeErrors []error
	config := types.Config{
		Importer: importer.ForCompiler(fset, "gc", lookup),
		Sizes:    types.SizesFor("gc", goarch),
		Error:    func(err error) { typeErrors = append(typeErrors, err) },
	}
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
	pkg, _ := config.Check(target.ImportPath, fset, files, info)
	if len(typeErrors) > 0 {
		return nil, fmt.Errorf("type check: %v", typeErrors[0])
	}
	g.link(pkg, info)
	return g, nil
}

// at finds the node whose declaration contains pos.
func (g *graph) at(order []int, pos token.Pos) int {
	i := sort.Search(len(order), func(i int) bool { return g.nodes[order[i]].end > pos })
	if i < len(order) && g.nodes[order[i]].pos <= pos {
		return order[i]
	}
	return -1
}

// link adds an edge from a declaration to every declaration of the package
// that it names, resolved by go/types, and:
//   - from a type to each of its methods, since code outside the package,
//     such as fmt or testcontainers, can call any of them on a value;
//   - from a call through an interface method to that method of every type
//     of the package that implements the interface (class hierarchy
//     analysis).
//
// A method's receiver is not an edge to its type, or reaching one method
// would reach all of them.
func (g *graph) link(pkg *types.Package, info *types.Info) {
	order := make([]int, len(g.nodes))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return g.nodes[order[i]].pos < g.nodes[order[j]].pos })
	edges := make([]map[int]bool, len(g.nodes))
	for i := range edges {
		edges[i] = map[int]bool{}
	}
	add := func(from, to int) {
		if from >= 0 && to >= 0 && from != to {
			edges[from][to] = true
		}
	}
	for i, n := range g.nodes {
		if n.recv == "" {
			continue
		}
		for _, t := range g.keys["type:"+n.recv] {
			add(t, i)
		}
	}
	var named []*types.Named
	for _, name := range pkg.Scope().Names() {
		if tn, ok := pkg.Scope().Lookup(name).(*types.TypeName); ok && !tn.IsAlias() {
			if t, ok := tn.Type().(*types.Named); ok && !types.IsInterface(t) {
				named = append(named, t)
			}
		}
	}
	implementations := map[*types.Func][]int{}
	dispatch := func(method *types.Func) []int {
		if targets, ok := implementations[method]; ok {
			return targets
		}
		iface, _ := method.Signature().Recv().Type().Underlying().(*types.Interface)
		targets := []int{}
		for _, t := range named {
			var receiver types.Type = types.NewPointer(t)
			if iface != nil && t.TypeParams().Len() == 0 && !types.Implements(receiver, iface) {
				continue
			}
			obj, _, _ := types.LookupFieldOrMethod(receiver, false, method.Pkg(), method.Name())
			if fn, ok := obj.(*types.Func); ok && fn.Pkg() == pkg {
				targets = append(targets, g.at(order, fn.Pos()))
			}
		}
		implementations[method] = targets
		return targets
	}
	for ident, obj := range info.Uses {
		from := g.at(order, ident.Pos())
		if from < 0 {
			continue
		}
		if n := g.nodes[from]; n.recvPos.IsValid() && n.recvPos <= ident.Pos() && ident.Pos() < n.recvEnd {
			continue
		}
		if fn, ok := obj.(*types.Func); ok {
			if recv := fn.Signature().Recv(); recv != nil && types.IsInterface(recv.Type()) {
				for _, to := range dispatch(fn) {
					add(from, to)
				}
			}
		}
		if obj.Pkg() == pkg {
			add(from, g.at(order, obj.Pos()))
		}
	}
	g.reverse = make([][]int, len(g.nodes))
	for from, targets := range edges {
		for to := range targets {
			g.reverse[to] = append(g.reverse[to], from)
		}
	}
	for i := range g.reverse {
		sort.Ints(g.reverse[i])
	}
}

// affected returns, for every node that reaches a changed node, one changed
// node that it reaches.
func (g *graph) affected(changed []int) map[int]int {
	reach := map[int]int{}
	queue := []int{}
	for _, c := range changed {
		if _, ok := reach[c]; !ok {
			reach[c] = c
			queue = append(queue, c)
		}
	}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, from := range g.reverse[n] {
			if _, ok := reach[from]; !ok {
				reach[from] = reach[n]
				queue = append(queue, from)
			}
		}
	}
	return reach
}
