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
	"path"
	"path/filepath"
	"slices"
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
	target, own, err := testPackages(root, env, pattern, tags)
	if err == nil && own != nil {
		err = errors.New("a package with both its own and external test files is not analysed")
	}
	return target, err
}

// testPackages is testPackage that accepts a package with both its own and
// external test files: it then returns the external test package and the
// package that it imports, the package's own test variant or the plain
// package.
func testPackages(root string, env []string, pattern, tags string) (listedPackage, *listedPackage, error) {
	packages, err := goList(root, env, "-test", "-tags="+tags, pattern)
	if err != nil {
		return listedPackage{}, nil, err
	}
	var plain, internal, external *listedPackage
	for i := range packages {
		p := &packages[i]
		if err := listError(*p); err != nil {
			return listedPackage{}, nil, err
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
		return listedPackage{}, nil, fmt.Errorf("go list found no package %s", pattern)
	}
	if len(plain.CgoFiles) > 0 {
		return listedPackage{}, nil, errors.New("cgo files are not analysed")
	}
	switch {
	case external != nil && internal != nil:
		return *external, internal, nil
	case external != nil && len(plain.GoFiles) > 0:
		return *external, plain, nil
	case external != nil:
		return *external, nil, nil
	case internal != nil:
		return *internal, nil, nil
	}
	return *plain, nil, nil
}

func realPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

// node is one top-level declaration of an analysed package.
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
	// imports are the import paths of each file.
	imports map[string][]string
	// values are the string constants that each node uses, when load was
	// asked for them.
	values [][]value
}

// loadOptions select what load accepts and records beyond the graph.
type loadOptions struct {
	// mixed accepts a package with both its own and external test files:
	// the package that the external one imports joins the graph from source.
	mixed bool
	// values records the string constants of every node.
	values bool
}

// load type-checks the test package of pattern under tags, with the
// non-test packages of the with directories from source, and builds the
// declaration reference graph of all of them.
func load(root, pattern string, with []string, tags, goos, goarch string, opts loadOptions) (*graph, error) {
	env := goEnv(goos, goarch)
	var target listedPackage
	var own *listedPackage
	var err error
	if opts.mixed {
		target, own, err = testPackages(root, env, pattern, tags)
	} else {
		target, err = testPackage(root, env, pattern, tags)
	}
	if err != nil {
		return nil, err
	}
	var companions []listedPackage
	for _, dir := range with {
		listed, err := goList(root, env, "-tags="+tags, "./"+dir)
		if err != nil {
			return nil, err
		}
		if len(listed) != 1 {
			return nil, fmt.Errorf("go list found %d packages in %s", len(listed), dir)
		}
		if listed[0].Error != nil && strings.Contains(listed[0].Error.Err, "build constraints exclude all Go files") {
			continue
		}
		if err := listError(listed[0]); err != nil {
			return nil, err
		}
		companions = append(companions, listed[0])
	}
	imports := []string{}
	for _, p := range append([]listedPackage{target}, companions...) {
		for _, path := range p.Imports {
			if strings.Contains(path, " [") && !opts.mixed {
				return nil, fmt.Errorf("the test package imports the test variant %s", path)
			}
			if path != "unsafe" && path != "C" {
				imports = append(imports, path)
			}
		}
	}
	exports := map[string]string{}
	if opts.mixed {
		// A dependency that imports the package under test is rebuilt for its
		// test as "dep [pkg.test]"; only the test build lists its export data.
		deps, err := goList(root, env, "-export", "-deps", "-test", "-tags="+tags, pattern)
		if err != nil {
			return nil, err
		}
		for _, p := range deps {
			if err := listError(p); err != nil {
				return nil, err
			}
			if p.Export != "" {
				exports[p.ImportPath] = p.Export
			}
		}
	} else if len(imports) > 0 {
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
	g := &graph{fset: fset, files: map[string]*outline{}, fileNodes: map[string][]int{}, keys: map[string][]int{},
		imports: map[string][]string{}}
	absRoot, err := realPath(root)
	if err != nil {
		return nil, err
	}
	// parse adds the files of one package to the graph. A declaration key
	// is qualified by its package, so names of different packages differ.
	parse := func(p listedPackage) ([]*ast.File, error) {
		dir, err := realPath(p.Dir)
		if err != nil {
			return nil, err
		}
		var files []*ast.File
		for _, name := range p.GoFiles {
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
			g.imports[rel] = o.imports
			for _, d := range o.decls {
				g.fileNodes[rel] = append(g.fileNodes[rel], len(g.nodes))
				for _, key := range d.keys {
					key = filepath.ToSlash(filepath.Dir(rel)) + ":" + key
					g.keys[key] = append(g.keys[key], len(g.nodes))
				}
				g.nodes = append(g.nodes, node{decl: d, file: rel})
			}
			files = append(files, f)
		}
		return files, nil
	}
	lookup := func(path string) (io.ReadCloser, error) {
		if mapped, ok := target.ImportMap[path]; ok {
			path = mapped
		} else if own != nil {
			if mapped, ok := own.ImportMap[path]; ok {
				path = mapped
			}
		}
		export, ok := exports[path]
		if !ok || export == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(export)
	}
	var typeErrors []error
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
	if opts.values {
		info.Types = map[ast.Expr]types.TypeAndValue{}
	}
	var parsed [][]*ast.File
	checked := map[string]*types.Package{}
	exported := importer.ForCompiler(fset, "gc", lookup)
	config := types.Config{
		Importer: importerFunc(func(path string) (*types.Package, error) {
			if p, ok := checked[path]; ok {
				return p, nil
			}
			return exported.Import(path)
		}),
		Sizes: types.SizesFor("gc", goarch),
		Error: func(err error) { typeErrors = append(typeErrors, err) },
	}
	var pkgs []*types.Package
	check := func(p listedPackage, path string) (*types.Package, error) {
		files, err := parse(p)
		if err != nil {
			return nil, err
		}
		pkg, _ := config.Check(path, fset, files, info)
		if len(typeErrors) > 0 {
			return nil, fmt.Errorf("type check: %v", typeErrors[0])
		}
		pkgs = append(pkgs, pkg)
		parsed = append(parsed, files)
		return pkg, nil
	}
	// The package's own test variant is checked from source for its own
	// tests and declarations, but the external test package imports it from
	// export data, like the dependencies that import it: one package must have
	// one identity. Its objects that the external package uses are mapped
	// back to their declarations by file and line.
	var ownPath string
	if own != nil {
		ownPath = own.ImportPath
		if own.ForTest != "" {
			ownPath = own.ForTest
		}
		if _, err := check(*own, ownPath); err != nil {
			return nil, err
		}
	}
	// Companions first: the test package imports them from source.
	for _, p := range append(companions, target) {
		pkg, err := check(p, p.ImportPath)
		if err != nil {
			return nil, err
		}
		checked[p.ImportPath] = pkg
	}
	var resolve func(types.Object) int
	if ownPath != "" {
		resolve = func(obj types.Object) int {
			if obj.Pkg() == nil || obj.Pkg().Path() != ownPath || !obj.Pos().IsValid() {
				return -1
			}
			position := fset.Position(obj.Pos())
			file, err := realPath(position.Filename)
			if err != nil {
				return -1
			}
			rel, err := filepath.Rel(absRoot, file)
			if err != nil {
				return -1
			}
			for _, n := range g.fileNodes[filepath.ToSlash(rel)] {
				if fset.Position(g.nodes[n].pos).Line <= position.Line && position.Line <= fset.Position(g.nodes[n].end).Line {
					return n
				}
			}
			return -1
		}
	}
	g.link(pkgs, info, resolve)
	if opts.values {
		g.collectValues(parsed, info)
	}
	return g, nil
}

type importerFunc func(path string) (*types.Package, error)

func (f importerFunc) Import(path string) (*types.Package, error) { return f(path) }

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
//
// resolve, when set, maps an object of a package that was imported from
// export data, although its declarations are in the graph, to its node.
func (g *graph) link(pkgs []*types.Package, info *types.Info, resolve func(types.Object) int) {
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
		for _, t := range g.keys[path.Dir(n.file)+":type:"+n.recv] {
			add(t, i)
		}
	}
	var named []*types.Named
	for _, pkg := range pkgs {
		for _, name := range pkg.Scope().Names() {
			if tn, ok := pkg.Scope().Lookup(name).(*types.TypeName); ok && !tn.IsAlias() {
				if t, ok := tn.Type().(*types.Named); ok && !types.IsInterface(t) {
					named = append(named, t)
				}
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
			if fn, ok := obj.(*types.Func); ok && slices.Contains(pkgs, fn.Pkg()) {
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
		if slices.Contains(pkgs, obj.Pkg()) {
			add(from, g.at(order, obj.Pos()))
		} else if resolve != nil {
			add(from, resolve(obj))
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
