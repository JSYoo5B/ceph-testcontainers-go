// Command testsupportcheck proves, without Docker, that test-only code stays
// out of the ordinary build of the module.
//
// Test-only code lives in _test.go files wherever the Go toolchain lets a test
// see it. What tests of other packages need lives in helper packages under
// internal/ (the manifest's test_only_packages), which only _test.go files
// may import, and which reach unexported cluster state through one registered
// seam (hookreg.Test, set by ceph/unit_access.go). For each ordinary tag set
// this command requires, from go list metadata and the syntax of the files the
// build selects, that:
//
//   - every test-only package exists and no package imports it from a non-test
//     file (go list's Imports leave out _test.go files);
//   - no selected non-test file outside the test-only packages declares a
//     function or method whose name starts with Test, or a type of the
//     manifest's forbidden list (the moved test-config and test-state types);
//   - the seam (hookreg.Test) is read only in the manifest's seam files. The
//     import is resolved by path to its local binding, so a renamed import is
//     caught, a local variable or a different package that happens to be
//     called hookreg is not, and a dot import of the seam package is rejected
//     outright in a non-seam file because its identifiers cannot be attributed
//     without type information;
//   - no package reports a list error.
//
// The test-only packages stay compiled by go build ./... and go vet ./... : the
// rule limits who imports them, it does not remove them from the build.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// defaultTagSets are the ordinary and optional-feature tag sets of make
// tag-compile. The empty set is a plain build.
var defaultTagSets = []string{
	"",
	"integration",
	"all",
	"all,goceph,native_regression,role_images,ci_optional,ci_batch_role_routing,ci_batch_role_mirror_refresh",
	"all,cephfs_fuse",
}

// Manifest names the test-only code of the module.
type Manifest struct {
	// TestOnlyPackages are module-relative directories of helper packages that
	// only _test.go files may import.
	TestOnlyPackages []string `json:"test_only_packages"`
	// SeamSelector is the registry member that links test code to production
	// state, as <module-relative package directory>.<Name>, and SeamFiles the
	// only non-test files that read it.
	SeamSelector string   `json:"seam_selector"`
	SeamFiles    []string `json:"seam_files"`
	// ForbiddenTypes are type names that no ordinary non-test file outside the
	// test-only packages declares.
	ForbiddenTypes []string `json:"forbidden_types"`
}

type listedPackage struct {
	ImportPath string
	Dir        string
	Imports    []string
	GoFiles    []string
	CgoFiles   []string
	Error      *struct{ Err string }
	DepsErrors []*struct{ Err string }
}

func (p listedPackage) selected() []string { return append(slices.Clone(p.GoFiles), p.CgoFiles...) }

func main() {
	root := flag.String("root", ".", "module root")
	manifestPath := flag.String("manifest", "", "manifest file (default: internal/tools/testsupportcheck/manifest.json under the root)")
	tagSets := flag.String("tag-sets", "", "semicolon-separated ordinary tag sets (default: the tag-compile sets)")
	flag.Parse()
	sets := defaultTagSets
	if *tagSets != "" {
		sets = strings.Split(*tagSets, ";")
	}
	manifest := *manifestPath
	if manifest == "" {
		manifest = filepath.Join(*root, "internal", "tools", "testsupportcheck", "manifest.json")
	}
	problems, err := Check(*root, manifest, sets)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testsupportcheck:", err)
		os.Exit(2)
	}
	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "FAIL", p)
		}
		os.Exit(1)
	}
	fmt.Printf("test-only code stays out of the ordinary build (%d tag sets checked)\n", len(sets))
}

// Check returns the violations of the ordinary build in root, or an error
// when the check could not run.
func Check(root, manifestPath string, tagSets []string) ([]string, error) {
	var manifest Manifest
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("manifest: %v", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("manifest: trailing data after the top-level object")
	}
	if len(manifest.TestOnlyPackages) == 0 || !strings.Contains(manifest.SeamSelector, ".") {
		return nil, errors.New("manifest: test_only_packages and a package.Name seam_selector are required")
	}
	var problems []string
	for _, tags := range tagSets {
		found, err := checkTagSet(root, manifest, tags)
		if err != nil {
			return nil, err
		}
		problems = append(problems, found...)
	}
	return problems, nil
}

func goList(root, tags string, patterns ...string) ([]listedPackage, error) {
	args := []string{"list", "-e", "-json"}
	if tags != "" {
		args = append(args, "-tags="+tags)
	}
	cmd := exec.Command("go", append(args, patterns...)...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=readonly", "GOWORK=off")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("go list %s: %v: %s", strings.Join(args[3:], " "), err, strings.TrimSpace(stderr.String()))
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

func relDir(root, dir string) string {
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return ""
	}
	return filepath.ToSlash(rel)
}

func checkTagSet(root string, manifest Manifest, tags string) ([]string, error) {
	label := "tags=" + tags
	if tags == "" {
		label = "no tags"
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	patterns := []string{"./..."}
	for _, dir := range manifest.TestOnlyPackages {
		patterns = append(patterns, "./"+dir)
	}
	packages, err := goList(absRoot, tags, patterns...)
	if err != nil {
		return nil, err
	}
	var problems []string
	helperPaths := map[string]string{} // import path -> directory
	seen := map[string]bool{}
	scanned := map[string]bool{} // module-relative non-test file -> outside the test-only packages
	for _, pkg := range packages {
		if pkg.Dir == "" {
			continue
		}
		dir := relDir(absRoot, pkg.Dir)
		helper := slices.Contains(manifest.TestOnlyPackages, dir)
		if helper {
			helperPaths[pkg.ImportPath] = dir
			seen[dir] = seen[dir] || len(pkg.selected()) > 0
		}
		if pkg.Error != nil {
			problems = append(problems, fmt.Sprintf("[%s] %s: %s", label, pkg.ImportPath, pkg.Error.Err))
		}
		for _, e := range pkg.DepsErrors {
			problems = append(problems, fmt.Sprintf("[%s] %s: dependency error: %s", label, pkg.ImportPath, e.Err))
		}
		for _, file := range pkg.selected() {
			if !strings.HasSuffix(file, "_test.go") {
				scanned[path.Join(dir, file)] = !helper
			}
		}
	}
	for _, dir := range manifest.TestOnlyPackages {
		if !seen[dir] {
			problems = append(problems, fmt.Sprintf("[%s] test-only package %s has no selected file: the rule would be vacuous", label, dir))
		}
	}
	var importers []string
	for _, pkg := range packages {
		if pkg.Dir == "" || slices.Contains(manifest.TestOnlyPackages, relDir(absRoot, pkg.Dir)) {
			continue
		}
		for _, imported := range pkg.Imports {
			if dir, ok := helperPaths[imported]; ok {
				importers = append(importers, fmt.Sprintf("[%s] non-test files of %s import the test-only package %s", label, pkg.ImportPath, dir))
			}
		}
	}
	sort.Strings(importers)
	problems = append(problems, slices.Compact(importers)...)
	files := make([]string, 0, len(scanned))
	for file := range scanned {
		files = append(files, file)
	}
	sort.Strings(files)
	cut := strings.LastIndex(manifest.SeamSelector, ".")
	module, err := modulePath(absRoot)
	if err != nil {
		return nil, err
	}
	seamImport, seamName := path.Join(module, manifest.SeamSelector[:cut]), manifest.SeamSelector[cut+1:]
	for _, file := range files {
		found, err := scan(filepath.Join(absRoot, filepath.FromSlash(file)), manifest.ForbiddenTypes, seamImport, seamName)
		if err != nil {
			return nil, err
		}
		for _, f := range found {
			switch {
			case f.seam:
				if !slices.Contains(manifest.SeamFiles, file) {
					problems = append(problems, fmt.Sprintf("[%s] %s %s, which only the seam files may", label, file, f.what))
				}
			case scanned[file]:
				problems = append(problems, fmt.Sprintf("[%s] %s declares %s, which only a _test.go file or a test-only package may contain", label, file, f.what))
			}
		}
	}
	return problems, nil
}

type finding struct {
	what string
	seam bool
}

// modulePath is the module path of the module in root.
func modulePath(root string) (string, error) {
	cmd := exec.Command("go", "list", "-m")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %v", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// scan lists the test-only declarations of a file (a function or method named
// Test followed by a character that is not lower case, and the forbidden type
// names) and every mention of the seam selector.
func scan(file string, forbiddenTypes []string, seamImport, seamName string) ([]finding, error) {
	// Object resolution stays on: an identifier that resolves to a declaration
	// in the file (a local variable or parameter) is not the imported package.
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		return nil, err
	}
	var found []finding
	for _, decl := range parsed.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if name := decl.Name.Name; isTestName(name) {
				kind := "func "
				if decl.Recv != nil {
					kind = "method "
				}
				found = append(found, finding{what: kind + name})
			}
		case *ast.GenDecl:
			if decl.Tok != token.TYPE {
				continue
			}
			for _, spec := range decl.Specs {
				if name := spec.(*ast.TypeSpec).Name.Name; slices.Contains(forbiddenTypes, name) {
					found = append(found, finding{what: "type " + name})
				}
			}
		}
	}
	binding := ""
	for _, spec := range parsed.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil || importPath != seamImport {
			continue
		}
		switch {
		case spec.Name == nil:
			binding = path.Base(importPath)
		case spec.Name.Name == ".":
			found = append(found, finding{what: "dot-imports the seam package", seam: true})
		case spec.Name.Name != "_":
			binding = spec.Name.Name
		}
	}
	if binding != "" {
		ast.Inspect(parsed, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == seamName {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == binding && id.Obj == nil {
					found = append(found, finding{what: "reads the seam", seam: true})
				}
			}
			return true
		})
	}
	return found, nil
}

func isTestName(name string) bool {
	rest, ok := strings.CutPrefix(name, "Test")
	if !ok {
		return false
	}
	return rest == "" || !(rest[0] >= 'a' && rest[0] <= 'z')
}
