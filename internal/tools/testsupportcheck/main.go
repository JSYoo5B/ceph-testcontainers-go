// Command testsupportcheck proves that an ordinary build of the module does
// not contain the test-support code.
//
// Test-only implementations (synthetic cluster constructors, privileged state
// accessors, the hook methods behind them and the shared unit-test fakes) are
// compiled only with the test_support build tag. A test command selects the tag
// explicitly; go test never does. The tag is not a member of any tag set that
// the repository uses for ordinary or optional-feature builds ("all", the CI
// profile tags, goceph and so on). Compiling test files with the tag enabled
// cannot show that an ordinary build excludes the code, so this command checks
// the ordinary build separately and without Docker, from go list metadata and
// the syntax of the files an ordinary build selects.
//
// For each ordinary tag set it requires that:
//   - the tag set does not itself contain test_support;
//   - every file of the manifest is ignored by the build constraints, and is
//     selected once test_support is added (so the tag, and nothing else, is
//     what excludes it);
//   - every package of the manifest's empty list has no selected file;
//   - no package of the module reports a list error (an importer of an empty
//     package would report one);
//   - no non-test file that the build selects declares a function or method
//     whose name starts with Test, or a type of the manifest's forbidden list.
//
// A test-only file that loses its tag is therefore reported by name: it is
// then selected, and it is no longer ignored.
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
	"strings"
)

// SupportTag is the build tag that selects the test-support code.
const SupportTag = "test_support"

// defaultTagSets are the ordinary and optional-feature tag sets of make
// tag-compile. The empty set is a plain build.
var defaultTagSets = []string{
	"",
	"integration",
	"all",
	"all,goceph,native_regression,role_images,ci_optional,ci_batch_role_routing,ci_batch_role_mirror_refresh",
	"all,cephfs_fuse",
}

// Manifest names the code that must stay out of an ordinary build.
type Manifest struct {
	// Files are module-relative slash paths of non-test files that only a
	// test_support build may select.
	Files []string `json:"files"`
	// EmptyPackages are module-relative directories whose files are all in
	// Files: an ordinary build selects nothing there.
	EmptyPackages []string `json:"empty_packages"`
	// ForbiddenTypes are type names that no ordinary non-test file declares.
	ForbiddenTypes []string `json:"forbidden_types"`
}

type listedPackage struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
	CgoFiles       []string
	IgnoredGoFiles []string
	Error          *struct{ Err string }
	DepsErrors     []*struct{ Err string }
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
	fmt.Printf("ordinary build excludes test support (%d tag sets checked)\n", len(sets))
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
	if len(manifest.Files) == 0 {
		return nil, errors.New("manifest: no test-support file is listed")
	}
	var problems []string
	for _, tags := range tagSets {
		if slices.Contains(strings.Split(tags, ","), SupportTag) {
			problems = append(problems, fmt.Sprintf("tag set %q selects %s: it is not an ordinary build", tags, SupportTag))
			continue
		}
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

func relative(root, dir string) (string, error) {
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(rel), nil
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
	// Resolve symlinks of the root so that go list's directories compare.
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	patterns := []string{"./..."}
	for _, dir := range manifest.EmptyPackages {
		patterns = append(patterns, "./"+dir)
	}
	for _, file := range manifest.Files {
		patterns = append(patterns, "./"+path.Dir(file))
	}
	ordinary, err := goList(absRoot, tags, patterns...)
	if err != nil {
		return nil, err
	}
	withSupport, err := goList(absRoot, tags+commaIf(tags)+SupportTag, patterns...)
	if err != nil {
		return nil, err
	}
	selectedOrdinary, selectedSupport := map[string]bool{}, map[string]bool{}
	ignored := map[string]bool{}
	var problems []string
	emptyErrors := map[string]bool{}
	for _, pkg := range ordinary {
		if pkg.Dir == "" {
			// A pattern that names no directory; the manifest checks below
			// report the file or package that is missing.
			continue
		}
		dir, err := relative(absRoot, pkg.Dir)
		if err != nil {
			return nil, err
		}
		for _, file := range pkg.selected() {
			selectedOrdinary[path.Join(dir, file)] = true
		}
		for _, file := range pkg.IgnoredGoFiles {
			ignored[path.Join(dir, file)] = true
		}
		if slices.Contains(manifest.EmptyPackages, dir) {
			emptyErrors[dir] = true
			continue
		}
		if pkg.Error != nil {
			problems = append(problems, fmt.Sprintf("[%s] %s: %s", label, pkg.ImportPath, pkg.Error.Err))
		}
		for _, e := range pkg.DepsErrors {
			problems = append(problems, fmt.Sprintf("[%s] %s: dependency error: %s", label, pkg.ImportPath, e.Err))
		}
	}
	for _, pkg := range withSupport {
		if pkg.Dir == "" {
			continue
		}
		dir, err := relative(absRoot, pkg.Dir)
		if err != nil {
			return nil, err
		}
		for _, file := range pkg.selected() {
			selectedSupport[path.Join(dir, file)] = true
		}
	}
	for _, file := range manifest.Files {
		switch {
		case selectedOrdinary[file]:
			problems = append(problems, fmt.Sprintf("[%s] test-support file %s is selected by an ordinary build", label, file))
		case !ignored[file]:
			problems = append(problems, fmt.Sprintf("[%s] test-support file %s is neither selected nor ignored: it does not exist", label, file))
		}
		if !selectedSupport[file] {
			problems = append(problems, fmt.Sprintf("[%s] test-support file %s is not selected with the %s tag, so the tag is not what excludes it", label, file, SupportTag))
		}
	}
	for _, dir := range manifest.EmptyPackages {
		if !emptyErrors[dir] {
			problems = append(problems, fmt.Sprintf("[%s] package %s was not listed", label, dir))
		}
		for file := range selectedOrdinary {
			if path.Dir(file) == dir {
				problems = append(problems, fmt.Sprintf("[%s] package %s has the selected file %s: it must be empty in an ordinary build", label, dir, file))
			}
		}
	}
	files := make([]string, 0, len(selectedOrdinary))
	for file := range selectedOrdinary {
		files = append(files, file)
	}
	sort.Strings(files)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		found, err := declaredTestSupport(filepath.Join(absRoot, filepath.FromSlash(file)), manifest.ForbiddenTypes)
		if err != nil {
			return nil, err
		}
		for _, name := range found {
			problems = append(problems, fmt.Sprintf("[%s] %s declares %s, which only a test-support build may contain", label, file, name))
		}
	}
	return problems, nil
}

func commaIf(tags string) string {
	if tags == "" {
		return ""
	}
	return ","
}

// declaredTestSupport lists the top-level declarations of a file that name
// test support: a function or method whose name starts with "Test" followed by
// a letter that is not lower case, and the forbidden type names.
func declaredTestSupport(file string, forbiddenTypes []string) ([]string, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var found []string
	for _, decl := range parsed.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			name := decl.Name.Name
			if isTestName(name) {
				if decl.Recv != nil {
					found = append(found, "method "+name)
				} else {
					found = append(found, "func "+name)
				}
			}
		case *ast.GenDecl:
			if decl.Tok != token.TYPE {
				continue
			}
			for _, spec := range decl.Specs {
				if name := spec.(*ast.TypeSpec).Name.Name; slices.Contains(forbiddenTypes, name) {
					found = append(found, "type "+name)
				}
			}
		}
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
