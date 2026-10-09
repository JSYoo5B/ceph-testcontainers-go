// Command tagcatalog reads test declarations and build expressions without
// importing or executing packages from the repository it inspects.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

type expression struct {
	Op       string        `json:"op"`
	Tag      string        `json:"tag,omitempty"`
	Children []*expression `json:"children,omitempty"`
}

type file struct {
	Path       string      `json:"path"`
	Package    string      `json:"package"`
	Constraint *expression `json:"constraint"`
	Tags       []string    `json:"tags"`
	Tests      []string    `json:"tests"`
	Metadata   []string    `json:"metadata"`
	SHA256     string      `json:"sha256"`
}

func convert(e constraint.Expr, tags map[string]bool) *expression {
	switch v := e.(type) {
	case *constraint.TagExpr:
		tags[v.Tag] = true
		return &expression{Op: "tag", Tag: v.Tag}
	case *constraint.NotExpr:
		return &expression{Op: "not", Children: []*expression{convert(v.X, tags)}}
	case *constraint.AndExpr:
		return &expression{Op: "and", Children: []*expression{convert(v.X, tags), convert(v.Y, tags)}}
	case *constraint.OrExpr:
		return &expression{Op: "or", Children: []*expression{convert(v.X, tags), convert(v.Y, tags)}}
	default:
		panic("unexpected Go build expression")
	}
}

func testName(name string) bool {
	if !strings.HasPrefix(name, "Test") || name == "TestMain" {
		return false
	}
	for _, r := range name[len("Test"):] {
		return !unicode.IsLower(r)
	}
	return true
}

func inspect(root, path string) (file, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return file{}, err
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, data, parser.ParseComments)
	if err != nil {
		return file{}, err
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return file{}, err
	}
	hash := sha256.Sum256(data)
	out := file{Path: filepath.ToSlash(rel), Package: f.Name.Name, Tests: []string{}, Tags: []string{}, Metadata: []string{}, SHA256: hex.EncodeToString(hash[:])}
	for _, group := range f.Comments {
		if group.Pos() > f.Package {
			continue
		}
		for _, comment := range group.List {
			if constraint.IsGoBuild(comment.Text) {
				if out.Constraint != nil {
					return file{}, fmt.Errorf("%s: multiple go:build lines", rel)
				}
				expr, err := constraint.Parse(comment.Text)
				if err != nil {
					return file{}, err
				}
				tags := map[string]bool{}
				out.Constraint = convert(expr, tags)
				for tag := range tags {
					out.Tags = append(out.Tags, tag)
				}
				sort.Strings(out.Tags)
			}
			if strings.HasPrefix(comment.Text, "//ci:") {
				out.Metadata = append(out.Metadata, strings.TrimSpace(strings.TrimPrefix(comment.Text, "//ci:")))
			}
		}
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !testName(fn.Name.Name) {
			continue
		}
		if fn.Type.TypeParams != nil || fn.Type.Results != nil || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
			return file{}, fmt.Errorf("%s: invalid test signature for %s", rel, fn.Name.Name)
		}
		field := fn.Type.Params.List[0]
		if len(field.Names) > 1 {
			return file{}, fmt.Errorf("%s: invalid test parameter for %s", rel, fn.Name.Name)
		}
		pointer, ok := field.Type.(*ast.StarExpr)
		if !ok {
			return file{}, fmt.Errorf("%s: invalid test parameter for %s", rel, fn.Name.Name)
		}
		selector, ok := pointer.X.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "T" {
			return file{}, fmt.Errorf("%s: invalid testing.T parameter for %s", rel, fn.Name.Name)
		}
		alias, ok := selector.X.(*ast.Ident)
		if !ok {
			return file{}, fmt.Errorf("%s: invalid testing import for %s", rel, fn.Name.Name)
		}
		valid := false
		for _, imported := range f.Imports {
			if imported.Path.Value == `"testing"` && ((imported.Name == nil && alias.Name == "testing") || (imported.Name != nil && imported.Name.Name == alias.Name)) {
				valid = true
			}
		}
		if !valid {
			return file{}, fmt.Errorf("%s: test parameter does not refer to testing.T", rel)
		}
		out.Tests = append(out.Tests, fn.Name.Name)
	}
	sort.Strings(out.Tests)
	return out, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: tagcatalog ROOT")
		os.Exit(2)
	}
	root, err := filepath.Abs(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	files := []file{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "artifacts" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("test source is a symlink: %s", path)
		}
		f, err := inspect(root, path)
		if err != nil {
			return err
		}
		files = append(files, f)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	if err := json.NewEncoder(os.Stdout).Encode(files); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
