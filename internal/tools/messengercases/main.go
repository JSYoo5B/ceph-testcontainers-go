// Command messengercases indexes top-level integration test declarations and
// their literal log markers. It parses source without importing or running it.
//
// It is the source-registration step of .github/scripts/messenger_evidence.py.
// A binding's test is registered only when its source file holds exactly one
// unambiguous func TestX(t *testing.T) definition with a literal parent-marker
// log call in its body. Comments, unused string literals, wrong signatures and
// duplicate definitions cannot register a test; duplicate names are rejected
// even across build-tag alternatives. Indexing proves neither reachability nor
// execution.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

type testCase struct {
	Source  string   `json:"source"`
	Name    string   `json:"name"`
	Markers []string `json:"markers"`
}

func index(root string) ([]testCase, error) {
	dir := filepath.Join(root, "internal", "integration")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := []testCase{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("integration source is a symlink: %s", entry.Name())
		}
		path := filepath.Join(dir, entry.Name())
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return nil, err
		}
		if f.Name.Name != "integration_test" {
			continue
		}
		aliases := map[string]bool{}
		for _, imp := range f.Imports {
			if imp.Path.Value != `"testing"` {
				continue
			}
			name := "testing"
			if imp.Name != nil {
				name = imp.Name.Name
			}
			if name != "_" && name != "." {
				aliases[name] = true
			}
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !isTest(fn.Name.Name) {
				continue
			}
			if !testSignature(fn, aliases) {
				return nil, fmt.Errorf("invalid test signature: %s/%s", entry.Name(), fn.Name.Name)
			}
			markers := []string{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Logf" && selector.Sel.Name != "Log" {
					return true
				}
				literal, ok := call.Args[0].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				text, err := strconv.Unquote(literal.Value)
				if err == nil {
					words := strings.Fields(text)
					if len(words) != 0 {
						markers = append(markers, words[0])
					}
				}
				return true
			})
			slices.Sort(markers)
			result = append(result, testCase{Source: "internal/integration/" + entry.Name(), Name: fn.Name.Name, Markers: slices.Compact(markers)})
		}
	}
	return result, nil
}

func isTest(name string) bool {
	if !strings.HasPrefix(name, "Test") || name == "TestMain" {
		return false
	}
	for _, r := range name[len("Test"):] {
		return !unicode.IsLower(r)
	}
	return true
}

func testSignature(fn *ast.FuncDecl, aliases map[string]bool) bool {
	if fn.Type.TypeParams != nil || fn.Type.Results != nil || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 || fn.Body == nil {
		return false
	}
	param := fn.Type.Params.List[0]
	if len(param.Names) > 1 {
		return false
	}
	ptr, ok := param.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := ptr.X.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "T" {
		return false
	}
	alias, ok := sel.X.(*ast.Ident)
	return ok && aliases[alias.Name]
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: messengercases repository-root")
		os.Exit(2)
	}
	result, err := index(os.Args[1])
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(result)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
