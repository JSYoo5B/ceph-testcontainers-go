package apiinventory_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

type namingMembers struct {
	Schema string              `json:"schema"`
	Scope  string              `json:"scope"`
	Counts map[string]int      `json:"counts"`
	Types  map[string][]string `json:"types"`
}

func namingSyntax(t *testing.T, node ast.Node) string {
	t.Helper()
	var buf bytes.Buffer
	if err := format.Node(&buf, token.NewFileSet(), node); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// Record declared members and embedding boundaries, not the method sets of
// external dependencies. Nested anonymous records remain visible at their path.
func collectNamingMembers(t *testing.T, expr ast.Expr, prefix string, result *[]string) {
	t.Helper()
	var fields *ast.FieldList
	kind := "field"
	switch expr := expr.(type) {
	case *ast.StructType:
		fields = expr.Fields
	case *ast.InterfaceType:
		fields = expr.Methods
		kind = "method"
	case *ast.StarExpr:
		collectNamingMembers(t, expr.X, prefix, result)
		return
	case *ast.ArrayType:
		collectNamingMembers(t, expr.Elt, prefix+"[]", result)
		return
	case *ast.MapType:
		collectNamingMembers(t, expr.Value, prefix+"[]", result)
		return
	case *ast.ParenExpr:
		collectNamingMembers(t, expr.X, prefix, result)
		return
	default:
		return
	}
	for _, field := range fields.List {
		syntax := namingSyntax(t, field.Type)
		if len(field.Names) == 0 {
			*result = append(*result, prefix+" embed "+syntax)
			continue
		}
		for _, name := range field.Names {
			if !name.IsExported() {
				continue
			}
			path := prefix + "." + name.Name
			entry := path + " " + kind + " " + syntax
			if field.Tag != nil {
				entry += " " + field.Tag.Value
			}
			*result = append(*result, entry)
			collectNamingMembers(t, field.Type, path, result)
		}
	}
}

func namingMembersInFile(t *testing.T, pkg string, file *ast.File) map[string][]string {
	t.Helper()
	result := map[string][]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			typ, ok := spec.(*ast.TypeSpec)
			if !ok || !typ.Name.IsExported() {
				continue
			}
			var members []string
			collectNamingMembers(t, typ.Type, typ.Name.Name, &members)
			if len(members) == 0 {
				continue
			}
			slices.Sort(members)
			result[pkg+"."+typ.Name.Name] = members
		}
	}
	return result
}

func namingMembersFromSource(t *testing.T) namingMembers {
	t.Helper()
	inventory := namingMembers{
		Schema: "ceph-api-naming-members/v1",
		Scope:  "Declared exported struct fields and interface methods, nested anonymous records and explicit embeddings in public types; dependency and promoted method sets are not expanded",
		Counts: map[string]int{}, Types: map[string][]string{},
	}
	for _, pkg := range slices.Sorted(maps.Keys(packages)) {
		inventory.Counts[pkg] = 0
		parseDir(t, pkg, func(file *ast.File) {
			for typ, members := range namingMembersInFile(t, pkg, file) {
				if prior, exists := inventory.Types[typ]; exists {
					if !slices.Equal(prior, members) {
						t.Fatalf("conflicting public member declarations: %s", typ)
					}
					continue
				}
				inventory.Types[typ] = members
				inventory.Counts[pkg] += len(members)
			}
		})
	}
	return inventory
}

func namingJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func TestPublicNamingMembers(t *testing.T) {
	data := namingJSON(t, namingMembersFromSource(t))
	path := filepath.Join(root, "docs/design/api-naming-members.json")
	if *updateNaming {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, data) {
		t.Fatal("public member inventory is stale; inspect the members, then run go test ./internal/apiinventory -run '^TestPublicNaming(Inventory|Members)$' -update-api-naming")
	}
}

func TestNamingMemberScope(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", `package synthetic
 type Record struct {
  A, B int
  Hidden struct { UUID string; private int }
  Rows []struct { Name string }
  Index map[string]struct { ID int }
  *external.Container
  privateEmbedding
  private struct { Exposed string }
 }
 type Endpoint interface { Read([]byte) (int, error); external.Closer }
 type private struct { Public string }
 `, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := namingMembersInFile(t, "synthetic", file)
	want := map[string][]string{
		"synthetic.Record": {
			"Record.A field int", "Record.B field int",
			"Record.Hidden field struct {\n\tUUID    string\n\tprivate int\n}", "Record.Hidden.UUID field string",
			"Record.Index field map[string]struct{ ID int }", "Record.Index[].ID field int",
			"Record.Rows field []struct{ Name string }", "Record.Rows[].Name field string",
			"Record embed *external.Container", "Record embed privateEmbedding",
		},
		"synthetic.Endpoint": {"Endpoint.Read method func([]byte) (int, error)", "Endpoint embed external.Closer"},
	}
	for _, entries := range want {
		slices.Sort(entries)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("member scope differs:\ngot: %#v\nwant: %#v", got, want)
	}
}

// Refreshing generated catalogues must never silently approve new names.
// This separately edited receipt pins the reviewed declaration/member snapshots.
func TestNamingReviewBindsCurrentSurface(t *testing.T) {
	var receipt struct {
		Schema             string         `json:"schema"`
		DeclarationsSHA256 string         `json:"declarations_sha256"`
		MembersSHA256      string         `json:"members_sha256"`
		Declarations       map[string]int `json:"declarations"`
		Members            map[string]int `json:"members"`
		Review             string         `json:"review"`
	}
	data, err := os.ReadFile(filepath.Join(root, "docs/design/api-naming-review.json"))
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	decls, members := namingInventoryFromSource(t), namingMembersFromSource(t)
	if receipt.Schema != "ceph-api-naming-review/v1" || receipt.Review != "api-naming-audit.md" {
		t.Fatal("unknown naming review receipt")
	}
	for _, entry := range decls.Entries {
		if entry.Decision == "proposal" {
			t.Fatal("review cannot finish with a pending proposal", entry.Current)
		}
	}
	if receipt.DeclarationsSHA256 != fmt.Sprintf("%x", sha256.Sum256(namingJSON(t, decls))) || receipt.MembersSHA256 != fmt.Sprintf("%x", sha256.Sum256(namingJSON(t, members))) || !maps.Equal(receipt.Declarations, decls.Counts) || !maps.Equal(receipt.Members, members.Counts) {
		t.Fatal("public naming surface changed since the recorded audit; review the change and explicitly update its receipt, not only the generated inventories")
	}
	if _, err := os.Stat(filepath.Join(root, "docs/design", receipt.Review)); err != nil {
		t.Fatal(err)
	}
}
