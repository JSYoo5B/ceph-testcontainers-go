package catalogue

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"maps"
	"slices"
	"strings"
)

// Schema names and scopes. A schema changes only with the file format.
const (
	DeclarationsSchema = "ceph-api-naming/v2"
	MembersSchema      = "ceph-api-naming-members/v2"
	ReceiptSchema      = "ceph-api-naming-review/v2"

	declarationsScope = "Declared exports of one package; excludes struct fields, interface members, promoted/inherited methods and dependency exports"
	membersScope      = "Declared exported struct fields and interface methods, nested anonymous records and explicit embeddings in the public types of one package; dependency and promoted method sets are not expanded"
)

// Config selects the module root and the reviewed rename proposals. Proposals
// are review inputs, not compatibility aliases or an API rewrite.
type Config struct {
	Root      string
	Proposals map[string]string
}

// Entry is one declared public name.
type Entry struct {
	Current   string `json:"current"`
	Kind      string `json:"kind"`
	Signature string `json:"signature,omitempty"`
	Receiver  string `json:"receiver,omitempty"`
	Proposed  string `json:"proposed"`
	Decision  string `json:"decision"`
}

// Declarations is the declaration catalogue of one package.
type Declarations struct {
	Schema  string  `json:"schema"`
	Scope   string  `json:"scope"`
	Package string  `json:"package"`
	Count   int     `json:"count"`
	Entries []Entry `json:"entries"`
}

// Members is the member catalogue of one package.
type Members struct {
	Schema  string              `json:"schema"`
	Scope   string              `json:"scope"`
	Package string              `json:"package"`
	Count   int                 `json:"count"`
	Types   map[string][]string `json:"types"`
}

// Surface is the source-derived catalogue of one package.
type Surface struct {
	Declarations Declarations
	Members      Members
}

// Marshal serializes a catalogue or receipt. The bytes are the file content
// and the input of the receipt hashes.
func Marshal(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func declarationsFromSource(c Config, pkg string) ([]Entry, error) {
	var entries []Entry
	err := parseDir(c.Root, pkg, func(file *ast.File) error {
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if !decl.Name.IsExported() {
					continue
				}
				name, kind, receiverType := decl.Name.Name, "function", ""
				if decl.Recv != nil {
					receiver := receiverName(decl.Recv.List[0].Type)
					if !ast.IsExported(receiver) {
						continue
					}
					name, kind = receiver+"."+name, "method"
					var err error
					if receiverType, err = syntax(decl.Recv.List[0].Type); err != nil {
						return err
					}
				}
				signature, err := syntax(decl.Type)
				if err != nil {
					return err
				}
				entries = append(entries, Entry{Current: pkg + "." + name, Kind: kind, Signature: signature, Receiver: receiverType})
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if spec.Name.IsExported() {
							entries = append(entries, Entry{Current: pkg + "." + spec.Name.Name, Kind: "type"})
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if name.IsExported() {
								entries = append(entries, Entry{Current: pkg + "." + name.Name, Kind: decl.Tok.String()})
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(a.Current, b.Current) })
	return entries, nil
}

// Build-tag alternatives can declare the same public name. Require identical
// declarations instead of silently dropping a conflicting one.
func uniqueEntries(c Config, raw []Entry, seen map[string]bool) ([]Entry, error) {
	var entries []Entry
	for _, entry := range raw {
		if seen[entry.Current] {
			prior := entries[len(entries)-1]
			if prior.Current != entry.Current || prior.Kind != entry.Kind || prior.Signature != entry.Signature || prior.Receiver != entry.Receiver {
				return nil, fmt.Errorf("conflicting API declarations: %s", entry.Current)
			}
			continue
		}
		seen[entry.Current] = true
		entry.Proposed, entry.Decision = entry.Current, "baseline"
		if proposal, ok := c.Proposals[entry.Current]; ok {
			entry.Proposed, entry.Decision = proposal, "proposal"
		} else if split := strings.LastIndex(entry.Current, "."); split >= 0 {
			if receiver, ok := c.Proposals[entry.Current[:split]]; ok {
				entry.Proposed, entry.Decision = receiver+entry.Current[split:], "proposal"
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Record declared members and embedding boundaries, not the method sets of
// external dependencies. Nested anonymous records remain visible at their path.
func collectMembers(expr ast.Expr, prefix string, result *[]string) error {
	var fields *ast.FieldList
	kind := "field"
	switch expr := expr.(type) {
	case *ast.StructType:
		fields = expr.Fields
	case *ast.InterfaceType:
		fields = expr.Methods
		kind = "method"
	case *ast.StarExpr:
		return collectMembers(expr.X, prefix, result)
	case *ast.ArrayType:
		return collectMembers(expr.Elt, prefix+"[]", result)
	case *ast.MapType:
		return collectMembers(expr.Value, prefix+"[]", result)
	case *ast.ParenExpr:
		return collectMembers(expr.X, prefix, result)
	default:
		return nil
	}
	for _, field := range fields.List {
		text, err := syntax(field.Type)
		if err != nil {
			return err
		}
		if len(field.Names) == 0 {
			*result = append(*result, prefix+" embed "+text)
			continue
		}
		for _, name := range field.Names {
			if !name.IsExported() {
				continue
			}
			path := prefix + "." + name.Name
			entry := path + " " + kind + " " + text
			if field.Tag != nil {
				entry += " " + field.Tag.Value
			}
			*result = append(*result, entry)
			if err := collectMembers(field.Type, path, result); err != nil {
				return err
			}
		}
	}
	return nil
}

// MembersInFile returns the member rows of the exported types of one file,
// keyed by the package-qualified type name. A type without members maps to an
// empty slice so that alternatives with and without members can conflict.
func MembersInFile(pkg string, file *ast.File) (map[string][]string, error) {
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
			if err := collectMembers(typ.Type, typ.Name.Name, &members); err != nil {
				return nil, err
			}
			// An empty member set is kept: build-tag alternatives are compared
			// by it, and the catalogue writer omits it.
			slices.Sort(members)
			result[pkg+"."+typ.Name.Name] = members
		}
	}
	return result, nil
}

func membersFromSource(c Config, pkg string) (Members, error) {
	members := Members{Schema: MembersSchema, Scope: membersScope, Package: pkg, Types: map[string][]string{}}
	// seen keeps empty member sets too; the catalogue lists only nonempty ones.
	seen := map[string][]string{}
	err := parseDir(c.Root, pkg, func(file *ast.File) error {
		found, err := MembersInFile(pkg, file)
		if err != nil {
			return err
		}
		for typ, rows := range found {
			if prior, exists := seen[typ]; exists {
				if !slices.Equal(prior, rows) {
					return fmt.Errorf("conflicting public member declarations: %s", typ)
				}
				continue
			}
			seen[typ] = rows
			if len(rows) == 0 {
				continue
			}
			members.Types[typ] = rows
			members.Count += len(rows)
		}
		return nil
	})
	return members, err
}

// Generate derives the catalogue of every named package from source. Pass the
// whole discovered package list: the name-collision and stale-proposal checks
// are global, so a subset would report proposals of other packages as stale.
// It checks only; it writes nothing and approves nothing.
func Generate(c Config, packages []string) (map[string]Surface, error) {
	surfaces := make(map[string]Surface, len(packages))
	seen := map[string]bool{}
	targets := map[string]string{}
	for _, pkg := range packages {
		raw, err := declarationsFromSource(c, pkg)
		if err != nil {
			return nil, err
		}
		entries, err := uniqueEntries(c, raw, seen)
		if err != nil {
			return nil, err
		}
		// Keep this audit tied to the same public surface as the reference-page check.
		names, err := ExportedNames(c.Root, pkg)
		if err != nil {
			return nil, err
		}
		if len(entries) != len(names) {
			return nil, fmt.Errorf("naming inventory omitted declarations in %s", pkg)
		}
		for _, entry := range entries {
			if prior := targets[entry.Proposed]; prior != "" {
				return nil, fmt.Errorf("name collision: %s and %s -> %s", prior, entry.Current, entry.Proposed)
			}
			targets[entry.Proposed] = entry.Current
		}
		members, err := membersFromSource(c, pkg)
		if err != nil {
			return nil, err
		}
		surfaces[pkg] = Surface{
			Declarations: Declarations{Schema: DeclarationsSchema, Scope: declarationsScope, Package: pkg, Count: len(entries), Entries: entries},
			Members:      members,
		}
	}
	for _, current := range slices.Sorted(maps.Keys(c.Proposals)) {
		if !seen[current] {
			return nil, fmt.Errorf("stale rename proposal: %s", current)
		}
	}
	return surfaces, nil
}
