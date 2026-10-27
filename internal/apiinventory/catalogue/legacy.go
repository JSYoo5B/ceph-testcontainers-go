package catalogue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
)

// This file holds the one-time migration from the global catalogues
// (api-naming-inventory.json, api-naming-members.json, api-naming-review.json)
// to the package-scoped files. It can be deleted together with the
// `migrate` commands of internal/tools/apinaming once the migration change has
// merged. It never issues a receipt: the receipts are issued by Approve.

const (
	legacyDeclarationsSchema = "ceph-api-naming/v1"
	legacyDeclarationsScope  = "Declared exports in ceph, fault, rbd, cephfs, rgw, rgw/multisite and capture; excludes struct fields, interface members, promoted/inherited methods and dependency exports"
	legacyMembersSchema      = "ceph-api-naming-members/v1"
	legacyMembersScope       = "Declared exported struct fields and interface methods, nested anonymous records and explicit embeddings in public types; dependency and promoted method sets are not expanded"
	legacyReceiptSchema      = "ceph-api-naming-review/v1"
)

type legacyDeclarations struct {
	Schema  string         `json:"schema"`
	Scope   string         `json:"scope"`
	Counts  map[string]int `json:"counts"`
	Entries []Entry        `json:"entries"`
}

type legacyMembers struct {
	Schema string              `json:"schema"`
	Scope  string              `json:"scope"`
	Counts map[string]int      `json:"counts"`
	Types  map[string][]string `json:"types"`
}

// LegacyReceipt is the old global receipt.
type LegacyReceipt struct {
	Schema             string         `json:"schema"`
	DeclarationsSHA256 string         `json:"declarations_sha256"`
	MembersSHA256      string         `json:"members_sha256"`
	Declarations       map[string]int `json:"declarations"`
	Members            map[string]int `json:"members"`
	Review             string         `json:"review"`
}

// DecodeStrict decodes one JSON document, refusing unknown fields and trailing data.
func DecodeStrict(data []byte, into any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return err
	}
	return requireEOF(dec)
}

// requireEOF refuses anything after the first JSON value. Decoder.More is not
// an end-of-input check: a stray closing delimiter makes it report false.
// Decoding again must return io.EOF exactly, which rejects a second value and
// any trailing syntax error alike. Whitespace after the value is accepted.
func requireEOF(dec *json.Decoder) error {
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after the JSON value")
	}
	return nil
}

// owner returns the package that owns a package-qualified name, using the
// package list of the legacy catalogue.
func owner(packages []string, name string) (string, error) {
	for _, pkg := range packages {
		if strings.HasPrefix(name, pkg+".") {
			return pkg, nil
		}
	}
	return "", fmt.Errorf("no package owns %q", name)
}

// SplitLegacy divides the global catalogues into per-package catalogues
// without changing a row. It refuses a catalogue whose counts disagree with
// its rows, an unknown schema, and a name that no package owns.
func SplitLegacy(declarationsJSON, membersJSON []byte) (map[string]Surface, error) {
	var declarations legacyDeclarations
	var members legacyMembers
	if err := DecodeStrict(declarationsJSON, &declarations); err != nil {
		return nil, fmt.Errorf("global declarations: %w", err)
	}
	if err := DecodeStrict(membersJSON, &members); err != nil {
		return nil, fmt.Errorf("global members: %w", err)
	}
	if declarations.Schema != legacyDeclarationsSchema || members.Schema != legacyMembersSchema {
		return nil, errors.New("unknown global catalogue schema")
	}
	packages := slices.Sorted(maps.Keys(declarations.Counts))
	if !slices.Equal(packages, slices.Sorted(maps.Keys(members.Counts))) {
		return nil, errors.New("global declaration and member catalogues list different packages")
	}
	surfaces := map[string]Surface{}
	for _, pkg := range packages {
		surfaces[pkg] = Surface{
			Declarations: Declarations{Schema: DeclarationsSchema, Scope: declarationsScope, Package: pkg, Count: declarations.Counts[pkg], Entries: []Entry{}},
			Members:      Members{Schema: MembersSchema, Scope: membersScope, Package: pkg, Count: members.Counts[pkg], Types: map[string][]string{}},
		}
	}
	for _, entry := range declarations.Entries {
		pkg, err := owner(packages, entry.Current)
		if err != nil {
			return nil, err
		}
		surface := surfaces[pkg]
		surface.Declarations.Entries = append(surface.Declarations.Entries, entry)
		surfaces[pkg] = surface
	}
	for typ, rows := range members.Types {
		pkg, err := owner(packages, typ)
		if err != nil {
			return nil, err
		}
		surfaces[pkg].Members.Types[typ] = rows
	}
	for pkg, surface := range surfaces {
		if len(surface.Declarations.Entries) != surface.Declarations.Count {
			return nil, fmt.Errorf("global declaration count of %s is %d for %d rows", pkg, surface.Declarations.Count, len(surface.Declarations.Entries))
		}
		rows := 0
		for _, typ := range surface.Members.Types {
			rows += len(typ)
		}
		if rows != surface.Members.Count {
			return nil, fmt.Errorf("global member count of %s is %d for %d rows", pkg, surface.Members.Count, rows)
		}
	}
	return surfaces, nil
}

// AssembleLegacy rebuilds the two global documents from per-package
// catalogues, entries in package order. The migration check compares the
// result with the previously approved global files byte for byte.
func AssembleLegacy(surfaces map[string]Surface) (declarationsJSON, membersJSON []byte, err error) {
	declarations := legacyDeclarations{Schema: legacyDeclarationsSchema, Scope: legacyDeclarationsScope, Counts: map[string]int{}, Entries: []Entry{}}
	members := legacyMembers{Schema: legacyMembersSchema, Scope: legacyMembersScope, Counts: map[string]int{}, Types: map[string][]string{}}
	for _, pkg := range slices.Sorted(maps.Keys(surfaces)) {
		surface := surfaces[pkg]
		declarations.Counts[pkg] = surface.Declarations.Count
		declarations.Entries = append(declarations.Entries, surface.Declarations.Entries...)
		members.Counts[pkg] = surface.Members.Count
		for typ, rows := range surface.Members.Types {
			if _, exists := members.Types[typ]; exists {
				return nil, nil, fmt.Errorf("type %s appears in two packages", typ)
			}
			members.Types[typ] = rows
		}
	}
	if declarationsJSON, err = Marshal(declarations); err != nil {
		return nil, nil, err
	}
	membersJSON, err = Marshal(members)
	return declarationsJSON, membersJSON, err
}

// VerifyLegacy proves that the per-package catalogues on disk reproduce the
// previously approved global files: the assembled documents equal them byte
// for byte, and the old receipt's per-package counts equal the new counts. A
// changed, dropped or added row fails. It reads the previous files from the
// arguments, so the caller decides where they come from (a Git revision).
func VerifyLegacy(root string, declarationsJSON, membersJSON, receiptJSON []byte) error {
	var receipt LegacyReceipt
	if err := DecodeStrict(receiptJSON, &receipt); err != nil {
		return fmt.Errorf("global receipt: %w", err)
	}
	if receipt.Schema != legacyReceiptSchema {
		return errors.New("unknown global receipt schema")
	}
	directories, err := Directories(root)
	if err != nil {
		return err
	}
	surfaces := map[string]Surface{}
	for _, pkg := range directories {
		var surface Surface
		if err := readStrict(PackageDir(root, pkg)+"/"+DeclarationsFile, &surface.Declarations); err != nil {
			return err
		}
		if err := readStrict(PackageDir(root, pkg)+"/"+MembersFile, &surface.Members); err != nil {
			return err
		}
		surfaces[pkg] = surface
	}
	var problems []error
	declarations, members, err := AssembleLegacy(surfaces)
	if err != nil {
		return err
	}
	if !bytes.Equal(declarations, declarationsJSON) {
		problems = append(problems, errors.New("assembled declarations differ from the previous global declaration catalogue"))
	}
	if !bytes.Equal(members, membersJSON) {
		problems = append(problems, errors.New("assembled members differ from the previous global member catalogue"))
	}
	for pkg, surface := range surfaces {
		if receipt.Declarations[pkg] != surface.Declarations.Count || receipt.Members[pkg] != surface.Members.Count {
			problems = append(problems, fmt.Errorf("%s counts differ from the previous receipt", pkg))
		}
	}
	if len(receipt.Declarations) != len(surfaces) || len(receipt.Members) != len(surfaces) {
		problems = append(problems, errors.New("the previous receipt lists a different set of packages"))
	}
	return errors.Join(problems...)
}
