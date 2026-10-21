package apiinventory_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"go/ast"
	"go/format"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

var updateNaming = flag.Bool("update-api-naming", false, "refresh the source-derived public naming inventory")

// These are review proposals, not compatibility aliases or an API rewrite.
// Only inventory rows below change when a proposal is edited.
var namingProposals = map[string]string{
	"ceph.Container.TemporaryConfig":               "ceph.Container.OverrideConfig",
	"ceph.Container.TemporaryOSDFlag":              "ceph.Container.OverrideOSDFlag",
	"ceph.Container.TemporaryMGRModule":            "ceph.Container.OverrideMGRModule",
	"ceph.Container.TemporaryMessengerPolicy":      "ceph.Container.OverrideMessengerPolicy",
	"ceph.Container.TemporaryClientRevocation":     "ceph.Container.RevokeClient",
	"cephfs.Filesystem.TemporarySessionTimeouts":   "cephfs.Filesystem.OverrideSessionTimeouts",
	"cephfs.Filesystem.TemporarySubvolumePin":      "cephfs.Filesystem.OverrideSubvolumePin",
	"cephfs.Filesystem.TemporarySubvolumeGroupPin": "cephfs.Filesystem.OverrideSubvolumeGroupPin",
	"cephfs.Filesystem.TemporarySnapshotSchedule":  "cephfs.Filesystem.AddSnapshotSchedule",
	"rbd.TemporaryImmutableObjectCache":            "rbd.StartImmutableObjectCache",
	"fault.TemporaryNetworkDelay":                  "fault.ApplyNetworkProfile",
	"fault.TemporaryLinkDelay":                     "fault.ApplyLinkProfile",
	"fault.NetworkDelay":                           "fault.NetworkProfile",
	"fault.NetworkDelayLease":                      "fault.NetworkProfileLease",
	"fault.NetworkDelayStatus":                     "fault.NetworkProfileStatus",
	"rgw.Multisite.TemporaryZoneLinkDelay":         "rgw.Multisite.ApplyZoneLinkProfile",
}

type namingEntry struct {
	Current   string `json:"current"`
	Kind      string `json:"kind"`
	Signature string `json:"signature,omitempty"`
	Receiver  string `json:"receiver,omitempty"`
	Proposed  string `json:"proposed"`
	Decision  string `json:"decision"`
}

type namingInventory struct {
	Schema  string         `json:"schema"`
	Scope   string         `json:"scope"`
	Counts  map[string]int `json:"counts"`
	Entries []namingEntry  `json:"entries"`
}

func namingInventoryFromSource(t *testing.T) namingInventory {
	t.Helper()
	inventory := namingInventory{
		Schema: "ceph-api-naming/v1",
		Scope:  "Declared exports in ceph, fault, rbd, cephfs and rgw; excludes struct fields, interface members, promoted/inherited methods and dependency exports",
		Counts: make(map[string]int),
	}
	seen := make(map[string]bool)
	for _, pkg := range slices.Sorted(maps.Keys(packages)) {
		var entries []namingEntry
		parseDir(t, pkg, func(file *ast.File) {
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
						var receiverSyntax bytes.Buffer
						if err := format.Node(&receiverSyntax, token.NewFileSet(), decl.Recv.List[0].Type); err != nil {
							t.Fatal(err)
						}
						receiverType = receiverSyntax.String()
					}
					var signature bytes.Buffer
					if err := format.Node(&signature, token.NewFileSet(), decl.Type); err != nil {
						t.Fatal(err)
					}
					entries = append(entries, namingEntry{Current: pkg + "." + name, Kind: kind, Signature: signature.String(), Receiver: receiverType})
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						switch spec := spec.(type) {
						case *ast.TypeSpec:
							if spec.Name.IsExported() {
								entries = append(entries, namingEntry{Current: pkg + "." + spec.Name.Name, Kind: "type"})
							}
						case *ast.ValueSpec:
							for _, name := range spec.Names {
								if name.IsExported() {
									entries = append(entries, namingEntry{Current: pkg + "." + name.Name, Kind: decl.Tok.String()})
								}
							}
						}
					}
				}
			}
		})
		slices.SortFunc(entries, func(a, b namingEntry) int { return strings.Compare(a.Current, b.Current) })
		// Build-tag alternatives can declare the same public name. Require identical
		// signatures instead of silently dropping a conflicting declaration.
		for _, entry := range entries {
			if seen[entry.Current] {
				prior := inventory.Entries[len(inventory.Entries)-1]
				if prior.Current != entry.Current || prior.Kind != entry.Kind || prior.Signature != entry.Signature || prior.Receiver != entry.Receiver {
					t.Fatalf("conflicting API declarations: %s", entry.Current)
				}
				continue
			}
			seen[entry.Current] = true
			entry.Proposed, entry.Decision = entry.Current, "baseline"
			if proposal, ok := namingProposals[entry.Current]; ok {
				entry.Proposed, entry.Decision = proposal, "proposal"
			} else if split := strings.LastIndex(entry.Current, "."); split >= 0 {
				if receiver, ok := namingProposals[entry.Current[:split]]; ok {
					entry.Proposed, entry.Decision = receiver+entry.Current[split:], "proposal"
				}
			}
			inventory.Counts[pkg]++
			inventory.Entries = append(inventory.Entries, entry)
		}
		// Keep this audit tied to the same public surface as the reference-page check.
		if inventory.Counts[pkg] != len(exported(t, pkg)) {
			t.Fatalf("naming inventory omitted declarations in %s", pkg)
		}
	}
	targets := make(map[string]string)
	for _, entry := range inventory.Entries {
		if prior := targets[entry.Proposed]; prior != "" {
			t.Fatalf("name collision: %s and %s -> %s", prior, entry.Current, entry.Proposed)
		}
		targets[entry.Proposed] = entry.Current
	}
	for current := range namingProposals {
		if !seen[current] {
			t.Fatalf("stale rename proposal: %s", current)
		}
	}
	return inventory
}

// This is a review baseline, not a claim that every baseline name was approved.
// Missing/stale rows, signature drift, duplicate names and colliding proposals
// fail the normal suite; refreshing requires an explicit opt-in flag.
func TestPublicNamingInventory(t *testing.T) {
	inventory := namingInventoryFromSource(t)
	data, err := json.MarshalIndent(inventory, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	path := filepath.Join(root, "docs/design/api-naming-inventory.json")
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
		t.Fatal("public naming inventory is stale; inspect API/proposal changes, then run go test ./internal/apiinventory -run '^TestPublicNamingInventory$' -update-api-naming")
	}
}
