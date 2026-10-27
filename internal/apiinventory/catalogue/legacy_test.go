package catalogue_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

// legacyFiles builds global documents for the fixture tree in the old format,
// with the old scope strings, from the per-package files of the tree.
func legacyFiles(t *testing.T, root string) (declarations, members, receipt []byte) {
	t.Helper()
	surfaces := map[string]catalogue.Surface{}
	for _, pkg := range []string{"alpha", "alpha/sub", "beta"} {
		var surface catalogue.Surface
		decode(t, root, pkg, catalogue.DeclarationsFile, &surface.Declarations)
		decode(t, root, pkg, catalogue.MembersFile, &surface.Members)
		surfaces[pkg] = surface
	}
	declarations, members, err := catalogue.AssembleLegacy(surfaces)
	if err != nil {
		t.Fatal(err)
	}
	receipt = []byte(`{
  "schema": "ceph-api-naming-review/v1",
  "declarations_sha256": "x",
  "members_sha256": "y",
  "declarations": {"alpha": 4, "alpha/sub": 1, "beta": 2},
  "members": {"alpha": 2, "alpha/sub": 0, "beta": 1},
  "review": "api-naming-audit.md"
}
`)
	return declarations, members, receipt
}

func decode(t *testing.T, root, pkg, name string, into any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(catalogue.PackageDir(root, pkg), name))
	if err != nil {
		t.Fatal(err)
	}
	if err := catalogue.DecodeStrict(data, into); err != nil {
		t.Fatal(err)
	}
}

func TestLegacySplitAndAssembleRoundTripByteForByte(t *testing.T) {
	root, _ := tree(t)
	declarations, members, receipt := legacyFiles(t, root)
	if err := catalogue.VerifyLegacy(root, declarations, members, receipt); err != nil {
		t.Fatalf("the assembled catalogues must reproduce the global files: %v", err)
	}
	surfaces, err := catalogue.SplitLegacy(declarations, members)
	if err != nil {
		t.Fatal(err)
	}
	again, againMembers, err := catalogue.AssembleLegacy(surfaces)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(declarations) || string(againMembers) != string(members) {
		t.Fatal("split then assemble changed bytes")
	}
	// The split rows are exactly the rows the per-package files hold.
	for pkg, surface := range surfaces {
		var disk catalogue.Surface
		decode(t, root, pkg, catalogue.DeclarationsFile, &disk.Declarations)
		decode(t, root, pkg, catalogue.MembersFile, &disk.Members)
		got, _ := catalogue.Marshal(surface)
		want, _ := catalogue.Marshal(disk)
		if string(got) != string(want) {
			t.Fatalf("%s: split differs from the per-package files", pkg)
		}
	}
}

func TestLegacyVerificationCatchesAnyRowChange(t *testing.T) {
	root, _ := tree(t)
	declarations, members, receipt := legacyFiles(t, root)
	mutations := map[string]func(root string){
		"changed row": func(root string) {
			replace(t, root, "docs/design/api-naming/alpha/declarations.json", `"proposed": "alpha.New"`, `"proposed": "alpha.Make"`)
		},
		"changed member": func(root string) {
			replace(t, root, "docs/design/api-naming/alpha/members.json", "Widget.Name field string", "Widget.Name field int")
		},
		"renamed row": func(root string) {
			replace(t, root, "docs/design/api-naming/beta/declarations.json", `"current": "beta.Run"`, `"current": "beta.Run2"`)
		},
		"count only": func(root string) {
			replace(t, root, "docs/design/api-naming/beta/declarations.json", `"count": 2`, `"count": 3`)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			copyTree(t, root, dir)
			mutate(dir)
			if err := catalogue.VerifyLegacy(dir, declarations, members, receipt); err == nil {
				t.Fatal("a change passed the equality proof")
			}
		})
	}
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		write(t, to, filepath.ToSlash(rel), string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLegacyVerificationChecksTheOldReceiptCounts(t *testing.T) {
	root, _ := tree(t)
	declarations, members, receipt := legacyFiles(t, root)
	bad := strings.Replace(string(receipt), `"beta": 2`, `"beta": 3`, 1)
	if err := catalogue.VerifyLegacy(root, declarations, members, []byte(bad)); err == nil {
		t.Fatal("a count that differs from the previous receipt passed")
	}
	extra := strings.Replace(string(receipt), `"beta": 2`, `"beta": 2, "ghost": 1`, 1)
	if err := catalogue.VerifyLegacy(root, declarations, members, []byte(extra)); err == nil {
		t.Fatal("a package missing from the new layout passed")
	}
}

func TestLegacySplitRefusesInconsistentInput(t *testing.T) {
	root, _ := tree(t)
	declarations, members, _ := legacyFiles(t, root)
	for name, pair := range map[string][2]string{
		"unowned row":    {strings.Replace(string(declarations), `"current": "beta.Run"`, `"current": "zeta.Run"`, 1), string(members)},
		"count mismatch": {strings.Replace(string(declarations), `"beta": 2`, `"beta": 5`, 1), string(members)},
		"member count":   {string(declarations), strings.Replace(string(members), `"beta": 1`, `"beta": 4`, 1)},
		"unknown schema": {strings.Replace(string(declarations), "naming/v1", "naming/v9", 1), string(members)},
	} {
		t.Run(name, func(t *testing.T) {
			if pair[0] == string(declarations) && pair[1] == string(members) {
				t.Fatal("the mutation changed nothing")
			}
			if _, err := catalogue.SplitLegacy([]byte(pair[0]), []byte(pair[1])); err == nil {
				t.Fatal("inconsistent global catalogue was split")
			}
		})
	}
}

func TestDecodeStrictRequiresEndOfInput(t *testing.T) {
	type doc struct {
		A int `json:"a"`
	}
	for _, good := range []string{`{"a":1}`, " \n{\"a\":1}\n\t "} {
		var d doc
		if err := catalogue.DecodeStrict([]byte(good), &d); err != nil || d.A != 1 {
			t.Fatalf("%q: %v %+v", good, err, d)
		}
	}
	for _, bad := range []string{`{"a":1}]`, `{"a":1}}`, `{"a":1} x`, `{"a":1}{"a":2}`, `{"a":1},`} {
		var d doc
		if err := catalogue.DecodeStrict([]byte(bad), &d); err == nil {
			t.Fatalf("%q was accepted", bad)
		}
	}
}
