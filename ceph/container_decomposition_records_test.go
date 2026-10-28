package ceph

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

// The Container decomposition records are one Markdown file per component under
// docs/design/container-decomposition/, discovered from the directory. No file
// lists them: adding, removing or renaming a record is a change to that record
// alone, and these guards find the records by reading the directory. The shared
// document docs/design/container-decomposition.md keeps the common principles,
// the measurements and the cross-component constraints.
const (
	decompositionDir       = "docs/design/container-decomposition"
	decompositionMainDoc   = "docs/design/container-decomposition.md"
	residualRecordName     = "residual-fields.md"
	decompositionRepoRoot  = ".."
	decompositionGuardFile = "ceph/container_decomposition_records_test.go"
)

var (
	// A record name is a lowercase stable component name. Anything else in the
	// directory (a README, a backup, a subdirectory) is refused, never skipped,
	// because a skipped file would silently leave the guards.
	decompositionRecordName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}\.md$`)

	// An ordinal position in a queue, in a file name or a heading. Component
	// records are named after the component.
	ordinalWord = `(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|eighteenth|nineteenth|twentieth|[0-9]+(?:st|nd|rd|th))`
	ordinalName = regexp.MustCompile(`(?:^|-)(?:` + ordinalWord + `|slice|slices)(?:-|\.md$)`)
	ordinalHead = regexp.MustCompile(`(?im)^#{1,6}[ \t]+` + ordinalWord + `\b[^\n]*\bslice\b`)

	// A citation of a record, in Go comments or in the shared document. The
	// name stops at ".md" so that a sentence period after it is not part of it.
	decompositionCitation = regexp.MustCompile(`container-decomposition/([a-z0-9][a-z0-9-]*\.md)`)
)

// decompositionRecordNames lists the records of the directory in fsys, sorted.
// It refuses a missing or empty directory and any entry that is not a regular
// file with a record name.
func decompositionRecordNames(fsys fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(fsys, decompositionDir)
	if err != nil {
		return nil, fmt.Errorf("the decomposition record directory is unreadable: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.Type().IsRegular() {
			return nil, fmt.Errorf("%s/%s is not a regular file", decompositionDir, e.Name())
		}
		if !decompositionRecordName.MatchString(e.Name()) {
			return nil, fmt.Errorf("%s/%s is not a record name (lowercase a-z, 0-9 and -, ending in .md)", decompositionDir, e.Name())
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%s holds no record", decompositionDir)
	}
	slices.Sort(names)
	return names, nil
}

// checkDecompositionRecords requires every discovered record to be named after
// its component (no ordinal) and to open with a level-one title.
func checkDecompositionRecords(fsys fs.FS) error {
	names, err := decompositionRecordNames(fsys)
	if err != nil {
		return err
	}
	for _, name := range names {
		if ordinalName.MatchString(name) {
			return fmt.Errorf("%s/%s is named by queue position, not by component", decompositionDir, name)
		}
		b, err := fs.ReadFile(fsys, path.Join(decompositionDir, name))
		if err != nil {
			return err
		}
		doc := string(b)
		if !strings.HasPrefix(doc, "# ") {
			return fmt.Errorf("%s/%s does not open with a level-one title", decompositionDir, name)
		}
		if loc := ordinalHead.FindString(doc); loc != "" {
			return fmt.Errorf("%s/%s has an ordinal slice heading %q", decompositionDir, name, loc)
		}
	}
	return nil
}

// missingCitedRecords returns the record names that the texts cite and that
// the directory does not hold, as "source: name" entries.
func missingCitedRecords(fsys fs.FS, texts map[string]string) ([]string, error) {
	names, err := decompositionRecordNames(fsys)
	if err != nil {
		return nil, err
	}
	var missing []string
	for source, text := range texts {
		for _, m := range decompositionCitation.FindAllStringSubmatch(text, -1) {
			if !slices.Contains(names, m[1]) {
				missing = append(missing, source+": "+m[1])
			}
		}
	}
	slices.Sort(missing)
	return slices.Compact(missing), nil
}

// decompositionCitingTexts returns the shared document and every Go file of
// package ceph, keyed by path, as the texts that may cite a record.
func decompositionCitingTexts(fsys fs.FS) (map[string]string, error) {
	texts := map[string]string{}
	main, err := fs.ReadFile(fsys, decompositionMainDoc)
	if err != nil {
		return nil, err
	}
	texts[decompositionMainDoc] = string(main)
	files, err := fs.Glob(fsys, "ceph/*.go")
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f == decompositionGuardFile {
			// Its fixtures cite invented records on purpose.
			continue
		}
		b, err := fs.ReadFile(fsys, f)
		if err != nil {
			return nil, err
		}
		texts[f] = string(b)
	}
	return texts, nil
}

// checkCitedRecordsExist fails when the shared document or a source file cites
// a record that is not in the directory (a removed or renamed record).
func checkCitedRecordsExist(fsys fs.FS) error {
	texts, err := decompositionCitingTexts(fsys)
	if err != nil {
		return err
	}
	missing, err := missingCitedRecords(fsys, texts)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("cited decomposition records are missing: %v", missing)
	}
	return nil
}

// residualRecordFields reads the field names of the residual-field record, the
// one record the field ledger test requires, from the directory in fsys. A
// missing record is an error, not an empty ledger.
func residualRecordFields(fsys fs.FS) ([]string, error) {
	b, err := fs.ReadFile(fsys, path.Join(decompositionDir, residualRecordName))
	if err != nil {
		return nil, fmt.Errorf("the residual-field record is required: %w", err)
	}
	names := residualDocFields(string(b))
	if len(names) == 0 {
		return nil, fmt.Errorf("%s/%s has no verdict rows", decompositionDir, residualRecordName)
	}
	return names, nil
}

func repoFS() fs.FS { return os.DirFS(decompositionRepoRoot) }

func TestDecompositionRecordsAreDiscoveredFromTheDirectory(t *testing.T) {
	if err := checkDecompositionRecords(repoFS()); err != nil {
		t.Fatal(err)
	}
	names, err := decompositionRecordNames(repoFS())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(names, residualRecordName) {
		t.Fatalf("records %v lack %s", names, residualRecordName)
	}
}

func TestDecompositionCitationsResolveToRecords(t *testing.T) {
	if err := checkCitedRecordsExist(repoFS()); err != nil {
		t.Fatal(err)
	}
}

// decompositionFixture is a small tree: the shared document, one source file
// that cites a record, and the records.
func decompositionFixture(records map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		decompositionMainDoc:    {Data: []byte("# Shared\n\nPrinciples and measurements; no list of records.\n")},
		"ceph/network_state.go": {Data: []byte("// See docs/design/container-decomposition/network-state.md.\npackage ceph\n")},
	}
	for name, body := range records {
		fsys[path.Join(decompositionDir, name)] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

const residualFixtureBody = "# Residual fields\n\n| `mu` | lock |\n| `closed` | flag |\n"

func goodRecords() map[string]string {
	return map[string]string{
		"network-state.md": "# Networking state\n\nBody.\n",
		residualRecordName: residualFixtureBody,
	}
}

func TestDecompositionGuardsDiscoverAnAddedRecord(t *testing.T) {
	records := goodRecords()
	if err := checkDecompositionRecords(decompositionFixture(records)); err != nil {
		t.Fatal(err)
	}
	// The new record is discovered with no edit to the shared document, to any
	// list or to any count.
	records["fault-registry.md"] = "# Fault registry\n\nBody.\n"
	fsys := decompositionFixture(records)
	names, err := decompositionRecordNames(fsys)
	if err != nil || !slices.Contains(names, "fault-registry.md") {
		t.Fatalf("names = %v, err = %v", names, err)
	}
	if err := checkDecompositionRecords(fsys); err != nil {
		t.Fatal(err)
	}
	// A source that cites the new record resolves, and one that cites it
	// before it exists does not.
	fsys["ceph/fault_registry.go"] = &fstest.MapFile{Data: []byte("// docs/design/container-decomposition/fault-registry.md\npackage ceph\n")}
	if err := checkCitedRecordsExist(fsys); err != nil {
		t.Fatal(err)
	}
	delete(records, "fault-registry.md")
	fsys = decompositionFixture(records)
	fsys["ceph/fault_registry.go"] = &fstest.MapFile{Data: []byte("// docs/design/container-decomposition/fault-registry.md\npackage ceph\n")}
	if checkCitedRecordsExist(fsys) == nil {
		t.Fatal("a citation of an absent record was accepted")
	}
}

func TestDecompositionGuardsRefuseARemovedOrRenamedRecord(t *testing.T) {
	records := goodRecords()
	if err := checkCitedRecordsExist(decompositionFixture(records)); err != nil {
		t.Fatal(err)
	}
	removed := goodRecords()
	delete(removed, "network-state.md")
	if checkCitedRecordsExist(decompositionFixture(removed)) == nil {
		t.Error("a removed record that a source cites was accepted")
	}
	renamed := goodRecords()
	renamed["networking.md"] = renamed["network-state.md"]
	delete(renamed, "network-state.md")
	if checkCitedRecordsExist(decompositionFixture(renamed)) == nil {
		t.Error("a renamed record that a source still cites by its old name was accepted")
	}
	// The shared document is checked the same way.
	fsys := decompositionFixture(goodRecords())
	fsys[decompositionMainDoc] = &fstest.MapFile{Data: []byte("# Shared\n\nSee container-decomposition/gone.md.\n")}
	if checkCitedRecordsExist(fsys) == nil {
		t.Error("a shared-document link to an absent record was accepted")
	}
}

func TestResidualLedgerRequiresItsRecord(t *testing.T) {
	got, err := residualRecordFields(decompositionFixture(goodRecords()))
	if err != nil || !slices.Equal(got, []string{"closed", "mu"}) {
		t.Fatalf("fields = %v, err = %v", got, err)
	}
	missing := goodRecords()
	delete(missing, residualRecordName)
	if _, err := residualRecordFields(decompositionFixture(missing)); err == nil {
		t.Error("a missing residual-field record was accepted")
	}
	// Rows in another record do not stand in for it.
	other := goodRecords()
	delete(other, residualRecordName)
	other["other.md"] = residualFixtureBody
	if _, err := residualRecordFields(decompositionFixture(other)); err == nil {
		t.Error("rows in a different record were read as the residual-field record")
	}
	empty := goodRecords()
	empty[residualRecordName] = "# Residual fields\n\nNo rows.\n"
	if _, err := residualRecordFields(decompositionFixture(empty)); err == nil {
		t.Error("a residual-field record without rows was accepted")
	}
}

func TestDecompositionRecordShapeIsRefused(t *testing.T) {
	cases := map[string]func(map[string]string){
		"a README":            func(m map[string]string) { m["README.md"] = "# Index\n" },
		"an upper-case name":  func(m map[string]string) { m["Network.md"] = "# N\n" },
		"a non-Markdown file": func(m map[string]string) { m["notes.txt"] = "x" },
		"a backup":            func(m map[string]string) { m["network-state.md~"] = "# N\n" },
		"an ordinal name":     func(m map[string]string) { m["fourteenth-slice.md"] = "# Member roster\n" },
		"a numeric name":      func(m map[string]string) { m["15th-member-roster.md"] = "# Member roster\n" },
		"a slice name":        func(m map[string]string) { m["member-roster-slice.md"] = "# Member roster\n" },
		"no title":            func(m map[string]string) { m["member-roster.md"] = "Body without a title.\n" },
		"an ordinal heading": func(m map[string]string) {
			m["member-roster.md"] = "# Member roster\n\n## Fourteenth internal slice: member roster\n"
		},
		"a numeric slice head": func(m map[string]string) { m["member-roster.md"] = "# Member roster\n\n### 14th slice\n" },
	}
	for name, edit := range cases {
		records := goodRecords()
		edit(records)
		if checkDecompositionRecords(decompositionFixture(records)) == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// A subdirectory is refused, not skipped.
	fsys := decompositionFixture(goodRecords())
	fsys[decompositionDir+"/sub/x.md"] = &fstest.MapFile{Data: []byte("# X\n")}
	if checkDecompositionRecords(fsys) == nil {
		t.Error("a subdirectory was accepted")
	}
	// A missing or empty directory is refused.
	if checkDecompositionRecords(fstest.MapFS{decompositionMainDoc: {Data: []byte("# S\n")}}) == nil {
		t.Error("a missing directory was accepted")
	}
	// Component names that contain an ordinal-looking word inside a longer
	// word are fine.
	records := goodRecords()
	records["firstly-named-component.md"] = "# Component\n"
	if err := checkDecompositionRecords(decompositionFixture(records)); err != nil {
		t.Errorf("a component name was refused: %v", err)
	}
}
