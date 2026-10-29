// Package moves checks cross-package API moves.
//
// A move of public API from one package to another edits two package
// catalogues under docs/design/api-naming. Each catalogue is reviewed on its
// own, so nothing in them says that a row that vanished from one package is
// the row that appeared in another. A move record,
// docs/design/api-naming/moves/<change-id>.json, states that mapping, and
// Check accounts for every row that was removed or added between a base
// revision and the working tree: each one is either a move, or a removal or
// addition that a record lists and a review note discusses. Without a base the
// accounting cannot run and the result is Skipped, never OK.
package moves

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

// RecordSchema names the record format.
const RecordSchema = "ceph-api-move/v1"

// RecordsDir holds the move records, relative to the module root.
const RecordsDir = catalogue.Dir + "/moves"

// maxFile bounds every file the check reads.
const maxFile = 16 << 20

// Move maps one declaration row to its row in another package.
type Move struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Record is one reviewed migration. Moves are rows that changed package;
// Removed and Added are rows that were deleted or introduced on purpose. All
// three name declaration rows by their catalogue "current" value.
type Record struct {
	Schema  string   `json:"schema"`
	Change  string   `json:"change"`
	Review  string   `json:"review"`
	Moves   []Move   `json:"moves,omitempty"`
	Removed []string `json:"removed,omitempty"`
	Added   []string `json:"added,omitempty"`
}

var (
	recordFile  = regexp.MustCompile(`\A([a-z0-9][a-z0-9-]{0,63})\.json\z`)
	packagePath = regexp.MustCompile(`\A[A-Za-z0-9_][A-Za-z0-9_.-]*(/[A-Za-z0-9_][A-Za-z0-9_.-]*)*\z`)
	rowName     = regexp.MustCompile(`\A([A-Za-z0-9_][A-Za-z0-9_-]*(?:/[A-Za-z0-9_][A-Za-z0-9_-]*)*)\.[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?\z`)
	revision    = regexp.MustCompile(`\A[A-Za-z0-9_][A-Za-z0-9._/@^~{}+-]*\z`)
)

// reservedIDs cannot name a record: they are catalogue file names.
var reservedIDs = map[string]bool{"declarations": true, "members": true, "review": true}

func isCatalogueName(name string) bool {
	return name == catalogue.DeclarationsFile || name == catalogue.MembersFile || name == catalogue.ReceiptFile
}

// fileKind says what a repository path below the catalogue directory is.
type fileKind int

const (
	kindNone     fileKind = iota // not a file this check reads
	kindCatalog                  // declarations.json, members.json or review.json of a package
	kindRecord                   // moves/<change-id>.json
	kindStray                    // a name in moves/ that is not a record
	kindReserved                 // moves/<catalogue file name>
)

// classify decides by exact, whole-string matching. A path that carries a
// trailing newline or any other unexpected byte is never a catalogue or
// record; inside moves/ it is reported as stray.
func classify(rel string) (kind fileKind, pkg string) {
	prefix := catalogue.Dir + "/"
	if !strings.HasPrefix(rel, prefix) {
		return kindNone, ""
	}
	rest := rel[len(prefix):]
	if strings.HasPrefix(rest, "moves/") {
		name := rest[len("moves/"):]
		switch {
		case strings.Contains(name, "/"):
			return kindNone, ""
		case isCatalogueName(name):
			return kindReserved, ""
		case recordFile.MatchString(name):
			return kindRecord, strings.TrimSuffix(name, ".json")
		}
		return kindStray, ""
	}
	i := strings.LastIndex(rest, "/")
	if i < 0 {
		return kindNone, ""
	}
	dir, name := rest[:i], rest[i+1:]
	if isCatalogueName(name) && packagePath.MatchString(dir) {
		return kindCatalog, dir
	}
	return kindNone, ""
}

func pkgOfRow(name string) (string, bool) {
	match := rowName.FindStringSubmatch(name)
	if match == nil {
		return "", false
	}
	return match[1], true
}

func decodeStrict(data []byte, into any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return err
	}
	// More does not establish the end of input: a stray closing delimiter
	// makes it report false. Decoding again must hit io.EOF exactly, which
	// rejects a second value and any trailing syntax error alike.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing data after the JSON value")
	}
	return nil
}

// ParseRecord reads one record strictly and validates it on its own: schema,
// identifier, row names, packages, and duplicate or contradictory entries.
// Whether the rows exist is the base accounting's concern.
func ParseRecord(id string, data []byte) (Record, []string) {
	var record Record
	var problems []string
	fail := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf("record %s: "+format, append([]any{id}, args...)...))
	}
	if err := decodeStrict(data, &record); err != nil {
		fail("%v", err)
		return record, problems
	}
	if record.Schema != RecordSchema {
		fail("schema %q is not %q", record.Schema, RecordSchema)
	}
	if record.Change != id {
		fail("change %q must equal the file name %q", record.Change, id)
	}
	if reservedIDs[id] {
		fail("%q is reserved for catalogue files", id)
	}
	if record.Review == "" || !filepath.IsLocal(record.Review) || !strings.HasSuffix(record.Review, ".md") || strings.Contains(record.Review, "/") {
		fail("review %q is not a note file name below docs/design", record.Review)
	}
	if len(record.Moves)+len(record.Removed)+len(record.Added) == 0 {
		fail("lists no move, removal or addition")
	}
	removal, addition := map[string]bool{}, map[string]bool{}
	use := func(set map[string]bool, name, role string) {
		if _, ok := pkgOfRow(name); !ok {
			fail("%s %q is not a qualified declaration name", role, name)
			return
		}
		if set[name] {
			fail("%s %q is listed more than once", role, name)
		}
		set[name] = true
	}
	for _, move := range record.Moves {
		use(removal, move.From, "move source")
		use(addition, move.To, "move target")
		from, fromOK := pkgOfRow(move.From)
		to, toOK := pkgOfRow(move.To)
		if fromOK && toOK && from == to {
			fail("move %s -> %s stays inside package %s; list it as a removal and an addition", move.From, move.To, from)
		}
	}
	for _, name := range record.Removed {
		use(removal, name, "removal")
	}
	for _, name := range record.Added {
		use(addition, name, "addition")
	}
	return record, problems
}
