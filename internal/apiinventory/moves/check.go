package moves

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

// Options selects what to check.
type Options struct {
	// Root is the module root, a Git work tree.
	Root string
	// Base is a revision to compare with, such as origin/main. The comparison
	// uses its merge base with HEAD. Empty means no base is available.
	Base string
	// RequireBase turns a skipped comparison into a failure.
	RequireBase bool
}

// Result is the outcome. A skipped comparison is neither a pass nor a failure
// unless RequireBase is set, and Status never calls it OK.
type Result struct {
	Problems []string
	Skipped  bool
	Reason   string
	Base     string
}

// Status is "FAILED", "SKIPPED" or "OK".
func (r Result) Status() string {
	switch {
	case len(r.Problems) > 0:
		return "FAILED"
	case r.Skipped:
		return "SKIPPED"
	}
	return "OK"
}

// tree is one side of the comparison, parsed.
type tree struct {
	declarations map[string]catalogue.Declarations
	members      map[string]catalogue.Members
	receipts     map[string]catalogue.Receipt
	raw          map[string][]byte // path -> bytes of catalogue, receipt and record files
	rows         map[string]string // declaration row -> kind
	records      map[string]Record
	fragments    map[string][]byte
}

func parseTree(side string, snap *snapshot) (*tree, []string) {
	t := &tree{
		declarations: map[string]catalogue.Declarations{}, members: map[string]catalogue.Members{},
		receipts: map[string]catalogue.Receipt{}, raw: map[string][]byte{}, rows: map[string]string{},
		records: map[string]Record{}, fragments: map[string][]byte{},
	}
	problems := slices.Clone(snap.problems)
	fail := func(format string, args ...any) { problems = append(problems, side+": "+fmt.Sprintf(format, args...)) }
	for _, path := range slices.Sorted(mapsKeys(snap.files)) {
		data := snap.files[path]
		kind, name := classify(path)
		switch kind {
		case kindFragment:
			t.fragments[path] = data
		case kindRecord:
			t.raw[path] = data
			record, bad := ParseRecord(name, data)
			problems = append(problems, bad...)
			t.records[name] = record
		case kindCatalog:
			t.raw[path] = data
			file := path[strings.LastIndex(path, "/")+1:]
			switch file {
			case catalogue.DeclarationsFile:
				var value catalogue.Declarations
				if err := decodeStrict(data, &value); err != nil {
					fail("%s: %v", path, err)
					continue
				}
				if value.Schema != catalogue.DeclarationsSchema || value.Package != name || value.Count != len(value.Entries) {
					fail("%s: schema, package or count does not match", path)
					continue
				}
				for _, entry := range value.Entries {
					if _, dup := t.rows[entry.Current]; dup {
						fail("%s: duplicate row %s", path, entry.Current)
					}
					t.rows[entry.Current] = entry.Kind
				}
				t.declarations[name] = value
			case catalogue.MembersFile:
				var value catalogue.Members
				if err := decodeStrict(data, &value); err != nil {
					fail("%s: %v", path, err)
					continue
				}
				if value.Schema != catalogue.MembersSchema || value.Package != name {
					fail("%s: schema or package does not match", path)
					continue
				}
				t.members[name] = value
			default:
				var value catalogue.Receipt
				if err := decodeStrict(data, &value); err != nil {
					fail("%s: %v", path, err)
					continue
				}
				if value.Schema != catalogue.ReceiptSchema || value.Package != name {
					fail("%s: schema or package does not match", path)
					continue
				}
				t.receipts[name] = value
			}
		}
	}
	return t, problems
}

func mapsKeys[V any](m map[string]V) func(yield func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func catalogPath(pkg, file string) string { return catalogue.Dir + "/" + pkg + "/" + file }

func recordPath(id string) string { return RecordsDir + "/" + id + ".json" }

func sum(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

// Check runs the record checks and, when a base is available, the accounting
// of every removed and added declaration row against it.
func Check(opts Options) Result {
	var result Result
	head, err := readHead(opts.Root)
	if err != nil {
		result.Problems = append(result.Problems, "reading the working tree: "+err.Error())
		return result
	}
	headTree, problems := parseTree("head", head)
	result.Problems = append(result.Problems, problems...)
	result.Problems = append(result.Problems, checkRecordSet(opts.Root, headTree)...)

	resolved := resolveBase(opts.Root, opts.Base)
	switch {
	case resolved.problem != "":
		result.Problems = append(result.Problems, resolved.problem)
	case resolved.skip != "":
		result.Skipped, result.Reason = true, resolved.skip
		if opts.RequireBase {
			result.Problems = append(result.Problems, "the base comparison was required but skipped: "+resolved.skip)
		}
	default:
		result.Base = resolved.sha
		base, err := readBase(opts.Root, resolved.sha)
		if err != nil {
			result.Problems = append(result.Problems, "reading the base: "+err.Error())
			break
		}
		baseTree, bad := parseTree("base", base)
		result.Problems = append(result.Problems, bad...)
		result.Problems = append(result.Problems, account(opts.Root, baseTree, headTree)...)
	}
	slices.Sort(result.Problems)
	result.Problems = slices.Compact(result.Problems)
	return result
}

func tokenIn(note []byte, id string) bool {
	return regexp.MustCompile(`(?:\A|[^a-z0-9-])` + regexp.QuoteMeta(id) + `(?:\z|[^a-z0-9-])`).Match(note)
}

// readNote reads a review note below docs/design, refusing links and
// irregular files.
func readNote(root, name string) ([]byte, error) {
	path := filepath.Join(root, "docs", "design", name)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxFile {
		return nil, fmt.Errorf("%s is not a regular file within the size limit", name)
	}
	return os.ReadFile(path)
}

// checkRecordSet runs the checks that need no base: every name is the source
// or the target of at most one record, and every record's review note exists
// and mentions the record's change identifier.
func checkRecordSet(root string, t *tree) []string {
	var problems []string
	removalBy, additionBy := map[string]string{}, map[string]string{}
	for _, id := range slices.Sorted(mapsKeys(t.records)) {
		record := t.records[id]
		claim := func(owners map[string]string, name, role string) {
			if other, ok := owners[name]; ok && other != id {
				problems = append(problems, fmt.Sprintf("records %s and %s both claim %s as %s", other, id, name, role))
			}
			owners[name] = id
		}
		for _, move := range record.Moves {
			claim(removalBy, move.From, "a removal")
			claim(additionBy, move.To, "an addition")
		}
		for _, name := range record.Removed {
			claim(removalBy, name, "a removal")
		}
		for _, name := range record.Added {
			claim(additionBy, name, "an addition")
		}
		if record.Review == "" || strings.Contains(record.Review, "/") || !strings.HasSuffix(record.Review, ".md") {
			continue
		}
		note, err := readNote(root, record.Review)
		if err != nil {
			problems = append(problems, fmt.Sprintf("record %s: review note: %v", id, err))
		} else if !tokenIn(note, id) {
			problems = append(problems, fmt.Sprintf("record %s: review note %s does not mention the change %s", id, record.Review, id))
		}
	}
	return problems
}

func memberCount(m catalogue.Members, key string) (int, bool) {
	list, ok := m.Types[key]
	return len(list), ok
}

// account compares the head with the base. Records new or changed since the
// base are the active ones: they must explain exactly the removed and added
// rows. An unchanged record is history and is not judged against later
// changes.
func account(root string, base, head *tree) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	removed, added := map[string]bool{}, map[string]bool{}
	for row := range base.rows {
		if _, ok := head.rows[row]; !ok {
			removed[row] = true
		}
	}
	for row := range head.rows {
		if _, ok := base.rows[row]; !ok {
			added[row] = true
		}
	}

	// Packages whose catalogue changed, in either direction.
	changed := map[string]bool{}
	for _, pkg := range append(slices.Collect(mapsKeys(base.declarations)), slices.Collect(mapsKeys(head.declarations))...) {
		for _, file := range []string{catalogue.DeclarationsFile, catalogue.MembersFile} {
			if string(base.raw[catalogPath(pkg, file)]) != string(head.raw[catalogPath(pkg, file)]) {
				changed[pkg] = true
			}
		}
	}

	accountedRemoval, accountedAddition := map[string]string{}, map[string]string{}
	touched := map[string]map[string]bool{} // record -> packages it relies on
	for _, id := range slices.Sorted(mapsKeys(head.records)) {
		path := recordPath(id)
		if old, ok := base.raw[path]; ok && string(old) == string(head.raw[path]) {
			continue
		}
		record := head.records[id]
		touched[id] = map[string]bool{}
		touch := func(name string) {
			if pkg, ok := pkgOfRow(name); ok {
				touched[id][pkg] = true
			}
		}
		removal := func(name, role string) bool {
			if other, ok := accountedRemoval[name]; ok {
				fail("record %s: %s %s is already accounted for by record %s", id, role, name, other)
				return false
			}
			accountedRemoval[name] = id
			touch(name)
			switch {
			case removed[name]:
				return true
			case head.rows[name] != "":
				fail("record %s: %s %s still exists in the head catalogue, so nothing moved or was removed", id, role, name)
			default:
				fail("record %s: %s %s is not a row of the base catalogue (record without a move)", id, role, name)
			}
			return false
		}
		addition := func(name, role string) bool {
			if other, ok := accountedAddition[name]; ok {
				fail("record %s: %s %s is already accounted for by record %s", id, role, name, other)
				return false
			}
			accountedAddition[name] = id
			touch(name)
			switch {
			case added[name]:
				return true
			case base.rows[name] != "":
				fail("record %s: %s %s already existed at the base, so it cannot be a new row", id, role, name)
			default:
				fail("record %s: %s %s is missing from the head catalogue (partial move)", id, role, name)
			}
			return false
		}
		for _, move := range record.Moves {
			okFrom, okTo := removal(move.From, "move source"), addition(move.To, "move target")
			if !okFrom || !okTo {
				continue
			}
			if base.rows[move.From] != head.rows[move.To] {
				fail("record %s: %s is a %s but %s is a %s", id, move.From, base.rows[move.From], move.To, head.rows[move.To])
			}
			fromPkg, _ := pkgOfRow(move.From)
			toPkg, _ := pkgOfRow(move.To)
			was, hadMembers := memberCount(base.members[fromPkg], move.From)
			now, hasMembers := memberCount(head.members[toPkg], move.To)
			switch {
			case hadMembers && !hasMembers:
				fail("record %s: %s has %d members at the base but %s has none (partial move)", id, move.From, was, move.To)
			case !hadMembers && hasMembers:
				fail("record %s: %s has %d members but %s had none at the base", id, move.To, now, move.From)
			case hadMembers && was != now:
				fail("record %s: %s has %d members at the base and %s has %d; a move keeps the member count, change the shape in a separate pull request", id, move.From, was, move.To, now)
			}
			if _, still := memberCount(head.members[fromPkg], move.From); still {
				fail("record %s: %s still has member rows in the head catalogue", id, move.From)
			}
		}
		for _, name := range record.Removed {
			removal(name, "removal")
		}
		for _, name := range record.Added {
			addition(name, "addition")
		}
	}

	for _, row := range slices.Sorted(mapsKeys(removed)) {
		if _, ok := accountedRemoval[row]; !ok {
			fail("row %s was removed from its package catalogue and no new or changed record under %s moves or removes it", row, RecordsDir)
		}
	}
	for _, row := range slices.Sorted(mapsKeys(added)) {
		if _, ok := accountedAddition[row]; !ok {
			fail("row %s was added to its package catalogue and no new or changed record under %s moves or adds it", row, RecordsDir)
		}
	}

	// A record names the review note both sides must carry in their receipts.
	for _, id := range slices.Sorted(mapsKeys(touched)) {
		for _, pkg := range slices.Sorted(mapsKeys(touched[id])) {
			if _, ok := head.declarations[pkg]; !ok {
				continue // the package no longer exists; its rows were all accounted for above
			}
			if receipt, ok := head.receipts[pkg]; ok && receipt.Review != head.records[id].Review {
				fail("record %s: the receipt of %s names the review note %s, not %s", id, pkg, receipt.Review, head.records[id].Review)
			}
		}
	}

	// Every package whose catalogue changed needs a receipt that is new in this
	// change and binds the head catalogue bytes.
	for _, pkg := range slices.Sorted(mapsKeys(changed)) {
		if _, ok := head.declarations[pkg]; !ok {
			continue
		}
		receiptPath := catalogPath(pkg, catalogue.ReceiptFile)
		receipt, ok := head.receipts[pkg]
		switch {
		case !ok:
			fail("package %s changed but has no valid receipt", pkg)
		case string(base.raw[receiptPath]) == string(head.raw[receiptPath]):
			fail("package %s changed but its receipt is the same as at the base (not re-approved)", pkg)
		case receipt.DeclarationsSHA256 != sum(head.raw[catalogPath(pkg, catalogue.DeclarationsFile)]) ||
			receipt.MembersSHA256 != sum(head.raw[catalogPath(pkg, catalogue.MembersFile)]):
			fail("package %s: the receipt does not bind the head catalogue files", pkg)
		}
	}

	if len(changed) > 0 {
		found := false
		for path, data := range head.fragments {
			if old, ok := base.fragments[path]; !ok || string(old) != string(data) {
				found = true
			}
		}
		if !found {
			fail("the catalogue of %s changed but no release-note fragment under docs/changes was added or changed (kind none with a reason is accepted)", strings.Join(slices.Sorted(mapsKeys(changed)), ", "))
		}
	}
	return problems
}
