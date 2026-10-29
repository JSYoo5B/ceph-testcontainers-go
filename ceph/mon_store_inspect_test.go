package ceph

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"
)

// monStoreInspectorFake and monStoreMonmapFake are the Docker-free stand-ins of
// the two ports. They return what the test sets and record the order of the
// calls; they model nothing of Docker or Ceph.
type monStoreInspectorFake struct {
	files   monStoreFiles
	err     error
	calls   *[]string
	volumes []string
}

func (f *monStoreInspectorFake) Inspect(_ context.Context, volume string) (monStoreFiles, error) {
	*f.calls = append(*f.calls, "inspect")
	f.volumes = append(f.volumes, volume)
	return f.files, f.err
}

type monStoreMonmapFake struct {
	member *monStoreMember
	err    error
	calls  *[]string
	asked  []string
}

func (f *monStoreMonmapFake) Member(_ context.Context, mon string) (*monStoreMember, error) {
	*f.calls = append(*f.calls, "monmap")
	f.asked = append(f.asked, mon)
	return f.member, f.err
}

func monStoreFilesFixture() monStoreFiles {
	rocks := "rocksdb\n"
	return monStoreFiles{kvBackend: &rocks, entries: []monStoreEntry{
		{path: "store.db", kind: monStoreKindDir, mode: 0o40755, inode: 11, size: 4096, mtimeNs: 1700000000000000000},
		{path: "store.db/000004.sst", kind: monStoreKindFile, mode: 0o100644, inode: 12, size: 2048, mtimeNs: 1700000000000000001, digest: digestB},
		{path: "kv_backend", kind: monStoreKindFile, mode: 0o100644, inode: 13, size: 8, mtimeNs: 1700000000000000002, digest: digestC},
		{path: "keyring", kind: monStoreKindFile, mode: 0o100600, inode: 14, size: 64, mtimeNs: 1700000000000000003, digest: digestA},
	}}
}

func monStoreMemberFixture() *monStoreMember {
	return &monStoreMember{name: "a", fsid: fsidOne, quorum: monStoreQuorumOut,
		addresses: []string{"v2:192.0.2.10:3300/0", "v1:192.0.2.10:6789/0"}}
}

// monStoreFilesFingerprint is the SHA-256 of monStoreFilesFixture under the
// encoding of monStoreFingerprint, computed by an independent script, so a
// change of the encoding fails here instead of silently orphaning every
// recorded fingerprint.
const monStoreFilesFingerprint = "1775f95c82da2e74edba47853c87d73e25dab648081044f69d9925b736c67b8b"

func TestMonStoreFingerprintMatchesTheIndependentGolden(t *testing.T) {
	if got := monStoreFingerprint(monStoreFilesFixture().entries); got != monStoreFilesFingerprint {
		t.Fatalf("fingerprint %s", got)
	}
}

func TestMonStoreFingerprintCoversEveryFieldAndIgnoresListingOrder(t *testing.T) {
	base := monStoreFingerprint(monStoreFilesFixture().entries)
	edits := map[string]func(e []monStoreEntry) []monStoreEntry{
		"path":   func(e []monStoreEntry) []monStoreEntry { e[1].path = "store.db/000005.sst"; return e },
		"kind":   func(e []monStoreEntry) []monStoreEntry { e[2].kind = monStoreKindOther; return e },
		"mode":   func(e []monStoreEntry) []monStoreEntry { e[1].mode++; return e },
		"inode":  func(e []monStoreEntry) []monStoreEntry { e[1].inode++; return e },
		"size":   func(e []monStoreEntry) []monStoreEntry { e[1].size++; return e },
		"mtime":  func(e []monStoreEntry) []monStoreEntry { e[1].mtimeNs++; return e },
		"digest": func(e []monStoreEntry) []monStoreEntry { e[1].digest = digestA; return e },
		"added": func(e []monStoreEntry) []monStoreEntry {
			return append(e, monStoreEntry{path: "extra", kind: monStoreKindDir})
		},
		"removed": func(e []monStoreEntry) []monStoreEntry { return e[:3] },
		// Without length delimiters these two listings would encode alike.
		"path split": func(e []monStoreEntry) []monStoreEntry {
			e[2].path, e[2].kind = "kv_backen", monStoreKindFile
			e[2].digest = "d" + e[2].digest[1:]
			return e
		},
	}
	for name, edit := range edits {
		if got := monStoreFingerprint(edit(monStoreFilesFixture().entries)); got == base {
			t.Errorf("a different %s kept the fingerprint", name)
		}
	}
	reversed := slices.Clone(monStoreFilesFixture().entries)
	slices.Reverse(reversed)
	if monStoreFingerprint(reversed) != base {
		t.Error("listing order changed the fingerprint")
	}
	input := monStoreFilesFixture().entries
	first := input[0].path
	monStoreFingerprint(input)
	if input[0].path != first {
		t.Error("fingerprinting reordered the caller's listing")
	}
}

func TestMonStoreFactsAcceptTheFixtureStoreAndBindItsKeyringAndShape(t *testing.T) {
	keyring, shape, fingerprint, err := monStoreFilesFixture().storeFacts()
	if err != nil || keyring != digestA || shape != monStoreShapeRocksDB || fingerprint != monStoreFilesFingerprint {
		t.Fatalf("facts %q %q %q: %v", keyring, shape, fingerprint, err)
	}
	// ceph-mon's marker has one trailing newline; the shell check strips it.
	for _, marker := range []string{"rocksdb", "rocksdb\n"} {
		f := monStoreFilesFixture()
		f.kvBackend = &marker
		if _, _, _, err := f.storeFacts(); err != nil {
			t.Errorf("marker %q refused: %v", marker, err)
		}
	}
}

func TestMonStoreFactsRefuseAnythingButTheBoundShape(t *testing.T) {
	str := func(s string) *string { return &s }
	without := func(path string) func(f *monStoreFiles) {
		return func(f *monStoreFiles) {
			f.entries = slices.DeleteFunc(f.entries, func(e monStoreEntry) bool { return e.path == path })
		}
	}
	retype := func(path string, kind monStoreKind, digest string) func(f *monStoreFiles) {
		return func(f *monStoreFiles) {
			for i := range f.entries {
				if f.entries[i].path == path {
					f.entries[i].kind, f.entries[i].digest = kind, digest
				}
			}
		}
	}
	cases := map[string]func(f *monStoreFiles){
		"no store.db":               without("store.db"),
		"store.db is a file":        retype("store.db", monStoreKindFile, digestA),
		"store.db is a symlink":     retype("store.db", monStoreKindSymlink, ""),
		"empty store.db":            without("store.db/000004.sst"),
		"no kv_backend":             without("kv_backend"),
		"kv_backend is a symlink":   retype("kv_backend", monStoreKindSymlink, ""),
		"kv_backend is a directory": retype("kv_backend", monStoreKindDir, ""),
		"kv_backend not read":       func(f *monStoreFiles) { f.kvBackend = nil },
		"kv_backend empty":          func(f *monStoreFiles) { f.kvBackend = str("") },
		"kv_backend newline only":   func(f *monStoreFiles) { f.kvBackend = str("\n") },
		"kv_backend leveldb":        func(f *monStoreFiles) { f.kvBackend = str("leveldb") },
		"kv_backend prefix":         func(f *monStoreFiles) { f.kvBackend = str("rocksdb2") },
		"kv_backend two lines":      func(f *monStoreFiles) { f.kvBackend = str("rocksdb\nx") },
		"no keyring":                without("keyring"),
		"keyring is a symlink":      retype("keyring", monStoreKindSymlink, ""),
		"keyring is a directory":    retype("keyring", monStoreKindDir, ""),
		"keyring digest missing":    retype("keyring", monStoreKindFile, ""),
		"keyring digest uppercase":  retype("keyring", monStoreKindFile, strings.ToUpper(digestA)),
		"keyring digest short":      retype("keyring", monStoreKindFile, digestA[:63]),
		"directory with digest":     retype("store.db", monStoreKindDir, digestA),
		"unknown kind":              retype("store.db/000004.sst", monStoreKind("socket"), ""),
		"nested symlink":            retype("store.db/000004.sst", monStoreKindSymlink, ""),
		"nested other":              retype("store.db/000004.sst", monStoreKindOther, ""),
		"duplicate path": func(f *monStoreFiles) {
			f.entries = append(f.entries, f.entries[1])
		},
		"absolute path":  func(f *monStoreFiles) { f.entries[1].path = "/store.db/x" },
		"parent escape":  func(f *monStoreFiles) { f.entries[1].path = "../x" },
		"inner dotdot":   func(f *monStoreFiles) { f.entries[1].path = "store.db/../x" },
		"unclean path":   func(f *monStoreFiles) { f.entries[1].path = "store.db//x" },
		"dot path":       func(f *monStoreFiles) { f.entries[1].path = "." },
		"empty path":     func(f *monStoreFiles) { f.entries[1].path = "" },
		"trailing slash": func(f *monStoreFiles) { f.entries[1].path = "store.db/x/" },
		"negative size":  func(f *monStoreFiles) { f.entries[1].size = -1 },
		"orphan entry": func(f *monStoreFiles) {
			f.entries = append(f.entries, monStoreEntry{path: "sub/file", kind: monStoreKindFile, digest: digestA})
		},
		"no entries":       func(f *monStoreFiles) { f.entries = nil },
		"nil entries only": func(f *monStoreFiles) { *f = monStoreFiles{} },
	}
	for name, edit := range cases {
		f := monStoreFilesFixture()
		edit(&f)
		_, _, _, err := f.storeFacts()
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		for _, secret := range []string{digestA, digestB, digestC} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: error prints a digest: %v", name, err)
			}
		}
	}
}

// fixtureVerifier returns a detached record whose identity is the one that the
// fixtures observe, and its fakes. Binding stores the keyring digest, shape and
// fingerprint of monStoreFilesFixture.
func monStoreVerifierFixture(t *testing.T) (*monStoreVolume, *monStoreDockerFake, *monStoreInspectorFake, *monStoreMonmapFake, *[]string) {
	t.Helper()
	_, r, d := newMonStoreFixture()
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	if err := r.recordContainer("ctr-1"); err != nil {
		t.Fatal(err)
	}
	identity := monStoreIdentity{keyringDigest: digestA, shape: monStoreShapeRocksDB, fingerprint: monStoreFilesFingerprint, fsid: fsidOne,
		addresses: []string{"v2:192.0.2.10:3300/0", "v1:192.0.2.10:6789/0"}}
	if err := r.bind(identity); err != nil {
		t.Fatal(err)
	}
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if err := r.detach(identity); err != nil {
		t.Fatal(err)
	}
	calls := &[]string{}
	return r, d, &monStoreInspectorFake{files: monStoreFilesFixture(), calls: calls}, &monStoreMonmapFake{member: monStoreMemberFixture(), calls: calls}, calls
}

func TestMonStoreVerifyStoppedAdmitsExactlyOneRequireDispatch(t *testing.T) {
	r, d, ins, mm, calls := monStoreVerifierFixture(t)
	creates := d.creates
	if err := r.verifyStopped(t.Context(), ins, mm); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(*calls, []string{"monmap", "inspect"}) || !slices.Equal(ins.volumes, []string{r.name}) || !slices.Equal(mm.asked, []string{"a"}) {
		t.Fatalf("calls %v, volumes %v, asked %v", *calls, ins.volumes, mm.asked)
	}
	launch, err := r.dispatch(t.Context())
	if err != nil || launch.mode != monStoreRequire || d.creates != creates {
		t.Fatalf("dispatch %+v, %v, creates %d->%d", launch, err, creates, d.creates)
	}
}

func TestMonStoreVerifyStoppedNamesFieldsAndNeverValues(t *testing.T) {
	mismatches := map[string]struct {
		field string
		edit  func(f *monStoreFiles, m *monStoreMember)
	}{
		"keyring": {"keyring digest", func(f *monStoreFiles, _ *monStoreMember) { f.entries[3].digest = digestC }},
		"content": {"store fingerprint", func(f *monStoreFiles, _ *monStoreMember) { f.entries[1].digest = digestC }},
		"mtime":   {"store fingerprint", func(f *monStoreFiles, _ *monStoreMember) { f.entries[1].mtimeNs++ }},
		"extra entry": {"store fingerprint", func(f *monStoreFiles, _ *monStoreMember) {
			f.entries = append(f.entries, monStoreEntry{path: "store.db/000005.log", kind: monStoreKindFile, digest: digestA})
		}},
		"fsid":    {"cluster FSID", func(_ *monStoreFiles, m *monStoreMember) { m.fsid = fsidTwo }},
		"port":    {"address vector", func(_ *monStoreFiles, m *monStoreMember) { m.addresses[0] = "v2:192.0.2.10:3301/0" }},
		"family":  {"address vector", func(_ *monStoreFiles, m *monStoreMember) { m.addresses[0] = "v2:[2001:db8::10]:3300/0" }},
		"missing": {"address vector", func(_ *monStoreFiles, m *monStoreMember) { m.addresses = m.addresses[:1] }},
		"extra":   {"address vector", func(_ *monStoreFiles, m *monStoreMember) { m.addresses = append(m.addresses, "v1:192.0.2.11:6789/0") }},
	}
	for name, c := range mismatches {
		r, d, ins, mm, _ := monStoreVerifierFixture(t)
		c.edit(&ins.files, mm.member)
		attachments, creates := r.attachment, d.creates
		err := r.verifyStopped(t.Context(), ins, mm)
		if err == nil || !strings.Contains(err.Error(), c.field) || r.verified {
			t.Errorf("%s: want a refusal naming %q, got %v (verified %v)", name, c.field, err, r.verified)
			continue
		}
		for _, secret := range []string{digestA, digestB, digestC, fsidOne, fsidTwo, monStoreFilesFingerprint, "192.0.2", "2001:db8"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: error prints a value: %v", name, err)
			}
		}
		if _, err := r.dispatch(t.Context()); err == nil || r.attachment != attachments || d.creates != creates || r.phase != monStoreDetached {
			t.Errorf("%s: a refused store was dispatched: %v", name, err)
		}
	}
}

func TestMonStoreVerifyStoppedRefusesMemberFactsThatAreAbsentOrUnusable(t *testing.T) {
	cases := map[string]struct {
		edit     func(m **monStoreMember)
		inspects bool
	}{
		"not in the map":     {func(m **monStoreMember) { *m = nil }, false},
		"another member":     {func(m **monStoreMember) { (*m).name = "b" }, false},
		"in quorum":          {func(m **monStoreMember) { (*m).quorum = monStoreQuorumIn }, false},
		"quorum unknown":     {func(m **monStoreMember) { (*m).quorum = 0 }, false},
		"quorum invalid":     {func(m **monStoreMember) { (*m).quorum = 7 }, false},
		"fsid missing":       {func(m **monStoreMember) { (*m).fsid = "" }, true},
		"fsid not canonical": {func(m **monStoreMember) { (*m).fsid = strings.ToUpper(fsidOne) }, true},
		"no addresses":       {func(m **monStoreMember) { (*m).addresses = nil }, true},
		"empty addresses":    {func(m **monStoreMember) { (*m).addresses = []string{} }, true},
		"empty address":      {func(m **monStoreMember) { (*m).addresses = []string{""} }, true},
		"duplicate address":  {func(m **monStoreMember) { (*m).addresses = []string{"v1:192.0.2.10:6789/0", "v1:192.0.2.10:6789/0"} }, true},
	}
	for name, c := range cases {
		r, _, ins, mm, calls := monStoreVerifierFixture(t)
		c.edit(&mm.member)
		if err := r.verifyStopped(t.Context(), ins, mm); err == nil || r.verified {
			t.Errorf("%s: accepted (%v)", name, err)
		}
		if got := slices.Contains(*calls, "inspect"); got != c.inspects {
			t.Errorf("%s: inspector called %v, want %v", name, got, c.inspects)
		}
	}
}

func TestMonStoreVerifyStoppedPropagatesSourceErrorsAndWithdrawsAnEarlierVerification(t *testing.T) {
	r, _, ins, mm, _ := monStoreVerifierFixture(t)
	if err := r.verifyStopped(t.Context(), ins, mm); err != nil || !r.verified {
		t.Fatal(err)
	}
	boom := errors.New("source unavailable")
	ins.err = boom
	if err := r.verifyStopped(t.Context(), ins, mm); !errors.Is(err, boom) || r.verified {
		t.Fatalf("inspector error: %v, verified %v", err, r.verified)
	}
	ins.err = nil
	if err := r.verifyStopped(t.Context(), ins, mm); err != nil {
		t.Fatal(err)
	}
	mm.err = boom
	if err := r.verifyStopped(t.Context(), ins, mm); !errors.Is(err, boom) || r.verified {
		t.Fatalf("monmap error: %v, verified %v", err, r.verified)
	}
}

func TestMonStoreVerifyStoppedOnlyInspectsAStoppedRetainedStore(t *testing.T) {
	// An attached generation: the inspector never reads a store a MON may write.
	r, _, ins, mm, calls := monStoreVerifierFixture(t)
	r.attachment = &monStoreAttachment{generation: 2, id: "ctr-2"}
	if err := r.verifyStopped(t.Context(), ins, mm); err == nil || len(*calls) != 0 {
		t.Fatalf("an attached store was inspected: %v, calls %v", err, *calls)
	}
	// Not yet bound: nothing is retained to compare with.
	_, fresh, _ := newMonStoreFixture()
	if err := fresh.verifyStopped(t.Context(), ins, mm); err == nil || len(*calls) != 0 {
		t.Fatalf("an unbound store was inspected: %v", err)
	}
	// Changed volume metadata is refused before either source is read.
	r, d, ins, mm, calls := monStoreVerifierFixture(t)
	d.stored.Labels[monStoreOwnerLabel] = "foreign"
	if err := r.verifyStopped(t.Context(), ins, mm); err == nil || len(*calls) != 0 || r.verified {
		t.Fatalf("a foreign volume was inspected: %v, calls %v", err, *calls)
	}
}

func TestMonStoreVerifyStoppedRejectsNilAndTypedNilPorts(t *testing.T) {
	var typedIns *monStoreInspectorFake
	var typedMon *monStoreMonmapFake
	for name, pair := range map[string]func(*monStoreInspectorFake, *monStoreMonmapFake) (monStoreInspector, monStoreMonmap){
		"nil inspector": func(_ *monStoreInspectorFake, m *monStoreMonmapFake) (monStoreInspector, monStoreMonmap) {
			return nil, m
		},
		"nil monmap": func(i *monStoreInspectorFake, _ *monStoreMonmapFake) (monStoreInspector, monStoreMonmap) {
			return i, nil
		},
		"typed nil inspector": func(_ *monStoreInspectorFake, m *monStoreMonmapFake) (monStoreInspector, monStoreMonmap) {
			return typedIns, m
		},
		"typed nil monmap": func(i *monStoreInspectorFake, _ *monStoreMonmapFake) (monStoreInspector, monStoreMonmap) {
			return i, typedMon
		},
	} {
		r, _, ins, mm, calls := monStoreVerifierFixture(t)
		i, m := pair(ins, mm)
		if err := r.verifyStopped(t.Context(), i, m); err == nil || r.verified || len(*calls) != 0 {
			t.Errorf("%s: %v, calls %v", name, err, *calls)
		}
	}
}

func TestMonStoreObservationDoesNotShareTheMonmapSlice(t *testing.T) {
	r, _, ins, mm, _ := monStoreVerifierFixture(t)
	observed, err := observeStopped(t.Context(), "a", r.name, ins, mm)
	if err != nil {
		t.Fatal(err)
	}
	mm.member.addresses[0] = "mutated"
	if slices.Contains(observed.addresses, "mutated") {
		t.Fatal("the observation aliases the monmap view's slice")
	}
}

// monStoreInspectSymbols are the names this file adds. Production code outside
// the file must not mention them until a slice adds a caller and updates this
// table in its own diff.
var monStoreInspectSymbols = []string{
	"monStoreInspector", "monStoreMonmap", "monStoreFiles", "monStoreEntry", "monStoreMember",
	"observeStopped", "verifyStopped", "storeFacts", "monStoreFingerprint", "isNilPort",
}

// monStoreInspectHomes are the production files that may mention the symbols
// above and implement the ports: the port file and the files of its adapters
// (monmap decoder, inspector container, shared JSON reader). See
// mon_store_adapters_test.go for the guard of the adapters themselves.
var monStoreInspectHomes = []string{
	"mon_store_inspect.go", "mon_store_monmap.go", "mon_store_inspect_container.go", "mon_store_inspect_engine.go", "mon_store_json.go",
	// mon_store_generation.go only reuses isNilPort for its nil check.
	"mon_store_generation.go",
}

// monStoreInspectProblems returns the references to the symbols from files
// outside monStoreInspectHomes, and the methods named Inspect or Member that
// would implement a port.
func monStoreInspectProblems(files map[string]*ast.File) (problems []string) {
	for file, parsed := range files {
		if slices.Contains(monStoreInspectHomes, file) {
			continue
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.Ident:
				if slices.Contains(monStoreInspectSymbols, x.Name) {
					problems = append(problems, file+" mentions "+x.Name)
				}
			case *ast.FuncDecl:
				if x.Recv != nil && (x.Name.Name == "Inspect" || x.Name.Name == "Member") {
					problems = append(problems, file+" declares a method "+x.Name.Name+" that could implement a MON store port")
				}
			}
			return true
		})
	}
	return problems
}

func TestMonStoreInspectionHasNoProductionCallerOrImplementation(t *testing.T) {
	files := parseProductionFiles(t)
	if _, ok := files["mon_store_inspect.go"]; !ok {
		t.Fatal("mon_store_inspect.go not found: the guard is vacuous")
	}
	if problems := monStoreInspectProblems(files); len(problems) != 0 {
		t.Fatalf("the MON store inspection is used in production: %v", problems)
	}
}

// TestMonStoreInspectionGuardDetectsACallerAndAnImplementation is the negative
// control of the guard above: a synthetic production file that calls the
// preflight and implements a port is reported.
func TestMonStoreInspectionGuardDetectsACallerAndAnImplementation(t *testing.T) {
	src := `package ceph
type adapter struct{}
func (adapter) Inspect() {}
func use(r *monStoreVolume) { _ = r.verifyStopped }
`
	parsed, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	problems := monStoreInspectProblems(map[string]*ast.File{"synthetic.go": parsed})
	if len(problems) != 2 {
		t.Fatalf("problems %v", problems)
	}
}

// TestMonStoreInspectionTypesHaveNoLock pins that the new types add no mutex:
// they hold plain values, and the preflight runs under Container.mu like the
// rest of the record.
func TestMonStoreInspectionTypesHaveNoLock(t *testing.T) {
	seen := 0
	for file, parsed := range parseProductionFiles(t) {
		if file != "mon_store_inspect.go" {
			continue
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if st, ok := n.(*ast.StructType); ok {
				seen++
				for _, f := range st.Fields.List {
					if sel, ok := f.Type.(*ast.SelectorExpr); ok {
						if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sync" {
							t.Errorf("%s declares a sync type", file)
						}
					}
				}
			}
			return true
		})
	}
	if seen != 3 {
		t.Fatalf("found %d structs, want 3 (entry, files, member)", seen)
	}
}

// TestMonStoreRefusalsSayWhy pins the reason for the refusals that other
// guards could also catch, so a control that disables one is not hidden by a
// neighbor.
func TestMonStoreRefusalsSayWhy(t *testing.T) {
	f := monStoreFilesFixture()
	f.entries[1].kind, f.entries[1].digest = monStoreKindSymlink, ""
	if _, _, _, err := f.storeFacts(); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("a symlink in the store: %v", err)
	}
	r, _, ins, mm, _ := monStoreVerifierFixture(t)
	mm.member.quorum = monStoreQuorumIn
	if err := r.verifyStopped(t.Context(), ins, mm); err == nil || !strings.Contains(err.Error(), "in quorum") {
		t.Fatalf("a member in quorum: %v", err)
	}
	mm.member.quorum = 0
	if err := r.verifyStopped(t.Context(), ins, mm); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("an unknown quorum state: %v", err)
	}
}
