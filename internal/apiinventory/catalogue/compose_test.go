package catalogue_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

// These tests need the git binary and nothing else: no Docker, no network,
// and none of the real catalogues. The scenarios are built from the shapes
// seen in real integrations of two pull requests on the same package: both
// sides add different declarations at the same place of declarations.json and
// both rewrite review.json with a different hash.

func git(t *testing.T, root string, args ...string) string {
	t.Helper()
	full := append([]string{"-C", root, "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid",
		"-c", "commit.gpgsign=false", "-c", "core.autocrlf=false"}, args...)
	out, err := exec.Command("git", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func gitReader(root string) catalogue.Reader {
	return func(rev, path string) ([]byte, error) {
		if exec.Command("git", "-C", root, "cat-file", "-e", rev+":"+path).Run() != nil {
			return nil, nil
		}
		return exec.Command("git", "-C", root, "show", rev+":"+path).Output()
	}
}

func approvedCommit(t *testing.T, cfg catalogue.Config, message string, packages ...string) string {
	t.Helper()
	if _, err := catalogue.Regenerate(cfg, packages); err != nil {
		t.Fatal(err)
	}
	if _, err := catalogue.Approve(cfg, packages, ""); err != nil {
		t.Fatal(err)
	}
	git(t, cfg.Root, "add", "-A")
	git(t, cfg.Root, "commit", "-m", message)
	return git(t, cfg.Root, "rev-parse", "HEAD")
}

// forkedTree returns a repository with a committed, approved base on main and
// two branches, each with its own approved change.
type fork struct {
	root, base, ours, theirs string
	cfg                      catalogue.Config
}

func forkWith(t *testing.T, ours, theirs func(t *testing.T, root string), approveTheirs bool) fork {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	root, cfg := tree(t)
	git(t, root, "init", "-q", "-b", "main")
	git(t, root, "add", "-A")
	git(t, root, "commit", "-q", "-m", "base")
	f := fork{root: root, cfg: cfg, base: git(t, root, "rev-parse", "HEAD")}
	git(t, root, "checkout", "-q", "-b", "theirs")
	theirs(t, root)
	if approveTheirs {
		f.theirs = approvedCommit(t, cfg, "theirs", "alpha")
	} else {
		if _, err := catalogue.Regenerate(cfg, []string{"alpha"}); err != nil {
			t.Fatal(err)
		}
		git(t, root, "add", "-A")
		git(t, root, "commit", "-q", "-m", "theirs without approval")
		f.theirs = git(t, root, "rev-parse", "HEAD")
	}
	git(t, root, "checkout", "-q", "main")
	ours(t, root)
	f.ours = approvedCommit(t, cfg, "ours", "alpha")
	return f
}

func addFunc(file, name string) func(t *testing.T, root string) {
	return func(t *testing.T, root string) {
		write(t, root, "alpha/"+file, "package alpha\n\nfunc "+name+"() {}\n")
	}
}

// merge starts the merge of theirs and reports which catalogue files Git left
// conflicted.
func (f fork) merge(t *testing.T) []string {
	t.Helper()
	cmd := exec.Command("git", "-C", f.root, "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "merge", "--no-commit", "--no-ff", "theirs")
	_ = cmd.Run()
	var conflicted []string
	for _, line := range strings.Split(git(t, f.root, "diff", "--name-only", "--diff-filter=U"), "\n") {
		if strings.HasPrefix(line, catalogue.Dir+"/") {
			conflicted = append(conflicted, line)
		}
	}
	return conflicted
}

func (f fork) opts() catalogue.ResolveOptions {
	return catalogue.ResolveOptions{Base: f.base, Ours: f.ours, Theirs: f.theirs, Read: gitReader(f.root)}
}

func TestResolveComposesTwoApprovedAdditionsToOnePackage(t *testing.T) {
	f := forkWith(t, addFunc("a.go", "AlphaOne"), addFunc("b.go", "AlphaTwo"), true)
	// Before: Git alone cannot combine the two approved states.
	conflicted := f.merge(t)
	for _, want := range []string{catalogue.Dir + "/alpha/declarations.json", catalogue.Dir + "/alpha/review.json"} {
		found := false
		for _, got := range conflicted {
			found = found || got == want
		}
		if !found {
			t.Fatalf("expected a textual conflict in %s, got %v", want, conflicted)
		}
	}
	if err := verify(t, f.cfg)["alpha"]; err == nil {
		t.Fatal("unresolved conflict markers must not verify")
	}
	// After: one command, no source change, and the merged tree verifies.
	results, err := catalogue.Resolve(f.cfg, f.opts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Package != "alpha" {
		t.Fatalf("results = %+v", results)
	}
	if problems := verify(t, f.cfg); len(problems) != 0 {
		t.Fatalf("merged tree does not verify: %v", problems)
	}
	declarations := read(t, f.root, catalogue.Dir+"/alpha/declarations.json")
	if !strings.Contains(declarations, "alpha.AlphaOne") || !strings.Contains(declarations, "alpha.AlphaTwo") || strings.Contains(declarations, "<<<<<<<") {
		t.Fatal("both additions must be present and no marker may remain")
	}
	// The resolved receipt is exactly what a separate human approve writes.
	resolved := read(t, f.root, catalogue.Dir+"/alpha/review.json")
	if _, err := catalogue.Approve(f.cfg, []string{"alpha"}, ""); err != nil {
		t.Fatal(err)
	}
	if again := read(t, f.root, catalogue.Dir+"/alpha/review.json"); again != resolved {
		t.Fatal("resolve and approve disagree about the receipt")
	}
}

func TestResolveComposesMembersAddedToOneType(t *testing.T) {
	ours := func(t *testing.T, root string) {
		replace(t, root, "alpha/alpha.go", "\tName string\n", "\tColor string\n\tName string\n")
	}
	theirs := func(t *testing.T, root string) {
		replace(t, root, "alpha/alpha.go", "\tSize int\n", "\tSize int\n\tWeight int\n")
	}
	f := forkWith(t, ours, theirs, true)
	f.merge(t)
	if _, err := catalogue.Resolve(f.cfg, f.opts(), []string{"alpha"}); err != nil {
		t.Fatal(err)
	}
	if problems := verify(t, f.cfg); len(problems) != 0 {
		t.Fatal(problems)
	}
	members := read(t, f.root, catalogue.Dir+"/alpha/members.json")
	for _, field := range []string{"Color", "Weight"} {
		if !strings.Contains(members, field) {
			t.Fatalf("member %s missing", field)
		}
	}
}

func TestResolveRefusesAParentThatWasNeverApproved(t *testing.T) {
	f := forkWith(t, addFunc("a.go", "AlphaOne"), addFunc("b.go", "AlphaTwo"), false)
	f.merge(t)
	before := read(t, f.root, catalogue.Dir+"/alpha/review.json")
	if _, err := catalogue.Resolve(f.cfg, f.opts(), nil); err == nil || !strings.Contains(err.Error(), "never approved") {
		t.Fatalf("err = %v", err)
	}
	if read(t, f.root, catalogue.Dir+"/alpha/review.json") != before {
		t.Fatal("a refused resolve must write nothing")
	}
}

func TestResolveRefusesARowNeitherParentHas(t *testing.T) {
	f := forkWith(t, addFunc("a.go", "AlphaOne"), addFunc("b.go", "AlphaTwo"), true)
	f.merge(t)
	// A merge resolution that also slips in an unreviewed API addition.
	write(t, f.root, "alpha/sneaky.go", "package alpha\n\nfunc Sneaky() {}\n")
	before := read(t, f.root, catalogue.Dir+"/alpha/declarations.json")
	_, err := catalogue.Resolve(f.cfg, f.opts(), nil)
	if err == nil || !strings.Contains(err.Error(), "alpha.Sneaky") {
		t.Fatalf("err = %v", err)
	}
	if read(t, f.root, catalogue.Dir+"/alpha/declarations.json") != before {
		t.Fatal("a refused resolve must write nothing")
	}
}

func TestResolveRefusesARowBothParentsKeepThatTheMergeDropped(t *testing.T) {
	f := forkWith(t, addFunc("a.go", "AlphaOne"), addFunc("b.go", "AlphaTwo"), true)
	f.merge(t)
	if err := os.Remove(filepath.Join(f.root, "alpha", "b.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := catalogue.Resolve(f.cfg, f.opts(), nil); err == nil || !strings.Contains(err.Error(), "alpha.AlphaTwo") {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveRefusesAPendingProposal(t *testing.T) {
	f := forkWith(t, addFunc("a.go", "AlphaOne"), addFunc("b.go", "AlphaTwo"), true)
	f.merge(t)
	f.cfg.Proposals = map[string]string{"alpha.AlphaOne": "alpha.Renamed"}
	if _, err := catalogue.Resolve(f.cfg, f.opts(), nil); err == nil {
		t.Fatal("a rename proposal row is a new decision and must be refused")
	}
}

func TestResolveDoesNothingWhenOnlyOneParentChangedThePackage(t *testing.T) {
	f := forkWith(t, func(t *testing.T, root string) { write(t, root, "docs/note.md", "x\n") }, addFunc("b.go", "AlphaTwo"), true)

	f.merge(t)
	results, err := catalogue.Resolve(f.cfg, f.opts(), nil)
	if err != nil || len(results) != 0 {
		t.Fatalf("results = %+v, err = %v", results, err)
	}
	if problems := verify(t, f.cfg); len(problems) != 0 {
		t.Fatalf("a one-sided change must merge cleanly: %v", problems)
	}
}

// state builds the committed bytes of one approved package state.
func state(t *testing.T, entries []catalogue.Entry, types map[string][]string, review string, approved bool) catalogue.State {
	t.Helper()
	d := catalogue.Declarations{Schema: catalogue.DeclarationsSchema, Package: "p", Count: len(entries), Entries: entries}
	count := 0
	for _, m := range types {
		count += len(m)
	}
	m := catalogue.Members{Schema: catalogue.MembersSchema, Package: "p", Count: count, Types: types}
	db, _ := catalogue.Marshal(d)
	mb, _ := catalogue.Marshal(m)
	r := catalogue.Receipt{Schema: catalogue.ReceiptSchema, Package: "p", Declarations: len(entries), Members: count, Review: review, Page: "docs/p.md",
		DeclarationsSHA256: fmt.Sprintf("%x", sha256.Sum256(db)), MembersSHA256: fmt.Sprintf("%x", sha256.Sum256(mb))}
	if !approved {
		r.DeclarationsSHA256 = strings.Repeat("0", 64)
	}
	rb, _ := catalogue.Marshal(r)
	return catalogue.State{Declarations: db, Members: mb, Receipt: rb}
}

func fn(name, sig string) catalogue.Entry {
	return catalogue.Entry{Current: "p." + name, Kind: "function", Signature: sig, Proposed: "p." + name, Decision: "baseline"}
}

func TestComposeRefusesTwoDifferentChangesToOneRow(t *testing.T) {
	base := state(t, []catalogue.Entry{fn("Run", "func()")}, map[string][]string{}, "a.md", true)
	ours := state(t, []catalogue.Entry{fn("Run", "func(int)")}, map[string][]string{}, "a.md", true)
	theirs := state(t, []catalogue.Entry{fn("Run", "func(string)")}, map[string][]string{}, "a.md", true)
	if _, err := catalogue.Compose("p", base, ours, theirs); err == nil || !strings.Contains(err.Error(), "declaration p.Run") {
		t.Fatalf("err = %v", err)
	}
	// Control: the same row changed identically on both sides is not a conflict.
	if _, err := catalogue.Compose("p", base, ours, ours); err != nil {
		t.Fatal(err)
	}
}

func TestComposeRefusesAChangeAgainstARemoval(t *testing.T) {
	base := state(t, []catalogue.Entry{fn("Run", "func()")}, map[string][]string{}, "a.md", true)
	ours := state(t, nil, map[string][]string{}, "a.md", true)
	theirs := state(t, []catalogue.Entry{fn("Run", "func(string)")}, map[string][]string{}, "a.md", true)
	if _, err := catalogue.Compose("p", base, ours, theirs); err == nil {
		t.Fatal("one side removed the row and the other changed it")
	}
	// Control: a removal against an untouched row composes to a removal.
	untouched := state(t, []catalogue.Entry{fn("Run", "func()")}, map[string][]string{}, "a.md", true)
	comp, err := catalogue.Compose("p", base, ours, untouched)
	if err != nil || len(comp.Declarations) != 0 {
		t.Fatalf("comp = %+v, err = %v", comp, err)
	}
}

func TestComposeRefusesConflictingReceiptFields(t *testing.T) {
	base := state(t, nil, map[string][]string{}, "a.md", true)
	ours := state(t, nil, map[string][]string{}, "b.md", true)
	theirs := state(t, nil, map[string][]string{}, "c.md", true)
	if _, err := catalogue.Compose("p", base, ours, theirs); err == nil || !strings.Contains(err.Error(), "receipt field review") {
		t.Fatalf("err = %v", err)
	}
	// Control: one side moved to another note, the other left the base's.
	comp, err := catalogue.Compose("p", base, ours, base)
	if err != nil || comp.Review != "b.md" {
		t.Fatalf("comp = %+v, err = %v", comp, err)
	}
}

func TestComposeRefusesUnapprovedAndMalformedSides(t *testing.T) {
	base := state(t, nil, map[string][]string{}, "a.md", true)
	good := state(t, nil, map[string][]string{}, "a.md", true)
	bad := state(t, nil, map[string][]string{}, "a.md", false)
	for name, c := range map[string]struct{ ours, theirs catalogue.State }{
		"ours receipt does not pin the bytes":   {bad, good},
		"theirs receipt does not pin the bytes": {good, bad},
		"ours receipt missing":                  {catalogue.State{Declarations: good.Declarations, Members: good.Members}, good},
		"theirs trailing data":                  {good, catalogue.State{Declarations: append(append([]byte{}, good.Declarations...), ']'), Members: good.Members, Receipt: good.Receipt}},
		"theirs catalogue null":                 {good, catalogue.State{Declarations: []byte("null\n"), Members: good.Members, Receipt: good.Receipt}},
		"theirs catalogue missing":              {good, catalogue.State{Members: good.Members, Receipt: good.Receipt}},
	} {
		if _, err := catalogue.Compose("p", base, c.ours, c.theirs); err == nil {
			t.Errorf("%s: want refusal", name)
		}
	}
	if _, err := catalogue.Compose("p", base, good, good); err != nil {
		t.Fatalf("control: %v", err)
	}
}

func memberCase(t *testing.T, base, ours, theirs []string) error {
	t.Helper()
	mk := func(rows []string) catalogue.State {
		return state(t, nil, map[string][]string{"p.T": rows}, "a.md", true)
	}
	_, err := catalogue.Compose("p", mk(base), mk(ours), mk(theirs))
	return err
}

func TestComposeKeepsMemberIdentityAcrossAnEdit(t *testing.T) {
	name := "T.Name field string"
	size := "T.Size field int"
	renamed := "T.Name field int"
	for label, c := range map[string]struct{ base, ours, theirs []string }{
		"removal against type edit":     {[]string{name, size}, []string{size}, []string{renamed, size}},
		"type edit against removal":     {[]string{name, size}, []string{renamed, size}, []string{size}},
		"edit against edit (types)":     {[]string{name, size}, []string{"T.Name field int", size}, []string{"T.Name field bool", size}},
		"edit against edit (tags)":      {[]string{name, size}, []string{"T.Name field string `json:\"a\"`", size}, []string{"T.Name field string `json:\"b\"`", size}},
		"tag edit against removal":      {[]string{name, size}, []string{size}, []string{"T.Name field string `json:\"a\"`", size}},
		"interface signature vs remove": {[]string{"T.Do method func()"}, []string{}, []string{"T.Do method func(int)"}},
		"interface edit against edit":   {[]string{"T.Do method func()"}, []string{"T.Do method func(int)"}, []string{"T.Do method func(string)"}},
	} {
		err := memberCase(t, c.base, c.ours, c.theirs)
		if err == nil || !strings.Contains(err.Error(), "member T.") {
			t.Errorf("%s: err = %v, want a member conflict", label, err)
		}
	}
	// Controls that must still compose: different members edited, the same
	// edit on both sides, and an edit against an untouched member.
	for label, c := range map[string]struct{ base, ours, theirs []string }{
		"different members edited":  {[]string{name, size}, []string{renamed, size}, []string{name, "T.Size field int64"}},
		"identical edit":            {[]string{name, size}, []string{renamed, size}, []string{renamed, size}},
		"edit against untouched":    {[]string{name, size}, []string{renamed, size}, []string{name, size}},
		"removal against untouched": {[]string{name, size}, []string{size}, []string{name, size}},
	} {
		if err := memberCase(t, c.base, c.ours, c.theirs); err != nil {
			t.Errorf("%s: %v", label, err)
		}
	}
	// Embeddings have no editable value: the embedded type is the identity.
	if err := memberCase(t, []string{"T embed A"}, []string{"T embed A", "T embed B"}, []string{"T embed A", "T embed C"}); err != nil {
		t.Errorf("two different embeddings added: %v", err)
	}
}

func TestComposeRefusesAMemberRemovalAgainstAnEditInExactSource(t *testing.T) {
	removeName := func(t *testing.T, root string) { replace(t, root, "alpha/alpha.go", "\tName string\n", "") }
	editName := func(t *testing.T, root string) { replace(t, root, "alpha/alpha.go", "\tName string\n", "\tName int\n") }
	for label, c := range map[string]struct{ ours, theirs func(*testing.T, string) }{
		"ours removes, theirs edits": {removeName, editName},
		"ours edits, theirs removes": {editName, removeName},
	} {
		f := forkWith(t, c.ours, c.theirs, true)
		f.merge(t)
		// The merged source keeps the edited member.
		write(t, f.root, "alpha/alpha.go", strings.Replace(alphaSource, "\tName string\n", "\tName int\n", 1))
		var before [3]string
		for i, name := range []string{"declarations.json", "members.json", "review.json"} {
			before[i] = read(t, f.root, catalogue.Dir+"/alpha/"+name)
		}
		if _, err := catalogue.Resolve(f.cfg, f.opts(), nil); err == nil || !strings.Contains(err.Error(), "member Widget.Name") {
			t.Fatalf("%s: err = %v", label, err)
		}
		for i, name := range []string{"declarations.json", "members.json", "review.json"} {
			if read(t, f.root, catalogue.Dir+"/alpha/"+name) != before[i] {
				t.Fatalf("%s: %s was written by a refused resolve", label, name)
			}
		}
	}
}

func TestComposeKeysReceiptFieldsByName(t *testing.T) {
	// An edit of one receipt field on each side composes; a different edit of
	// the same field does not (see the review-note control above).
	pageState := func(page string) catalogue.State {
		st := state(t, nil, map[string][]string{}, "a.md", true)
		var r catalogue.Receipt
		if err := decodeForTest(st.Receipt, &r); err != nil {
			t.Fatal(err)
		}
		r.Page = page
		st.Receipt, _ = catalogue.Marshal(r)
		return st
	}
	base := pageState("docs/p.md")
	review := state(t, nil, map[string][]string{}, "b.md", true)
	comp, err := catalogue.Compose("p", base, review, pageState("docs/q.md"))
	if err != nil || comp.Review != "b.md" || comp.Page != "docs/q.md" {
		t.Fatalf("comp = %+v, err = %v", comp, err)
	}
	if _, err := catalogue.Compose("p", base, pageState("docs/q.md"), pageState("docs/r.md")); err == nil || !strings.Contains(err.Error(), "receipt field page") {
		t.Fatalf("err = %v", err)
	}
}

func decodeForTest(data []byte, into any) error { return json.Unmarshal(data, into) }

func TestResolveRefusesAMergedMemberFormNeitherParentHas(t *testing.T) {
	f := forkWith(t, addFunc("a.go", "AlphaOne"), addFunc("b.go", "AlphaTwo"), true)
	f.merge(t)
	replace(t, f.root, "alpha/alpha.go", "\tSize int\n", "\tSize int64\n")
	if _, err := catalogue.Resolve(f.cfg, f.opts(), nil); err == nil || !strings.Contains(err.Error(), "member Widget.Size") {
		t.Fatalf("err = %v", err)
	}
}
