package catalogue_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
)

// The fixtures are synthetic Go packages in a temporary module tree. Nothing
// here needs Docker or the real catalogues.

const alphaSource = `package alpha

type Widget struct {
	Name string
	Size int
}

func (w Widget) Render() string { return "" }

func New() Widget { return Widget{} }

const Version = "1"
`

const betaSource = `package beta

type Config struct{ Level int }

func Run() {}
`

const subSource = `package sub

func Helper() {}
`

func write(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, root, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func replace(t *testing.T, root, name, old, replacement string) {
	t.Helper()
	text := read(t, root, name)
	if !strings.Contains(text, old) {
		t.Fatalf("%s does not contain %q", name, old)
	}
	write(t, root, name, strings.Replace(text, old, replacement, 1))
}

// tree returns a module tree whose catalogues are regenerated and whose
// receipts are approved.
func tree(t *testing.T) (string, catalogue.Config) {
	t.Helper()
	root := t.TempDir()
	write(t, root, "alpha/alpha.go", alphaSource)
	write(t, root, "alpha/sub/sub.go", subSource)
	write(t, root, "beta/beta.go", betaSource)
	write(t, root, "beta/beta_test.go", "package beta\nfunc TestOnly() {}\n")
	write(t, root, "onlytests/x_test.go", "package onlytests\n")
	write(t, root, "internal/hidden/hidden.go", "package hidden\nfunc Exported() {}\n")
	write(t, root, "alpha/testdata/data.go", "package data\nfunc Exported() {}\n")
	write(t, root, ".github/tool/tool.go", "package tool\nfunc Exported() {}\n")
	for _, page := range []string{"alpha", "alpha-sub", "beta"} {
		write(t, root, "docs/"+page+".md", "page\n")
	}
	write(t, root, "docs/design/api-naming-audit.md", "review\n")
	cfg := catalogue.Config{Root: root, Proposals: map[string]string{}}
	if _, err := catalogue.Regenerate(cfg, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := catalogue.Approve(cfg, []string{"alpha", "alpha/sub", "beta"}, ""); err != nil {
		t.Fatal(err)
	}
	return root, cfg
}

// verify is the union of the repository tests for one tree.
func verify(t *testing.T, cfg catalogue.Config) map[string]error {
	t.Helper()
	packages, err := catalogue.Discover(cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	surfaces, err := catalogue.Generate(cfg, packages)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]error{}
	for _, pkg := range packages {
		if err := catalogue.CheckCatalogue(cfg.Root, pkg, surfaces[pkg]); err != nil {
			result[pkg] = err
		} else if err := catalogue.CheckReceipt(cfg.Root, pkg, surfaces[pkg]); err != nil {
			result[pkg] = err
		}
	}
	return result
}

func TestDiscoverFindsOnlyPublicPackages(t *testing.T) {
	root, _ := tree(t)
	got, err := catalogue.Discover(root)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"alpha", "alpha/sub", "beta"}; !slices.Equal(got, want) {
		t.Fatalf("packages %v, want %v", got, want)
	}
}

func TestDiscoverRefusesSourcesAtTheModuleRoot(t *testing.T) {
	root := t.TempDir()
	write(t, root, "root.go", "package root\nfunc Exported() {}\n")
	if _, err := catalogue.Discover(root); err == nil {
		t.Fatal("a Go source in the module root has no catalogue key and must be refused")
	}
}

func TestFreshTreeVerifies(t *testing.T) {
	root, cfg := tree(t)
	if failures := verify(t, cfg); len(failures) != 0 {
		t.Fatalf("fresh tree failed: %v", failures)
	}
	if err := catalogue.CheckLayout(root); err != nil {
		t.Fatal(err)
	}
}

func TestLayoutRefusesAPackageDirectoryWithoutAReceipt(t *testing.T) {
	root, _ := tree(t)
	if err := os.Remove(filepath.Join(root, "docs/design/api-naming/beta/review.json")); err != nil {
		t.Fatal(err)
	}
	err := catalogue.CheckLayout(root)
	if err == nil || !strings.Contains(err.Error(), "beta is missing") || !strings.Contains(err.Error(), "review.json") {
		t.Fatalf("a package directory without a receipt must be refused, got %v", err)
	}
}

func TestLayoutRefusesAnEmptyCatalogueOfANewPackage(t *testing.T) {
	root, _ := tree(t)
	write(t, root, "gamma/gamma.go", "package gamma\nfunc Run() {}\n")
	err := catalogue.CheckLayout(root)
	if err == nil || !strings.Contains(err.Error(), "public package gamma has no catalogue directory") {
		t.Fatalf("a new public package must need a catalogue, got %v", err)
	}
}

func TestLayoutRefusesACatalogueDirectoryWithoutAPackage(t *testing.T) {
	root, _ := tree(t)
	write(t, root, "docs/design/api-naming/ghost/declarations.json", "{}\n")
	err := catalogue.CheckLayout(root)
	if err == nil || !strings.Contains(err.Error(), "ghost has no public package") {
		t.Fatalf("an orphan catalogue directory must be refused, got %v", err)
	}
}

func TestLayoutRefusesAMissingCatalogueFile(t *testing.T) {
	root, _ := tree(t)
	if err := os.Remove(filepath.Join(root, "docs/design/api-naming/alpha/members.json")); err != nil {
		t.Fatal(err)
	}
	if err := catalogue.CheckLayout(root); err == nil || !strings.Contains(err.Error(), "members.json") {
		t.Fatalf("missing members.json must be refused, got %v", err)
	}
}

func TestRegenerationDoesNotApproveAChangedAPI(t *testing.T) {
	root, cfg := tree(t)
	before := read(t, root, "docs/design/api-naming/alpha/review.json")
	replace(t, root, "alpha/alpha.go", "func New() Widget", "func Make() Widget")
	if _, err := catalogue.Regenerate(cfg, nil); err != nil {
		t.Fatal(err)
	}
	if got := read(t, root, "docs/design/api-naming/alpha/review.json"); got != before {
		t.Fatal("regeneration rewrote a receipt")
	}
	failures := verify(t, cfg)
	if failures["alpha"] == nil || !strings.Contains(failures["alpha"].Error(), "changed since the recorded review") {
		t.Fatalf("a regenerated but unapproved API must fail the review check, got %v", failures)
	}
	if len(failures) != 1 {
		t.Fatalf("only the changed package may go stale: %v", failures)
	}
	if _, err := catalogue.Approve(cfg, []string{"alpha"}, ""); err != nil {
		t.Fatal(err)
	}
	if failures := verify(t, cfg); len(failures) != 0 {
		t.Fatalf("approval did not restore the tree: %v", failures)
	}
}

func TestApprovalOfOnePackageLeavesOthersAlone(t *testing.T) {
	root, cfg := tree(t)
	other := read(t, root, "docs/design/api-naming/beta/review.json")
	declarations := read(t, root, "docs/design/api-naming/alpha/declarations.json")
	replace(t, root, "alpha/alpha.go", "const Version", "const Release")
	if _, err := catalogue.Regenerate(cfg, []string{"alpha"}); err != nil {
		t.Fatal(err)
	}
	changed := read(t, root, "docs/design/api-naming/alpha/declarations.json")
	if changed == declarations {
		t.Fatal("regeneration did not update the changed package")
	}
	if _, err := catalogue.Approve(cfg, []string{"alpha"}, ""); err != nil {
		t.Fatal(err)
	}
	if read(t, root, "docs/design/api-naming/alpha/declarations.json") != changed {
		t.Fatal("approval rewrote a catalogue")
	}
	if read(t, root, "docs/design/api-naming/beta/review.json") != other {
		t.Fatal("approving alpha rewrote the receipt of beta")
	}
}

func TestSourceChangeStalesOnlyItsOwnPackage(t *testing.T) {
	root, cfg := tree(t)
	replace(t, root, "beta/beta.go", "func Run() {}", "func Run() {}\n\nfunc Extra() {}")
	failures := verify(t, cfg)
	if len(failures) != 1 || failures["beta"] == nil {
		t.Fatalf("only beta may fail, got %v", failures)
	}
}

func TestMissingDeclarationAndMemberRowsAreDetected(t *testing.T) {
	root, cfg := tree(t)
	// A source member that the catalogue does not list.
	replace(t, root, "alpha/alpha.go", "Size int", "Size int\n\tColor string")
	failures := verify(t, cfg)
	if failures["alpha"] == nil || !strings.Contains(failures["alpha"].Error(), "members.json is stale") {
		t.Fatalf("an unlisted member must be detected, got %v", failures)
	}
	// A catalogue row that the source no longer has.
	if _, err := catalogue.Regenerate(cfg, nil); err != nil {
		t.Fatal(err)
	}
	replace(t, root, "docs/design/api-naming/beta/declarations.json", `"current": "beta.Run"`, `"current": "beta.Gone"`)
	failures = verify(t, cfg)
	if failures["beta"] == nil || !strings.Contains(failures["beta"].Error(), "declarations.json is stale") {
		t.Fatalf("a vanished or renamed row must be detected, got %v", failures)
	}
}

func TestMissingCatalogueFilesAreDetected(t *testing.T) {
	root, cfg := tree(t)
	if err := os.Remove(filepath.Join(root, "docs/design/api-naming/beta/declarations.json")); err != nil {
		t.Fatal(err)
	}
	if failures := verify(t, cfg); failures["beta"] == nil {
		t.Fatal("a missing catalogue file must fail")
	}
}

func TestNameCollisionIsRefused(t *testing.T) {
	_, cfg := tree(t)
	cfg.Proposals = map[string]string{"alpha.New": "alpha.Version"}
	if _, err := catalogue.Regenerate(cfg, nil); err == nil || !strings.Contains(err.Error(), "name collision") {
		t.Fatalf("a proposal onto a retained name must be refused, got %v", err)
	}
	cfg.Proposals = map[string]string{"alpha.New": "beta.Run"}
	if _, err := catalogue.Regenerate(cfg, nil); err == nil || !strings.Contains(err.Error(), "name collision") {
		t.Fatalf("a proposal onto another package's name must be refused, got %v", err)
	}
	cfg.Proposals = map[string]string{"alpha.New": "alpha.Fresh", "alpha.Version": "alpha.Fresh"}
	if _, err := catalogue.Regenerate(cfg, nil); err == nil || !strings.Contains(err.Error(), "name collision") {
		t.Fatalf("two proposals onto one name must be refused, got %v", err)
	}
}

func TestStaleProposalIsRefused(t *testing.T) {
	_, cfg := tree(t)
	cfg.Proposals = map[string]string{"alpha.Missing": "alpha.Other"}
	if _, err := catalogue.Regenerate(cfg, nil); err == nil || !strings.Contains(err.Error(), "stale rename proposal") {
		t.Fatalf("a proposal for a name that does not exist must be refused, got %v", err)
	}
}

func TestPendingProposalBlocksAnyApproval(t *testing.T) {
	root, cfg := tree(t)
	cfg.Proposals = map[string]string{"alpha.New": "alpha.Create"}
	// A receiver proposal also marks its methods.
	cfg.Proposals["alpha.Widget"] = "alpha.Gadget"
	if _, err := catalogue.Regenerate(cfg, []string{"alpha"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(read(t, root, "docs/design/api-naming/alpha/declarations.json"), `"decision": "proposal"`) {
		t.Fatal("proposal rows were not recorded")
	}
	if !strings.Contains(read(t, root, "docs/design/api-naming/alpha/declarations.json"), `"proposed": "alpha.Gadget.Render"`) {
		t.Fatal("the receiver proposal did not reach its method")
	}
	before := read(t, root, "docs/design/api-naming/alpha/review.json")
	if _, err := catalogue.Approve(cfg, []string{"alpha"}, ""); err == nil || !strings.Contains(err.Error(), "pending proposals") {
		t.Fatalf("approval must refuse a pending proposal, got %v", err)
	}
	if read(t, root, "docs/design/api-naming/alpha/review.json") != before {
		t.Fatal("a refused approval changed the receipt")
	}
	if failures := verify(t, cfg); failures["alpha"] == nil {
		t.Fatal("a pending proposal must also fail the review check")
	}
}

func TestConflictingDuplicateDeclarationsAreRefusedAndNothingIsWritten(t *testing.T) {
	root, cfg := tree(t)
	before := read(t, root, "docs/design/api-naming/beta/declarations.json")
	write(t, root, "beta/beta_linux.go", "package beta\n\nfunc Run(extra int) {}\n")
	if _, err := catalogue.Regenerate(cfg, nil); err == nil || !strings.Contains(err.Error(), "conflicting API declarations: beta.Run") {
		t.Fatalf("conflicting build-tag alternatives must be refused, got %v", err)
	}
	if read(t, root, "docs/design/api-naming/beta/declarations.json") != before {
		t.Fatal("a refused regeneration wrote a catalogue")
	}
}

func TestIdenticalDuplicateDeclarationsAreOneRow(t *testing.T) {
	root, cfg := tree(t)
	write(t, root, "beta/beta_linux.go", "package beta\n\nfunc Run() {}\n")
	if _, err := catalogue.Regenerate(cfg, nil); err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(read(t, root, "docs/design/api-naming/beta/declarations.json"), `"current": "beta.Run"`); count != 1 {
		t.Fatalf("identical alternatives produced %d rows", count)
	}
}

func TestConflictingDuplicateMembersAreRefused(t *testing.T) {
	root, cfg := tree(t)
	write(t, root, "beta/beta_linux.go", "package beta\n\ntype Config struct{ Level string }\n")
	if _, err := catalogue.Regenerate(cfg, nil); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("conflicting member declarations must be refused, got %v", err)
	}
}

func TestApprovalRefusesAStaleCatalogue(t *testing.T) {
	root, cfg := tree(t)
	before := read(t, root, "docs/design/api-naming/alpha/review.json")
	replace(t, root, "alpha/alpha.go", "func New() Widget", "func Make() Widget")
	if _, err := catalogue.Approve(cfg, []string{"alpha"}, ""); err == nil || !strings.Contains(err.Error(), "is stale") {
		t.Fatalf("approval must not bless a catalogue that is behind the source, got %v", err)
	}
	if read(t, root, "docs/design/api-naming/alpha/review.json") != before {
		t.Fatal("a refused approval changed the receipt")
	}
}

func TestApprovalIsAllOrNothing(t *testing.T) {
	root, cfg := tree(t)
	replace(t, root, "alpha/alpha.go", "func New() Widget", "func Make() Widget")
	if _, err := catalogue.Regenerate(cfg, []string{"beta"}); err != nil {
		t.Fatal(err)
	}
	replace(t, root, "beta/beta.go", "func Run() {}", "func Run() {}\n\nfunc Extra() {}")
	if _, err := catalogue.Regenerate(cfg, []string{"beta"}); err != nil {
		t.Fatal(err)
	}
	beta := read(t, root, "docs/design/api-naming/beta/review.json")
	// alpha's catalogue is stale, beta's is fresh but unapproved.
	if _, err := catalogue.Approve(cfg, []string{"beta", "alpha"}, ""); err == nil {
		t.Fatal("approval of a stale package must fail")
	}
	if read(t, root, "docs/design/api-naming/beta/review.json") != beta {
		t.Fatal("a refused approval still wrote the receipt of a valid package")
	}
}

func TestApprovalNeedsExistingNotesAndPages(t *testing.T) {
	root, cfg := tree(t)
	if _, err := catalogue.Approve(cfg, []string{"alpha"}, "missing.md"); err == nil {
		t.Fatal("a review note that does not exist must be refused")
	}
	if _, err := catalogue.Approve(cfg, []string{"alpha"}, "../escape.md"); err == nil {
		t.Fatal("a review note outside docs/design must be refused")
	}
	if _, err := catalogue.Approve(cfg, []string{"alpha"}, "."); err == nil {
		t.Fatal("a directory is not a review note")
	}
	if err := os.Remove(filepath.Join(root, "docs/beta.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := catalogue.Approve(cfg, []string{"beta"}, ""); err == nil {
		t.Fatal("a package without its reference page must be refused")
	}
}

func TestApprovalNeedsNamedKnownPackages(t *testing.T) {
	_, cfg := tree(t)
	if _, err := catalogue.Approve(cfg, nil, ""); err == nil {
		t.Fatal("approval of nothing named must be refused, never default to all")
	}
	if _, err := catalogue.Approve(cfg, []string{"nosuch"}, ""); err == nil {
		t.Fatal("an unknown package must be refused")
	}
	if _, err := catalogue.Regenerate(cfg, []string{"nosuch"}); err == nil {
		t.Fatal("regenerating an unknown package must be refused")
	}
}

func TestReceiptIsReadStrictly(t *testing.T) {
	root, cfg := tree(t)
	path := "docs/design/api-naming/beta/review.json"
	good := read(t, root, path)
	packages, _ := catalogue.Discover(root)
	surfaces, err := catalogue.Generate(cfg, packages)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"unknown field":    strings.Replace(good, `"schema"`, `"extra": 1, "schema"`, 1),
		"trailing data":    good + "{}\n",
		"wrong package":    strings.Replace(good, `"package": "beta"`, `"package": "alpha"`, 1),
		"wrong schema":     strings.Replace(good, "review/v2", "review/v1", 1),
		"empty review":     strings.Replace(good, `"review": "api-naming-audit.md"`, `"review": ""`, 1),
		"review traversal": strings.Replace(good, `"review": "api-naming-audit.md"`, `"review": "../design/api-naming-audit.md"`, 1),
		"page outside":     strings.Replace(good, `"page": "docs/beta.md"`, `"page": "README.md"`, 1),
		"missing page":     strings.Replace(good, `"page": "docs/beta.md"`, `"page": "docs/none.md"`, 1),
		"wrong hash":       strings.Replace(good, `"declarations_sha256": "`, `"declarations_sha256": "0`, 1),
		"wrong count":      strings.Replace(good, `"declarations": 2`, `"declarations": 3`, 1),
		"empty receipt":    "{}\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if content == good {
				t.Fatal("the mutation changed nothing")
			}
			write(t, root, path, content)
			if err := catalogue.CheckReceipt(root, "beta", surfaces["beta"]); err == nil {
				t.Fatal("a malformed or inconsistent receipt passed")
			}
		})
	}
	write(t, root, path, good)
	if err := catalogue.CheckReceipt(root, "beta", surfaces["beta"]); err != nil {
		t.Fatalf("restored receipt failed: %v", err)
	}
}

func TestAggregateIsComputedAndNotCommitted(t *testing.T) {
	root, _ := tree(t)
	before, err := catalogue.Directories(root)
	if err != nil {
		t.Fatal(err)
	}
	rows, total, err := catalogue.Summarize(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].Package != "alpha" || rows[0].Declarations != 4 || rows[0].Types != 1 {
		t.Fatalf("rows %+v", rows)
	}
	sum := 0
	for _, row := range rows {
		sum += row.Declarations
	}
	if total.Declarations != sum || total.Declarations != 7 {
		t.Fatalf("total %+v", total)
	}
	table := catalogue.Markdown(rows, total)
	if !strings.Contains(table, "| `alpha/sub` | 1 |") || !strings.Contains(table, "| Total | 7 |") {
		t.Fatalf("table:\n%s", table)
	}
	after, _ := catalogue.Directories(root)
	if !slices.Equal(before, after) {
		t.Fatal("summarizing changed the tree")
	}
	if entries, _ := os.ReadDir(filepath.Join(root, "docs/design/api-naming")); len(entries) != 2 {
		t.Fatalf("an aggregate file appeared: %v", entries)
	}
}

func TestSummarizeRefusesAMismatchedCount(t *testing.T) {
	root, _ := tree(t)
	replace(t, root, "docs/design/api-naming/beta/declarations.json", `"count": 2`, `"count": 7`)
	if _, _, err := catalogue.Summarize(root); err == nil {
		t.Fatal("a count that disagrees with its rows must be refused")
	}
}

func TestDefaultPageFollowsThePackageDirectory(t *testing.T) {
	for pkg, want := range map[string]string{"ceph": "docs/ceph.md", "rbd/mirror": "docs/rbd-mirror.md", "rgw/multisite": "docs/rgw-multisite.md"} {
		if got := catalogue.DefaultPage(pkg); got != want {
			t.Errorf("%s: %s, want %s", pkg, got, want)
		}
	}
}
