package moves_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/moves"
)

// The fixtures are temporary Git repositories holding synthetic catalogues
// shaped like the CephFS mirror move (a Mirror prefix dropped while a family
// of rows changes package). Nothing here needs Docker or the real catalogues.

// docsDir is spelled apart from the file names so that the fixtures do not
// look like readers of real documentation files to the CI inventory test.
const docsDir = "docs"

const (
	note     = docsDir + "/design/audit.md"
	changeID = "9-alpha-mirror"
	noteText = "# Audit\n\nThe " + changeID + " change moves the mirror family.\n"
)

type row struct{ name, kind string }

// pkgFiles describes one package's committed files.
type pkgFiles struct {
	rows    []row
	members map[string][]string
	review  string // empty means "audit.md"
}

type repo struct {
	t   *testing.T
	dir string
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
}

func newRepo(t *testing.T) *repo {
	t.Helper()
	requireGit(t)
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	r := &repo{t: t, dir: t.TempDir()}
	r.git("init", "-q", "-b", "main")
	return r
}

func (r *repo) git(args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", append([]string{"-C", r.dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (r *repo) write(name, content string) {
	r.t.Helper()
	path := filepath.Join(r.dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r *repo) remove(name string) {
	r.t.Helper()
	if err := os.RemoveAll(filepath.Join(r.dir, filepath.FromSlash(name))); err != nil {
		r.t.Fatal(err)
	}
}

func (r *repo) read(name string) string {
	r.t.Helper()
	data, err := os.ReadFile(filepath.Join(r.dir, filepath.FromSlash(name)))
	if err != nil {
		r.t.Fatal(err)
	}
	return string(data)
}

func (r *repo) commit(message string) {
	r.t.Helper()
	r.git("add", "-A")
	r.git("commit", "-q", "--allow-empty", "-m", message)
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := catalogue.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// setPackage writes the three files of a package, with a receipt that binds
// the catalogue bytes exactly as Approve would.
func (r *repo) setPackage(pkg string, files pkgFiles) {
	r.t.Helper()
	declarations := catalogue.Declarations{Schema: catalogue.DeclarationsSchema, Package: pkg, Count: len(files.rows)}
	for _, item := range files.rows {
		declarations.Entries = append(declarations.Entries, catalogue.Entry{Current: item.name, Kind: item.kind, Proposed: item.name, Decision: "baseline"})
	}
	members := catalogue.Members{Schema: catalogue.MembersSchema, Package: pkg, Types: files.members}
	if members.Types == nil {
		members.Types = map[string][]string{}
	}
	for _, list := range members.Types {
		members.Count += len(list)
	}
	declarationsBytes, membersBytes := mustMarshal(r.t, declarations), mustMarshal(r.t, members)
	review := files.review
	if review == "" {
		review = "audit.md"
	}
	receipt := catalogue.Receipt{
		Schema: catalogue.ReceiptSchema, Package: pkg,
		DeclarationsSHA256: fmt.Sprintf("%x", sha256.Sum256(declarationsBytes)),
		MembersSHA256:      fmt.Sprintf("%x", sha256.Sum256(membersBytes)),
		Declarations:       declarations.Count, Members: members.Count, Review: review, Page: docsDir + "/x.md",
	}
	dir := catalogue.Dir + "/" + pkg + "/"
	r.write(dir+catalogue.DeclarationsFile, string(declarationsBytes))
	r.write(dir+catalogue.MembersFile, string(membersBytes))
	r.write(dir+catalogue.ReceiptFile, string(mustMarshal(r.t, receipt)))
}

// reapprove rewrites only the receipt of a package from its current files.
func (r *repo) reapprove(pkg, review string) {
	r.t.Helper()
	dir := catalogue.Dir + "/" + pkg + "/"
	receipt := catalogue.Receipt{
		Schema: catalogue.ReceiptSchema, Package: pkg,
		DeclarationsSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(r.read(dir+catalogue.DeclarationsFile)))),
		MembersSHA256:      fmt.Sprintf("%x", sha256.Sum256([]byte(r.read(dir+catalogue.MembersFile)))),
		Review:             review, Page: docsDir + "/x.md",
	}
	r.write(dir+catalogue.ReceiptFile, string(mustMarshal(r.t, receipt)))
}

func (r *repo) check(base string) moves.Result {
	r.t.Helper()
	return moves.Check(moves.Options{Root: r.dir, Base: base})
}

// The base state: alpha holds the mirror family and one row that stays.
var (
	alphaBase = pkgFiles{
		rows: []row{
			{"alpha.Keep", "function"},
			{"alpha.MirrorConfig", "type"},
			{"alpha.MirrorConfig.Validate", "method"},
			{"alpha.RunMirror", "function"},
		},
		members: map[string][]string{"alpha.MirrorConfig": {"MirrorConfig.Name field string", "MirrorConfig.Size field int"}},
	}
	alphaHead  = pkgFiles{rows: []row{{"alpha.Keep", "function"}}}
	mirrorHead = pkgFiles{
		rows: []row{
			{"alpha/mirror.Config", "type"},
			{"alpha/mirror.Config.Validate", "method"},
			{"alpha/mirror.Run", "function"},
		},
		members: map[string][]string{"alpha/mirror.Config": {"Config.Name field string", "Config.Size field int"}},
	}
)

const legitRecord = `{
  "schema": "ceph-api-move/v1",
  "change": "9-alpha-mirror",
  "review": "audit.md",
  "moves": [
    {"from": "alpha.MirrorConfig", "to": "alpha/mirror.Config"},
    {"from": "alpha.MirrorConfig.Validate", "to": "alpha/mirror.Config.Validate"},
    {"from": "alpha.RunMirror", "to": "alpha/mirror.Run"}
  ]
}
`

// baseRepo commits the base state on main and returns the repository. The
// working tree then holds whatever the test changes: the head.
func baseRepo(t *testing.T) *repo {
	t.Helper()
	r := newRepo(t)
	r.write(note, "# Audit\n")
	r.setPackage("alpha", alphaBase)
	r.commit("base")
	return r
}

// legitHead applies a complete, correct move to the working tree.
func legitHead(r *repo) {
	r.setPackage("alpha", alphaHead)
	r.setPackage("alpha/mirror", mirrorHead)
	r.write(note, noteText)
	r.write(moves.RecordsDir+"/"+changeID+".json", legitRecord)
}

func contains(problems []string, parts ...string) bool {
	for _, problem := range problems {
		ok := true
		for _, part := range parts {
			ok = ok && strings.Contains(problem, part)
		}
		if ok {
			return true
		}
	}
	return false
}
