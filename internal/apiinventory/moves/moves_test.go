package moves_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/catalogue"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/apiinventory/moves"
)

func TestMovesLegitimateMove(t *testing.T) {
	r := baseRepo(t)
	legitHead(r)
	result := r.check("main")
	if result.Status() != "OK" || len(result.Problems) != 0 || result.Skipped {
		t.Fatalf("a complete move must pass: %+v", result)
	}
	if len(result.Base) != 40 {
		t.Fatalf("the compared base must be a full commit id, got %q", result.Base)
	}
	// The same move committed on a branch, compared with the merge base after
	// the base branch advanced, is the same migration.
	r.git("switch", "-q", "-c", "topic")
	r.commit("move")
	r.git("switch", "-q", "main")
	r.setPackage("beta", pkgFiles{rows: []row{{"beta.Other", "function"}}})
	r.write(docsDir+"/changes/9-beta.md", "---\nchange: 9-beta\nkind: none\nreason: test\n---\n")
	r.commit("main advances with an unrelated package")
	r.git("switch", "-q", "topic")
	if result := r.check("main"); result.Status() != "OK" {
		t.Fatalf("a committed move after the base advanced must pass: %+v", result)
	}
}

func TestMovesControlTable(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(r *repo)
		want   []string // every substring must appear in one problem each
	}{
		{"removal without any record", func(r *repo) {
			legitHead(r)
			r.remove(moves.RecordsDir)
		}, []string{"row alpha.MirrorConfig was removed", "row alpha/mirror.Config was added"}},
		{"one-sided move: target side never appears", func(r *repo) {
			legitHead(r)
			r.setPackage("alpha/mirror", pkgFiles{rows: mirrorHead.rows[:2], members: mirrorHead.members})
		}, []string{"alpha/mirror.Run is missing from the head catalogue"}},
		{"one-sided move: source side keeps the row", func(r *repo) {
			legitHead(r)
			r.setPackage("alpha", pkgFiles{rows: append([]row{{"alpha.RunMirror", "function"}}, alphaHead.rows...)})
		}, []string{"alpha.RunMirror still exists in the head catalogue"}},
		{"renamed target differs from the record", func(r *repo) {
			legitHead(r)
			rows := append([]row(nil), mirrorHead.rows...)
			rows[0] = row{"alpha/mirror.Settings", "type"}
			r.setPackage("alpha/mirror", pkgFiles{rows: rows, members: mirrorHead.members})
		}, []string{"alpha/mirror.Config is missing from the head catalogue", "row alpha/mirror.Settings was added"}},
		{"partial move: record lists two of three rows", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/"+changeID+".json", strings.Replace(legitRecord,
				`,
    {"from": "alpha.RunMirror", "to": "alpha/mirror.Run"}`, "", 1))
		}, []string{"row alpha.RunMirror was removed", "row alpha/mirror.Run was added"}},
		{"partial move: members stay behind", func(r *repo) {
			legitHead(r)
			r.setPackage("alpha/mirror", pkgFiles{rows: mirrorHead.rows})
		}, []string{"alpha.MirrorConfig has 2 members at the base but alpha/mirror.Config has none"}},
		{"moved type changes its member count", func(r *repo) {
			legitHead(r)
			r.setPackage("alpha/mirror", pkgFiles{rows: mirrorHead.rows, members: map[string][]string{"alpha/mirror.Config": {"Config.Name field string"}}})
		}, []string{"keeps the member count"}},
		{"duplicate target inside one record", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/"+changeID+".json", strings.Replace(legitRecord,
				`"to": "alpha/mirror.Run"`, `"to": "alpha/mirror.Config"`, 1))
		}, []string{"listed more than once"}},
		{"conflicting records claim one source", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/9-other.json", strings.Replace(strings.Replace(legitRecord, changeID, "9-other", 1),
				"alpha/mirror.Run", "alpha/mirror.Run2", 1))
			r.write(note, noteText+"9-other\n")
		}, []string{"both claim alpha.MirrorConfig as a removal"}},
		{"record without a move", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/"+changeID+".json", strings.Replace(legitRecord,
				`"from": "alpha.MirrorConfig",`, `"from": "alpha.Ghost",`, 1))
		}, []string{"alpha.Ghost is not a row of the base catalogue"}},
		{"record points to a row that exists at the base", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/"+changeID+".json", strings.Replace(legitRecord,
				`"to": "alpha/mirror.Run"`, `"to": "alpha.Keep"`, 1))
		}, []string{"alpha.Keep already existed at the base"}},
		{"kinds differ", func(r *repo) {
			legitHead(r)
			rows := append([]row(nil), mirrorHead.rows...)
			rows[2] = row{"alpha/mirror.Run", "var"}
			r.setPackage("alpha/mirror", pkgFiles{rows: rows, members: mirrorHead.members})
		}, []string{"alpha.RunMirror is a function but alpha/mirror.Run is a var"}},
		{"move inside one package", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/"+changeID+".json", strings.Replace(legitRecord,
				`"to": "alpha/mirror.Run"`, `"to": "alpha.Run"`, 1))
		}, []string{"stays inside package alpha"}},
		{"source package receipt not re-approved", func(r *repo) {
			legitHead(r)
			r.write(catalogue.Dir+"/alpha/review.json", baseReceipt(t, r, "alpha"))
		}, []string{"package alpha changed but its receipt is the same as at the base"}},
		{"target package receipt does not bind the catalogue", func(r *repo) {
			legitHead(r)
			path := catalogue.Dir + "/alpha/mirror/review.json"
			r.write(path, strings.Replace(r.read(path), `"declarations_sha256": "`, `"declarations_sha256": "0`, 1))
		}, []string{"package alpha/mirror: the receipt does not bind the head catalogue files"}},
		{"target package has no receipt", func(r *repo) {
			legitHead(r)
			r.remove(catalogue.Dir + "/alpha/mirror/review.json")
		}, []string{"package alpha/mirror changed but has no valid receipt"}},
		{"receipt names another review note", func(r *repo) {
			legitHead(r)
			r.reapprove("alpha/mirror", "elsewhere.md")
		}, []string{"the receipt of alpha/mirror names the review note elsewhere.md, not audit.md"}},
		{"review note does not mention the change", func(r *repo) {
			legitHead(r)
			r.write(note, "# Audit\n\n"+changeID+"x is unrelated\n")
		}, []string{"does not mention the change " + changeID}},
		{"release-note fragment missing", func(r *repo) {
			legitHead(r)
			r.remove(fragment)
		}, []string{"no release-note fragment"}},
		{"fragment with a newline in its name is not a fragment", func(r *repo) {
			legitHead(r)
			r.remove(fragment)
			r.write(fragment+"\n", fragmentMD)
		}, []string{"no release-note fragment"}},
		{"record file name with a trailing newline", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/"+changeID+".json\n", legitRecord)
		}, []string{"unexpected file"}},
		{"record id reserved for catalogue files", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/review.json", legitRecord)
		}, []string{"unexpected file"}},
		{"record with an unknown field", func(r *repo) {
			legitHead(r)
			r.write(moves.RecordsDir+"/"+changeID+".json", strings.Replace(legitRecord, `"review"`, `"extra": 1, "review"`, 1))
		}, []string{"unknown field"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := baseRepo(t)
			tc.mutate(r)
			result := r.check("main")
			if result.Status() != "FAILED" {
				t.Fatalf("want FAILED, got %s: %+v", result.Status(), result)
			}
			for _, want := range tc.want {
				if !contains(result.Problems, want) {
					t.Errorf("no problem contains %q in %q", want, result.Problems)
				}
			}
		})
	}
}

// baseReceipt returns the receipt bytes of a package as committed at main.
func baseReceipt(t *testing.T, r *repo, pkg string) string {
	t.Helper()
	return r.git("show", "main:"+catalogue.Dir+"/"+pkg+"/"+catalogue.ReceiptFile) + "\n"
}

func TestMovesReviewedRemovalAndAddition(t *testing.T) {
	removal := `{"schema": "ceph-api-move/v1", "change": "9-drop", "review": "audit.md", "removed": ["alpha.Keep"]}` + "\n"
	apply := func(r *repo) {
		r.setPackage("alpha", pkgFiles{rows: alphaBase.rows[1:], members: alphaBase.members})
		r.write(fragment, fragmentMD)
	}
	t.Run("listed and discussed", func(t *testing.T) {
		r := baseRepo(t)
		apply(r)
		r.write(moves.RecordsDir+"/9-drop.json", removal)
		r.write(note, "# Audit\n\n9-drop removes Keep.\n")
		if result := r.check("main"); result.Status() != "OK" {
			t.Fatalf("a reviewed removal must pass: %+v", result)
		}
	})
	t.Run("listed but never discussed", func(t *testing.T) {
		r := baseRepo(t)
		apply(r)
		r.write(moves.RecordsDir+"/9-drop.json", removal)
		if result := r.check("main"); result.Status() != "FAILED" || !contains(result.Problems, "does not mention the change 9-drop") {
			t.Fatalf("an unreviewed removal must fail: %+v", result)
		}
	})
	t.Run("not listed", func(t *testing.T) {
		r := baseRepo(t)
		apply(r)
		if result := r.check("main"); result.Status() != "FAILED" || !contains(result.Problems, "row alpha.Keep was removed") {
			t.Fatalf("an unlisted removal must fail: %+v", result)
		}
	})
	t.Run("added row is listed", func(t *testing.T) {
		r := baseRepo(t)
		r.setPackage("alpha", pkgFiles{rows: append([]row{{"alpha.Extra", "function"}}, alphaBase.rows...), members: alphaBase.members})
		r.write(fragment, fragmentMD)
		r.write(moves.RecordsDir+"/9-add.json", `{"schema": "ceph-api-move/v1", "change": "9-add", "review": "audit.md", "added": ["alpha.Extra"]}`+"\n")
		r.write(note, "# Audit\n\n9-add introduces Extra.\n")
		if result := r.check("main"); result.Status() != "OK" {
			t.Fatalf("a reviewed addition must pass: %+v", result)
		}
		r.remove(moves.RecordsDir)
		if result := r.check("main"); result.Status() != "FAILED" || !contains(result.Problems, "row alpha.Extra was added") {
			t.Fatalf("an unlisted addition must fail: %+v", result)
		}
	})
}

func TestMovesNoChangeAndHistoricalRecords(t *testing.T) {
	r := baseRepo(t)
	if result := r.check("main"); result.Status() != "OK" {
		t.Fatalf("an unchanged tree must pass: %+v", result)
	}
	// An earlier migration's record stays in the tree. It is history: later
	// catalogue changes are not judged against it, and it needs no re-approval.
	legitHead(r)
	r.commit("the migration")
	if result := r.check("main"); result.Status() != "OK" {
		t.Fatalf("a merged record with no change since must pass: %+v", result)
	}
	r.setPackage("alpha", pkgFiles{rows: append([]row{{"alpha.Later", "function"}}, alphaHead.rows...)})
	r.write(moves.RecordsDir+"/9-later.json", `{"schema": "ceph-api-move/v1", "change": "9-later", "review": "audit.md", "added": ["alpha.Later"]}`+"\n")
	r.write(note, noteText+"9-later adds Later.\n")
	r.write(docsDir+"/changes/9-later.md", "---\nchange: 9-later\nkind: none\nreason: test\n---\n")
	if result := r.check("main"); result.Status() != "OK" {
		t.Fatalf("a later change must not be judged against the old record: %+v", result)
	}
}

func TestMovesMissingBaseIsSkippedNotPassed(t *testing.T) {
	r := baseRepo(t)
	legitHead(r)
	for _, base := range []string{"", "no-such-branch", "0123456789abcdef0123456789abcdef01234567"} {
		result := r.check(base)
		if !result.Skipped || result.Status() != "SKIPPED" || result.Status() == "OK" || result.Reason == "" || result.Base != "" {
			t.Errorf("base %q: want SKIPPED with a reason, got %+v", base, result)
		}
	}
	// Record checks still ran: a malformed record fails even with no base.
	r.write(moves.RecordsDir+"/"+changeID+".json", "{}")
	if result := r.check(""); result.Status() != "FAILED" {
		t.Errorf("record checks must run without a base: %+v", result)
	}
	// With the base required the skip is a failure.
	result := moves.Check(moves.Options{Root: r.dir, Base: "", RequireBase: true})
	if result.Status() != "FAILED" || !contains(result.Problems, "required but skipped") {
		t.Errorf("a required base must fail when missing: %+v", result)
	}
}

func TestMovesSkippedWithoutHistory(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	r := &repo{t: t, dir: dir}
	// Not a Git repository at all.
	if result := moves.Check(moves.Options{Root: dir, Base: "main"}); result.Status() != "SKIPPED" {
		t.Fatalf("a directory without Git must be SKIPPED: %+v", result)
	}
	_ = r
	// A repository with a base that is unrelated to HEAD has no merge base.
	r = newRepo(t)
	r.write("a.txt", "a\n")
	r.commit("a")
	r.git("switch", "-q", "--orphan", "other")
	r.write("b.txt", "b\n")
	r.commit("b")
	if result := r.check("main"); result.Status() != "SKIPPED" || !strings.Contains(result.Reason, "no merge base") {
		t.Fatalf("unrelated histories must be SKIPPED: %+v", result)
	}
}

func TestMovesRejectsOddBaseRevisions(t *testing.T) {
	r := baseRepo(t)
	legitHead(r)
	witness := filepath.Join(r.dir, "witness")
	for _, base := range []string{
		"--output=" + witness, "-h", "--upload-pack=touch " + witness, "main\n", "main ", " main",
		"main;touch " + witness, "$(touch " + witness + ")", "`touch " + witness + "`", "main\x00x", "ma in", "",
	} {
		result := r.check(base)
		if base == "" {
			if result.Status() != "SKIPPED" {
				t.Errorf("empty base must be SKIPPED, got %+v", result)
			}
			continue
		}
		if result.Status() != "FAILED" || !contains(result.Problems, "is not a plain revision name") {
			t.Errorf("base %q: want a rejected revision, got %+v", base, result)
		}
		if _, err := os.Stat(witness); err == nil {
			t.Fatalf("base %q ran a command or wrote a file", base)
		}
	}
}

func TestMovesIgnoresPoisonedBaseNames(t *testing.T) {
	requireGit(t)
	r := newRepo(t)
	r.write(note, "# Audit\n")
	r.setPackage("alpha", alphaBase)
	// A base path that only looks like a catalogue file because of a trailing
	// newline, a symbolic link at a catalogue path, and a stray file in moves/.
	poison := catalogue.Dir + "/ghost/" + catalogue.DeclarationsFile + "\n"
	r.write(poison, string(mustMarshal(t, catalogue.Declarations{Schema: catalogue.DeclarationsSchema, Package: "ghost", Count: 1,
		Entries: []catalogue.Entry{{Current: "ghost.Vanish", Kind: "function"}}})))
	r.write(moves.RecordsDir+"/odd.json\n", "{}")
	link := filepath.Join(r.dir, catalogue.Dir, "link", catalogue.DeclarationsFile)
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../alpha/declarations.json", link); err != nil {
		t.Skip("symbolic links are not available")
	}
	r.commit("base with odd names")
	r.remove(poison)
	r.remove(moves.RecordsDir + "/odd.json\n")
	r.remove(catalogue.Dir + "/link")
	if result := r.check("main"); result.Status() != "OK" {
		t.Fatalf("names that are not exactly a catalogue path must not create rows: %+v", result)
	}
}

func TestMovesRefusesSymbolicRecord(t *testing.T) {
	r := baseRepo(t)
	legitHead(r)
	r.remove(moves.RecordsDir + "/" + changeID + ".json")
	r.write("elsewhere.json", legitRecord)
	if err := os.Symlink("../../../../elsewhere.json", filepath.Join(r.dir, moves.RecordsDir, changeID+".json")); err != nil {
		t.Skip("symbolic links are not available")
	}
	if result := r.check("main"); result.Status() != "FAILED" || !contains(result.Problems, "is not a regular file") {
		t.Fatalf("a symbolic-link record must be refused: %+v", result)
	}
}

func TestMovesBaseWithMalformedCatalogueFails(t *testing.T) {
	r := newRepo(t)
	r.write(note, "# Audit\n")
	r.write(catalogue.Dir+"/alpha/"+catalogue.DeclarationsFile, "{not json")
	r.commit("broken base")
	r.setPackage("alpha", alphaBase)
	if result := r.check("main"); result.Status() != "FAILED" || !contains(result.Problems, "base:") {
		t.Fatalf("a malformed base must fail, not pass: %+v", result)
	}
}

func TestParseRecord(t *testing.T) {
	good := func() string { return legitRecord }
	cases := []struct {
		name, id, data string
		want           string // empty: valid
	}{
		{"valid", changeID, good(), ""},
		{"schema", changeID, strings.Replace(good(), "move/v1", "move/v2", 1), "schema"},
		{"change differs from file", "other", good(), "must equal the file name"},
		{"reserved id", "members", strings.Replace(good(), changeID, "members", 1), "reserved"},
		{"review with a directory", changeID, strings.Replace(good(), `"audit.md"`, `"../audit.md"`, 1), "review"},
		{"review not markdown", changeID, strings.Replace(good(), `"audit.md"`, `"audit.json"`, 1), "review"},
		{"empty record", changeID, `{"schema":"ceph-api-move/v1","change":"9-alpha-mirror","review":"audit.md"}`, "lists no move"},
		{"unqualified name", changeID, strings.Replace(good(), `"alpha.MirrorConfig"`, `"MirrorConfig"`, 1), "not a qualified declaration name"},
		{"name with a newline", changeID, strings.Replace(good(), `"alpha.RunMirror"`, `"alpha.RunMirror\n"`, 1), "not a qualified declaration name"},
		{"trailing data", changeID, good() + "{}", "trailing data"},
		{"trailing closing brace", changeID, good() + "}", "trailing data"},
		{"trailing closing bracket", changeID, good() + "]", "trailing data"},
		{"trailing closing brace and text", changeID, good() + "} arbitrary", "trailing data"},
		{"trailing garbage", changeID, good() + "garbage", "trailing data"},
		{"trailing second object after spaces", changeID, good() + "  \n {\"a\":1}", "trailing data"},
		{"trailing whitespace only is valid", changeID, good() + " \n\t\n", ""},
		{"source listed as removal too", changeID, strings.Replace(good(), `"moves"`, `"removed": ["alpha.RunMirror"], "moves"`, 1), "listed more than once"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, problems := moves.ParseRecord(tc.id, []byte(tc.data))
			if tc.want == "" && len(problems) != 0 {
				t.Fatalf("unexpected problems: %q", problems)
			}
			if tc.want != "" && !contains(problems, tc.want) {
				t.Fatalf("want a problem containing %q, got %q", tc.want, problems)
			}
		})
	}
}

func TestMovesMembersAppearForTypeWithoutMembers(t *testing.T) {
	requireGit(t)
	r := newRepo(t)
	r.write(note, "# Audit\n")
	r.setPackage("alpha", pkgFiles{rows: alphaBase.rows})
	r.commit("base without members")
	legitHead(r)
	result := r.check("main")
	if result.Status() != "FAILED" || !contains(result.Problems, "had none at the base") {
		t.Fatalf("members that appear from nowhere must fail: %+v", result)
	}
}

func TestMovesCheckRejectsTrailingDelimiterRecord(t *testing.T) {
	for _, tail := range []string{"}", "]", "} arbitrary"} {
		r := baseRepo(t)
		legitHead(r)
		r.write(moves.RecordsDir+"/"+changeID+".json", legitRecord+tail)
		if result := r.check("main"); result.Status() != "FAILED" || !contains(result.Problems, "trailing data") {
			t.Errorf("tail %q: a record with trailing input must fail Check: %+v", tail, result)
		}
	}
}
