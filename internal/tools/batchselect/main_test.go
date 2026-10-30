package main

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// fixture copies testdata/sel into a new git repository with one commit.
func fixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	err := filepath.WalkDir("testdata/sel", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel("testdata/sel", path)
		if entry.IsDir() {
			return os.MkdirAll(filepath.Join(dir, rel), 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, rel), data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	run(t, dir, "init", "-q")
	run(t, dir, "add", "-A")
	run(t, dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false",
		"commit", "-q", "-m", "fixture")
	return dir
}

func run(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}

func edit(t *testing.T, dir, file, old, replacement string) {
	t.Helper()
	path := filepath.Join(dir, file)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), old) {
		t.Fatalf("%s does not contain %q", file, old)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), old, replacement, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
}

// add writes a new file and stages it, so that the diff reports it added.
func add(t *testing.T, dir, file, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, dir, "add", "-A")
}

func selection(t *testing.T, dir string) result {
	t.Helper()
	out, err := selectBatches(options{root: dir, base: "HEAD", pattern: "./pkg", unionTags: "all",
		goos: runtime.GOOS, goarch: runtime.GOARCH, batches: []batchSpec{
			{ID: "a", Tags: "ci,ci_batch_a"}, {ID: "b", Tags: "ci,ci_batch_b"},
			{ID: "c", Tags: "ci,ci_batch_c"}, {ID: "d", Tags: "ci,ci_batch_d"},
		}})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func selected(out result) []string {
	ids := []string{}
	for _, b := range out.Batches {
		if b.Selected {
			ids = append(ids, b.ID)
		}
	}
	return ids
}

func TestSelectBatches(t *testing.T) {
	every := []string{"a", "b", "c", "d"}
	for name, test := range map[string]struct {
		change   func(t *testing.T, dir string)
		selected []string
		fallback string
	}{
		"nothing changed": {change: func(*testing.T, string) {}, selected: []string{}},
		"one batch file": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/a_test.go", `t.Fatal("helperA")`, `t.Fatalf("helperA: %s", helperA())`)
		}, selected: []string{"a"}},
		"helper of one batch": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/helpers_test.go", `strings.ToUpper("a")`, `strings.ToUpper("a" + "")`)
		}, selected: []string{"a"}},
		// fastRunner.run is called directly by c and, through the runner
		// interface in drive, by b: class hierarchy analysis does not
		// know that b only passes a slowRunner.
		"interface implementation": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/helpers_test.go", "func (fastRunner) run() int { return 1 }",
				"func (fastRunner) run() int {\n\treturn 1\n}")
		}, selected: []string{"b", "c"}},
		"interface declaration": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/helpers_test.go", "\trun() int\n", "\trun() int // the result\n")
		}, selected: []string{"b"}},
		"removed function that nothing names": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/helpers_test.go", "func unused() int {\n\treturn 4\n}\n\n", "")
		}, selected: []string{}},
		"comment between declarations": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/helpers_test.go", "// Helpers of single batches.", "// Helpers of one batch each.")
		}, selected: []string{}},
		"package-level initializer": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/helpers_test.go", "var shared = 3", "var shared = 1 + 2")
		}, selected: every},
		"documentation only": {change: func(t *testing.T, dir string) {
			edit(t, dir, "README.md", "# Selection fixture", "# Fixture")
		}, selected: []string{}},
		"comment in the import block": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/d_test.go", "\t_ \"embed\"\n", "\t_ \"embed\" // for data\n")
		}, selected: []string{}},
		// helpers_test.go imports strings already: only TestA changes.
		"import of a linked package": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/a_test.go", "import \"testing\"", "import (\n\t\"strings\"\n\t\"testing\"\n)")
			edit(t, dir, "pkg/a_test.go", "helperA() != \"A\"", "helperA() != strings.ToUpper(\"a\")")
		}, selected: []string{"a"}},
		"new fault package is outside integration": {change: func(t *testing.T, dir string) {
			if err := os.MkdirAll(filepath.Join(dir, "fault"), 0o755); err != nil {
				t.Fatal(err)
			}
			add(t, dir, "fault/network.go", "package fault\n")
		}, selected: every, fallback: "fault/network.go: outside pkg"},
		"import of a new package": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/a_test.go", "import \"testing\"", "import (\n\t_ \"net/http/pprof\"\n\t\"testing\"\n)")
		}, selected: every, fallback: "adds an import of net/http/pprof"},
		"removed import of the only importer": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/c_test.go", "\t\"os\"\n", "")
			edit(t, dir, "pkg/c_test.go", "os.ReadFile(\"testdata/expected.md\")", "[]byte(\"x\"), error(nil)")
		}, selected: every, fallback: "removes an import of os"},
		"embedded file": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/data.txt", "embedded", "changed")
		}, selected: every, fallback: "pkg/data.txt"},
		// Markdown inside the analysed package is an input, not
		// documentation: one file embedded, one read at run time.
		"embedded Markdown": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/notes.md", "embedded notes", "")
		}, selected: every, fallback: "pkg/notes.md"},
		"runtime-read Markdown": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/testdata/expected.md", "expected output", "")
		}, selected: every, fallback: "pkg/testdata/expected.md"},
		"go.mod": {change: func(t *testing.T, dir string) {
			edit(t, dir, "go.mod", "go 1.26", "go 1.26.0")
		}, selected: every, fallback: "go.mod changed"},
		"build constraint": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/a_test.go", "ci_batch_a)", "ci_batch_a) && !race")
		}, selected: every, fallback: "file header"},
		"added empty file": {change: func(t *testing.T, dir string) {
			add(t, dir, "pkg/e_test.go", "package pkg_test\n")
		}, selected: []string{}},
		// A new test of batch a, in a file of its own, and a helper in
		// a new file that every batch compiles but only a calls.
		"added batch file and helper file": {change: func(t *testing.T, dir string) {
			add(t, dir, "pkg/a2_test.go", "//go:build all || (ci && ci_batch_a)\n\npackage pkg_test\n\n"+
				"import \"testing\"\n\nfunc TestA2(t *testing.T) {\n\tif helperA2() != \"a\" {\n\t\tt.Fatal(\"a2\")\n\t}\n}\n")
			add(t, dir, "pkg/a2_helpers_test.go", "//go:build all || ci\n\npackage pkg_test\n\n"+
				"import \"strings\"\n\nfunc helperA2() string { return strings.ToLower(\"A\") }\n")
		}, selected: []string{"a"}},
		// A method in a new file changes the method set of slowRunner,
		// which b uses.
		"added method": {change: func(t *testing.T, dir string) {
			add(t, dir, "pkg/slow_test.go", "//go:build all || ci\n\npackage pkg_test\n\n"+
				"func (slowRunner) String() string { return \"slow\" }\n")
		}, selected: []string{"b"}},
		"added file with a new import": {change: func(t *testing.T, dir string) {
			add(t, dir, "pkg/e_test.go", "//go:build all || ci\n\npackage pkg_test\n\nimport _ \"net/http/pprof\"\n")
		}, selected: every, fallback: "imports net/http/pprof, which no other file of the build imports"},
		"added init function": {change: func(t *testing.T, dir string) {
			add(t, dir, "pkg/e_test.go", "//go:build all || ci\n\npackage pkg_test\n\nfunc init() {}\n")
		}, selected: every},
		"deleted file": {change: func(t *testing.T, dir string) {
			run(t, dir, "rm", "-q", "pkg/a_test.go")
		}, selected: every, fallback: "deleted file"},
		"untracked file": {change: func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, "pkg/e_test.go"), []byte("package pkg_test\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, selected: every, fallback: "untracked file"},
		"type error": {change: func(t *testing.T, dir string) {
			edit(t, dir, "pkg/a_test.go", "helperA() != \"A\"", "helperA() != 1")
		}, selected: every, fallback: "type-based analysis failed"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := fixture(t)
			test.change(t, dir)
			out := selection(t, dir)
			if got := selected(out); !slices.Equal(got, test.selected) {
				t.Fatalf("selected %v, want %v: %+v", got, test.selected, out)
			}
			if out.Selected != len(test.selected) || out.Total != 4 {
				t.Fatalf("counts %d of %d", out.Selected, out.Total)
			}
			if test.fallback == "" && len(out.Fallback) > 0 || test.fallback != "" &&
				!slices.ContainsFunc(out.Fallback, func(reason string) bool { return strings.Contains(reason, test.fallback) }) {
				t.Fatalf("fallback %q, want %q", out.Fallback, test.fallback)
			}
		})
	}
}

func TestParseDiff(t *testing.T) {
	files, err := parseDiff(`diff --git a/x y.go b/x y.go
index 1..2 100644
--- a/x y.go
+++ b/x y.go
@@ -3 +3,2 @@ func f() {
-a
+b
+c
@@ -10,2 +11,0 @@
-d
-e
diff --git a/new.go b/new.go
new file mode 100644
@@ -0,0 +1 @@
+package p
diff --git a/gone.go b/gone.go
deleted file mode 100644
@@ -1 +0,0 @@
-package p
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 || files[0].path != "x y.go" || files[0].status != 'M' || files[1].status != 'A' || files[2].status != 'D' {
		t.Fatalf("%+v", files)
	}
	if !slices.Equal(files[0].hunks, []hunk{{3, 1, 3, 2}, {10, 2, 11, 0}}) {
		t.Fatalf("%+v", files[0].hunks)
	}
	if _, err := parseDiff("diff --git \"a/q\\tq\" \"b/q\\tq\"\n"); err == nil {
		t.Fatal("a quoted path was accepted")
	}
}

// The declarations of a non-test package named by -with join the graph: a
// change there selects the batches that reach it. A test file of another
// package is never part of the build.
func TestSelectBatchesWithCompanion(t *testing.T) {
	for name, test := range map[string]struct {
		change   func(t *testing.T, dir string)
		with     []string
		selected []string
		fallback string
	}{
		"companion function of one batch": {change: func(t *testing.T, dir string) {
			edit(t, dir, "kit/kit.go", `return "E"`, `return "E" + ""`)
		}, with: []string{"kit"}, selected: []string{"e"}},
		"other companion function": {change: func(t *testing.T, dir string) {
			edit(t, dir, "kit/kit.go", "return 2", "return 1 + 1")
		}, with: []string{"kit"}, selected: []string{"f"}},
		"companion not analysed": {change: func(t *testing.T, dir string) {
			edit(t, dir, "kit/kit.go", "return 2", "return 1 + 1")
		}, fallback: "kit/kit.go: outside pkg"},
		"test file of another package": {change: func(t *testing.T, dir string) {
			edit(t, dir, "other/other_test.go", "func TestOther(t *testing.T) {}", "func TestOther(t *testing.T) { t.Log() }")
		}, with: []string{"kit"}, selected: []string{}},
	} {
		t.Run(name, func(t *testing.T) {
			dir := fixture(t)
			test.change(t, dir)
			out, err := selectBatches(options{root: dir, base: "HEAD", pattern: "./pkg", with: test.with, unionTags: "all",
				goos: runtime.GOOS, goarch: runtime.GOARCH, batches: []batchSpec{
					{ID: "e", Tags: "ci,ci_batch_e"}, {ID: "f", Tags: "ci,ci_batch_f"}}})
			if err != nil {
				t.Fatal(err)
			}
			if test.fallback != "" {
				if len(out.Fallback) == 0 || !strings.Contains(out.Fallback[0], test.fallback) {
					t.Fatalf("fallback %q, want %q", out.Fallback, test.fallback)
				}
				return
			}
			if len(out.Fallback) > 0 {
				t.Fatalf("fallback %q", out.Fallback)
			}
			if got := selected(out); !slices.Equal(got, test.selected) {
				t.Fatalf("selected %v, want %v (%+v)", got, test.selected, out.Batches)
			}
		})
	}
}
