package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a small module: a test-only helper package with two exported
// declarations, and a runtime type with two seams of which one says so in its
// comment. Every one that must be classified is.
var fixture = map[string]string{
	"helper/helper.go": "package helper\n\n" +
		"// Counter counts.\n//\n//testdouble:shared-double\ntype Counter struct{ N int }\n\n" +
		"//testdouble:shared-double\nfunc NewCounter() *Counter { return &Counter{} }\n\nfunc internal() {}\n",
	"core/core.go": "package core\n\n" +
		"type Core struct {\n\t// runner replaces the real start in tests.\n\t//\n\t//testdouble:nil-default-hook\n\trunner func()\n" +
		"\t//testdouble:nil-default-hook\n\tdocker func()\n\t// plain is an ordinary field kept for tests that check it.\n\tplain int\n}\n",
	"core/core_test.go":                 "package core\n\ntype fakeRunner struct{}\n",
	"manifest.json":                     `{"test_only_packages": ["helper"]}`,
	"internal/tools/skipped/skipped.go": "package skipped\n\n// Mentions test-only code in its own words.\nvar x int\n",
}

func write(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func variant(change func(files map[string]string)) map[string]string {
	files := map[string]string{}
	for name, content := range fixture {
		files[name] = content
	}
	change(files)
	return files
}

func run(t *testing.T, files map[string]string) []string {
	t.Helper()
	root := write(t, files)
	problems, err := Check(root, filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	return problems
}

func requireProblem(t *testing.T, problems []string, parts ...string) {
	t.Helper()
	for _, problem := range problems {
		matched := true
		for _, part := range parts {
			matched = matched && strings.Contains(problem, part)
		}
		if matched {
			return
		}
	}
	t.Fatalf("no problem contains all of %q: %q", parts, problems)
}

func TestClassifiedSourcePasses(t *testing.T) {
	if problems := run(t, fixture); len(problems) != 0 {
		t.Fatalf("problems %q", problems)
	}
}

func TestUnclassifiedSharedDeclarationFails(t *testing.T) {
	problems := run(t, variant(func(f map[string]string) {
		f["helper/helper.go"] += "\nfunc Extra() {}\n"
	}))
	requireProblem(t, problems, "helper/helper.go#Extra", "exported by a test-only package")
}

func TestUnclassifiedSeamFails(t *testing.T) {
	problems := run(t, variant(func(f map[string]string) {
		f["core/core.go"] = strings.Replace(f["core/core.go"], "\tplain int", "\t// start is a test seam.\n\tstart func()\n\tplain int", 1)
	}))
	requireProblem(t, problems, "core/core.go#Core.start", "says it exists for tests")
}

func TestSeamPhrases(t *testing.T) {
	for _, comment := range []string{"replaced in tests.", "unit tests replace it", "a test-only hook", "kept for the tests", "the unit-test hook"} {
		problems := run(t, variant(func(f map[string]string) {
			f["core/core.go"] = strings.Replace(f["core/core.go"], "\tplain int", "\t// "+comment+"\n\tother func()\n\tplain int", 1)
		}))
		requireProblem(t, problems, "core/core.go#Core.other")
	}
	// An ordinary field whose comment mentions tests inside a longer phrase is
	// not a seam: the base fixture's plain field passes.
}

func TestDirectiveShapeIsStrict(t *testing.T) {
	for name, change := range map[string]func(string) string{
		"unknown class": func(s string) string {
			return strings.Replace(s, "//testdouble:shared-double\nfunc", "//testdouble:fake\nfunc", 1)
		},
		"spaced": func(s string) string {
			return strings.Replace(s, "//testdouble:shared-double\nfunc", "// testdouble: shared-double\nfunc", 1)
		},
		"two classes": func(s string) string {
			return strings.Replace(s, "//testdouble:shared-double\nfunc", "//testdouble:shared-double\n//testdouble:shared-assertion\nfunc", 1)
		},
		"floating": func(s string) string { return s + "\n//testdouble:shared-double\n" },
	} {
		problems := run(t, variant(func(f map[string]string) { f["helper/helper.go"] = change(f["helper/helper.go"]) }))
		if len(problems) == 0 {
			t.Errorf("%s: want a problem", name)
		}
	}
}

func TestDirectiveMarksASeamWithoutAPhrase(t *testing.T) {
	// docker has a directive and no phrase; it passes. Dropping the directive
	// from runner, which has a phrase, fails.
	problems := run(t, variant(func(f map[string]string) {
		f["core/core.go"] = strings.Replace(f["core/core.go"], "\t//\n\t//testdouble:nil-default-hook\n\trunner", "\trunner", 1)
	}))
	requireProblem(t, problems, "core/core.go#Core.runner")
	if len(problems) != 1 {
		t.Fatalf("problems %q, want only runner", problems)
	}
}

func TestDoubleNamedDeclarationOutsideTestOnlyFails(t *testing.T) {
	for _, source := range []string{"type FakeThing struct{}", "type thingMock struct{}", "func newStub() {}", "func SpyOn() {}"} {
		problems := run(t, variant(func(f map[string]string) {
			f["core/extra.go"] = "package core\n\n" + source + "\n"
		}))
		requireProblem(t, problems, "core/extra.go", "named like a test double")
	}
	// The same name in a _test.go file passes.
	if problems := run(t, variant(func(f map[string]string) {
		f["core/extra_test.go"] = "package core\n\ntype FakeThing struct{}\n"
	})); len(problems) != 0 {
		t.Fatalf("_test.go double: problems %q", problems)
	}
}
