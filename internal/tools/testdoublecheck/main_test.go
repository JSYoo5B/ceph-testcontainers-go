package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const inventory = "## Shared test-support packages\n\n" +
	"| Declarations | Role |\n| --- | --- |\n" +
	"| `helper/helper.go#Counter`, `helper/helper.go#NewCounter` | shared double |\n\n" +
	"## Runtime injection seams\n\n" +
	"| Declarations | Role |\n| --- | --- |\n" +
	"| `core/core.go#Core.runner`, `core/core.go#Core.docker` | nil-default hook |\n"

// fixture is a small module: a test-only helper package with two exported
// declarations, and a runtime type with two seams of which one says so in its
// comment.
var fixture = map[string]string{
	"helper/helper.go": "package helper\n\ntype Counter struct{ N int }\n\nfunc NewCounter() *Counter { return &Counter{} }\n\nfunc internal() {}\n",
	"core/core.go": "package core\n\n" +
		"type Core struct {\n\t// runner replaces the real start in tests.\n\trunner func()\n\tdocker func()\n\t// plain is an ordinary field kept for tests that check it.\n\tplain int\n}\n",
	"core/core_test.go":                 "package core\n\ntype fakeRunner struct{}\n",
	"inventory.md":                      inventory,
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

func run(t *testing.T, files map[string]string) ([]string, error) {
	t.Helper()
	root := write(t, files)
	return Check(root, filepath.Join(root, "inventory.md"), filepath.Join(root, "manifest.json"))
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

func TestMatchingInventoryPasses(t *testing.T) {
	problems, err := run(t, fixture)
	if err != nil || len(problems) != 0 {
		t.Fatalf("problems %q, error %v", problems, err)
	}
}

func TestUnlistedSharedDeclarationFails(t *testing.T) {
	problems, err := run(t, variant(func(f map[string]string) {
		f["helper/helper.go"] += "\nfunc Extra() {}\n"
	}))
	if err != nil {
		t.Fatal(err)
	}
	requireProblem(t, problems, "helper/helper.go#Extra", "Shared test-support packages")
}

func TestUnlistedSeamFails(t *testing.T) {
	problems, err := run(t, variant(func(f map[string]string) {
		f["core/core.go"] = strings.Replace(f["core/core.go"], "\tplain int", "\t// start is a test seam.\n\tstart func()\n\tplain int", 1)
	}))
	if err != nil {
		t.Fatal(err)
	}
	requireProblem(t, problems, "core/core.go#Core.start", "Runtime injection seams")
}

func TestSeamPhrases(t *testing.T) {
	for _, comment := range []string{"replaced in tests.", "unit tests replace it", "a test-only hook", "kept for the tests", "the unit-test hook"} {
		problems, err := run(t, variant(func(f map[string]string) {
			f["core/core.go"] = strings.Replace(f["core/core.go"], "\tplain int", "\t// "+comment+"\n\tother func()\n\tplain int", 1)
		}))
		if err != nil {
			t.Fatal(err)
		}
		requireProblem(t, problems, "core/core.go#Core.other")
	}
	// An ordinary field whose comment mentions tests inside a longer phrase is
	// not a seam: the base fixture's plain field passes.
}

func TestStaleRowsFail(t *testing.T) {
	problems, err := run(t, variant(func(f map[string]string) {
		f["inventory.md"] = strings.Replace(f["inventory.md"], "`core/core.go#Core.docker`", "`core/core.go#Core.gone`", 1)
		f["helper/helper.go"] = strings.Replace(f["helper/helper.go"], "NewCounter", "MakeCounter", 1)
	}))
	if err != nil {
		t.Fatal(err)
	}
	requireProblem(t, problems, "Runtime injection seams", "core/core.go#Core.gone", "no non-test file declares")
	requireProblem(t, problems, "Shared test-support packages", "helper/helper.go#NewCounter", "no non-test file declares")
}

func TestDoubleNamedDeclarationOutsideTestOnlyFails(t *testing.T) {
	for _, source := range []string{"type FakeThing struct{}", "type thingMock struct{}", "func newStub() {}", "func SpyOn() {}"} {
		problems, err := run(t, variant(func(f map[string]string) {
			f["core/extra.go"] = "package core\n\n" + source + "\n"
		}))
		if err != nil {
			t.Fatal(err)
		}
		requireProblem(t, problems, "core/extra.go", "named like a test double")
	}
	// The same name in a _test.go file or in a test-only package passes.
	problems, err := run(t, variant(func(f map[string]string) {
		f["core/extra_test.go"] = "package core\n\ntype FakeThing struct{}\n"
	}))
	if err != nil || len(problems) != 0 {
		t.Fatalf("_test.go double: problems %q, error %v", problems, err)
	}
}

func TestInventoryShapeIsStrict(t *testing.T) {
	for name, text := range map[string]string{
		"missing seam section":   "## Shared test-support packages\n| `helper/helper.go#Counter` | x |\n",
		"missing shared section": "## Runtime injection seams\n| `core/core.go#Core.runner` | x |\n",
		"empty tables":           "## Shared test-support packages\n\n## Runtime injection seams\n",
	} {
		if _, err := run(t, variant(func(f map[string]string) { f["inventory.md"] = text })); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestSymbolsInLaterColumnsDoNotCount(t *testing.T) {
	problems, err := run(t, variant(func(f map[string]string) {
		f["helper/helper.go"] += "\nfunc Extra() {}\n"
		f["inventory.md"] = strings.Replace(f["inventory.md"], "| shared double |", "| shared double, see `helper/helper.go#Extra` |", 1)
	}))
	if err != nil {
		t.Fatal(err)
	}
	requireProblem(t, problems, "helper/helper.go#Extra", "has no row")
}
