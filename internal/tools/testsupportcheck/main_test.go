package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a small module that follows the layout the command checks: a
// package with a tagged hook file, an otherwise empty support package and a
// consumer of the hook package.
var fixture = map[string]string{
	"go.mod": "module example.test/fixture\n\ngo 1.26\n",
	"hooks/hooks.go": `package hooks

func Lock() {}
`,
	"hooks/testaccess.go": `//go:build test_support

package hooks

type testState struct{}

type Impl struct{}

func (Impl) TestState() testState { return testState{} }

func TestCluster() {}
`,
	"support/support.go": `//go:build test_support

package support

func Fake() {}
`,
	"consumer/consumer.go": `package consumer

import "example.test/fixture/hooks"

func Use() { hooks.Lock() }
`,
	"manifest.json": `{
  "files": ["hooks/testaccess.go", "support/support.go"],
  "empty_packages": ["support"],
  "forbidden_types": ["testState"]
}
`,
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

func run(t *testing.T, files map[string]string, tagSets ...string) []string {
	t.Helper()
	if len(tagSets) == 0 {
		tagSets = []string{"", "extra"}
	}
	root := write(t, files)
	problems, err := Check(root, filepath.Join(root, "manifest.json"), tagSets)
	if err != nil {
		t.Fatalf("check could not run: %v", err)
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

func TestTaggedSupportIsExcluded(t *testing.T) {
	if problems := run(t, fixture); len(problems) != 0 {
		t.Fatalf("an ordinary build that excludes the support code failed: %q", problems)
	}
}

// Negative control: a test-only file that loses its tag is selected again.
func TestFileThatLosesItsTagFails(t *testing.T) {
	files := variant(func(files map[string]string) {
		files["hooks/testaccess.go"] = strings.Replace(files["hooks/testaccess.go"], "//go:build test_support\n\n", "", 1)
	})
	problems := run(t, files)
	requireProblem(t, problems, "hooks/testaccess.go", "selected by an ordinary build")
	requireProblem(t, problems, "hooks/testaccess.go", "declares method TestState")
	requireProblem(t, problems, "hooks/testaccess.go", "declares func TestCluster")
	requireProblem(t, problems, "hooks/testaccess.go", "declares type testState")
}

// Negative control: a constraint that is not the support tag does not count.
func TestFileGuardedByAnotherTagFails(t *testing.T) {
	files := variant(func(files map[string]string) {
		files["support/support.go"] = strings.Replace(files["support/support.go"], "test_support", "extra", 1)
	})
	problems := run(t, files, "", "extra")
	requireProblem(t, problems, "support/support.go", "selected by an ordinary build")
	requireProblem(t, problems, "support/support.go", "is not selected with the test_support tag")
	requireProblem(t, problems, "package support has the selected file")
}

// Negative control: a new test-only declaration in an untagged file is found
// even though the manifest does not name the file.
func TestUnlistedUntaggedTestDeclarationFails(t *testing.T) {
	files := variant(func(files map[string]string) {
		files["consumer/helper.go"] = "package consumer\n\ntype Fake struct{}\n\nfunc (Fake) TestHook() {}\n"
	})
	requireProblem(t, run(t, files), "consumer/helper.go", "declares method TestHook")
}

// Negative control: an ordinary importer of an empty support package breaks
// the ordinary build and is reported.
func TestImporterOfSupportPackageFails(t *testing.T) {
	files := variant(func(files map[string]string) {
		files["consumer/consumer.go"] = "package consumer\n\nimport \"example.test/fixture/support\"\n\nfunc Use() { support.Fake() }\n"
	})
	requireProblem(t, run(t, files), "consumer", "support")
}

func TestManifestFileThatDoesNotExistFails(t *testing.T) {
	files := variant(func(files map[string]string) {
		delete(files, "support/support.go")
		files["manifest.json"] = strings.Replace(files["manifest.json"], `"empty_packages": ["support"]`, `"empty_packages": []`, 1)
	})
	requireProblem(t, run(t, files), "support/support.go", "does not exist")
}

func TestTagSetWithSupportTagIsRefused(t *testing.T) {
	problems := run(t, fixture, "extra,test_support")
	requireProblem(t, problems, "not an ordinary build")
}

func TestTestNamePrefix(t *testing.T) {
	for name, want := range map[string]bool{
		"Test": true, "TestX": true, "Test_x": true, "Test1": true, "Testing": false, "Tester": false, "test": false,
	} {
		if got := isTestName(name); got != want {
			t.Errorf("isTestName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestManifestIsStrict(t *testing.T) {
	for name, manifest := range map[string]string{
		"unknown field": `{"files":["a.go"],"extra":1}`,
		"trailing data": `{"files":["a.go"]} {}`,
		"empty list":    `{"files":[]}`,
	} {
		files := variant(func(files map[string]string) { files["manifest.json"] = manifest })
		root := write(t, files)
		if _, err := Check(root, filepath.Join(root, "manifest.json"), []string{""}); err == nil {
			t.Errorf("%s: the manifest was accepted", name)
		}
	}
}
