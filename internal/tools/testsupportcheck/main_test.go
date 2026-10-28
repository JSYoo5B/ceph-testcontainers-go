package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a small module with the layout the command checks: a registry
// package, a seam file that sets it, a helper package that reads it, and a
// consumer whose _test.go file imports the helper.
var fixture = map[string]string{
	"go.mod":                    "module example.test/fixture\n\ngo 1.26\n",
	"hookreg/hookreg.go":        "package hookreg\n\nvar Test any\n",
	"core/core.go":              "package core\n\nimport \"example.test/fixture/hookreg\"\n\ntype access struct{}\n\nfunc init() { hookreg.Test = access{} }\n\nfunc Lock() {}\n",
	"helper/helper.go":          "package helper\n\nimport \"example.test/fixture/hookreg\"\n\ntype TestStateView struct{}\n\nfunc TestCluster() any { return hookreg.Test }\n",
	"consumer/consumer.go":      "package consumer\n\nimport \"example.test/fixture/core\"\n\nfunc Use() { core.Lock() }\n",
	"consumer/consumer_test.go": "package consumer\n\nimport (\n\t\"testing\"\n\n\t\"example.test/fixture/helper\"\n)\n\nfunc TestUse(t *testing.T) { _ = helper.TestCluster() }\n",
	"manifest.json": `{
  "test_only_packages": ["helper"],
  "seam_selector": "hookreg.Test",
  "seam_files": ["core/core.go", "hookreg/hookreg.go", "helper/helper.go"],
  "forbidden_types": ["TestStateView", "testState"]
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

func run(t *testing.T, files map[string]string) []string {
	t.Helper()
	root := write(t, files)
	problems, err := Check(root, filepath.Join(root, "manifest.json"), []string{"", "extra"})
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

// A _test.go importer of the helper and a helper that declares Test* names and
// reads the seam pass.
func TestTestFileImporterPasses(t *testing.T) {
	if problems := run(t, fixture); len(problems) != 0 {
		t.Fatalf("a valid layout failed: %q", problems)
	}
}

// Negative control: a non-test file importing the helper fails.
func TestNonTestImporterFails(t *testing.T) {
	files := variant(func(files map[string]string) {
		files["consumer/consumer.go"] = "package consumer\n\nimport \"example.test/fixture/helper\"\n\nfunc Use() { _ = helper.TestCluster() }\n"
	})
	requireProblem(t, run(t, files), "example.test/fixture/consumer", "test-only package helper")
}

// Negative control: a moved symbol reintroduced in an ordinary file fails,
// whichever form it takes.
func TestReintroducedSymbolsFail(t *testing.T) {
	for name, source := range map[string]string{
		"method":   "package core\n\ntype Hooks struct{}\n\nfunc (Hooks) TestCluster() {}\n",
		"function": "package core\n\nfunc TestState() {}\n",
		"type":     "package core\n\ntype TestStateView struct{}\n",
	} {
		files := variant(func(files map[string]string) { files["core/extra.go"] = source })
		t.Run(name, func(t *testing.T) {
			requireProblem(t, run(t, files), "core/extra.go", "which only a _test.go file or a test-only package may contain")
		})
	}
}

// seamAccess is how a file outside the seam files can reach hookreg.Test.
// Each form below must be one violation; the controls after it must be none.
func seamUser(source string) map[string]string {
	return variant(func(files map[string]string) {
		files["consumer/consumer.go"] = source
		files["other/other.go"] = "package other\n\nvar Test = 1\n"
	})
}

const hookregPath = `"example.test/fixture/hookreg"`

// Negative controls: the seam is read from a file that is not listed, by
// every spelling of the import.
func TestSeamAccessOutsideSeamFilesFails(t *testing.T) {
	for name, source := range map[string]string{
		"plain":                "package consumer\n\nimport " + hookregPath + "\n\nfunc Use() any { return hookreg.Test }\n",
		"renamed":              "package consumer\n\nimport reg " + hookregPath + "\n\nfunc Use() any { return reg.Test }\n",
		"dot":                  "package consumer\n\nimport . " + hookregPath + "\n\nfunc Use() any { return Test }\n",
		"first of two aliases": "package consumer\n\nimport (\n\tfirst " + hookregPath + "\n\tsecond " + hookregPath + "\n)\n\nfunc Use() any {\n\t_ = second.Other\n\treturn first.Test\n}\n",
		"last of two aliases":  "package consumer\n\nimport (\n\tfirst " + hookregPath + "\n\tsecond " + hookregPath + "\n)\n\nfunc Use() any {\n\t_ = first.Other\n\treturn second.Test\n}\n",
		"blank dot mix":        "package consumer\n\nimport (\n\t_ " + hookregPath + "\n\treg " + hookregPath + "\n)\n\nfunc Use() any { return reg.Test }\n",
	} {
		t.Run(name, func(t *testing.T) {
			requireProblem(t, run(t, seamUser(source)), "consumer/consumer.go", "seam")
		})
	}
}

// Paired controls: none of these reaches the registration slot.
func TestLookalikesOfTheSeamPass(t *testing.T) {
	for name, source := range map[string]string{
		"renamed import of a different package with a Test member": "package consumer\n\nimport hookreg \"example.test/fixture/other\"\n\nfunc Use() any { return hookreg.Test }\n",
		"local variable named like the package":                    "package consumer\n\nfunc Use() any {\n\thookreg := struct{ Test int }{}\n\treturn hookreg.Test\n}\n",
		"parameter named like the package":                         "package consumer\n\nfunc Use(hookreg struct{ Test int }) any { return hookreg.Test }\n",
		"blank import only":                                        "package consumer\n\nimport _ " + hookregPath + "\n",
		"other member of the seam package":                         "package consumer\n\nimport reg " + hookregPath + "\n\nfunc Use() any { return reg.Other }\n",
	} {
		t.Run(name, func(t *testing.T) {
			files := seamUser(source)
			files["hookreg/hookreg.go"] = "package hookreg\n\nvar Test any\n\nvar Other any\n"
			if problems := run(t, files); len(problems) != 0 {
				t.Fatalf("a lookalike failed: %q", problems)
			}
		})
	}
}

// The import-only boundary of the helper package is by import path, so a
// renamed, dot or blank import of it fails as well.
func TestHelperImportSpellingsFail(t *testing.T) {
	for name, source := range map[string]string{
		"plain":       "package consumer\n\nimport \"example.test/fixture/helper\"\n\nvar _ = helper.TestCluster\n",
		"renamed":     "package consumer\n\nimport h \"example.test/fixture/helper\"\n\nvar _ = h.TestCluster\n",
		"dot":         "package consumer\n\nimport . \"example.test/fixture/helper\"\n\nvar _ = TestCluster\n",
		"blank":       "package consumer\n\nimport _ \"example.test/fixture/helper\"\n",
		"two aliases": "package consumer\n\nimport (\n\ta \"example.test/fixture/helper\"\n\tb \"example.test/fixture/helper\"\n)\n\nvar _ = a.TestCluster\nvar _ = b.TestCluster\n",
	} {
		t.Run(name, func(t *testing.T) {
			files := variant(func(files map[string]string) { files["consumer/consumer.go"] = source })
			requireProblem(t, run(t, files), "example.test/fixture/consumer", "test-only package helper")
		})
	}
}

// The rule would be vacuous when the helper package loses all its files.
func TestMissingHelperPackageFails(t *testing.T) {
	files := variant(func(files map[string]string) {
		delete(files, "helper/helper.go")
		files["consumer/consumer_test.go"] = "package consumer\n"
	})
	requireProblem(t, run(t, files), "test-only package helper has no selected file")
}

func TestNamePrefix(t *testing.T) {
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
		"unknown field": `{"test_only_packages":["helper"],"seam_selector":"a.B","extra":1}`,
		"trailing data": `{"test_only_packages":["helper"],"seam_selector":"a.B"} {}`,
		"empty list":    `{"test_only_packages":[],"seam_selector":"a.B"}`,
		"bad selector":  `{"test_only_packages":["helper"],"seam_selector":"B"}`,
	} {
		files := variant(func(files map[string]string) { files["manifest.json"] = manifest })
		root := write(t, files)
		if _, err := Check(root, filepath.Join(root, "manifest.json"), []string{""}); err == nil {
			t.Errorf("%s: the manifest was accepted", name)
		}
	}
}
