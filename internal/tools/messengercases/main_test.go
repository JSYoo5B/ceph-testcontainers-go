package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeSource(t *testing.T, root, name, source string) {
	t.Helper()
	dir := filepath.Join(root, "internal", "integration")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestIndexParsesDeclarationsWithoutExecutingThem(t *testing.T) {
	root := t.TempDir()
	writeSource(t, root, "case_test.go", `package integration_test
import tt "testing"
func init() { panic("must not execute") }
// func TestComment(t *tt.T) { t.Logf("COMMENT_RECEIPT %s", "fake") }
const decoy = "func TestText(t *tt.T)"
func TestActual(t *tt.T) {
 // t.Logf("COMMENT_MARKER %s", "fake")
 _ = "UNUSED_RECEIPT"
 t.Logf("ACTUAL_RECEIPT %s", "value")
 t.Log("OTHER_RECEIPT", "value")
}
`)
	got, err := index(root)
	want := []testCase{{Source: "internal/integration/case_test.go", Name: "TestActual", Markers: []string{"ACTUAL_RECEIPT", "OTHER_RECEIPT"}}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("index = %+v, %v; want %+v", got, err, want)
	}
}

func TestIndexPreservesAmbiguousDefinitions(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a_test.go", "b_test.go"} {
		writeSource(t, root, name, "package integration_test\nimport \"testing\"\nfunc TestDuplicate(t *testing.T) {}\n")
	}
	got, err := index(root)
	if err != nil || len(got) != 2 || got[0].Name != got[1].Name || got[0].Source == got[1].Source {
		t.Fatalf("ambiguous index = %+v, %v", got, err)
	}
}

func TestIndexRejectsWrongTestSignature(t *testing.T) {
	for _, signature := range []string{"func TestWrong(t *testing.B) {}", "func TestWrong(t testing.T) {}", "func TestWrong() {}", "func TestWrong(t *testing.T) int { return 0 }"} {
		t.Run(signature, func(t *testing.T) {
			root := t.TempDir()
			writeSource(t, root, "case_test.go", "package integration_test\nimport \"testing\"\n"+signature)
			if _, err := index(root); err == nil {
				t.Fatal("invalid signature indexed as a test")
			}
		})
	}
}
