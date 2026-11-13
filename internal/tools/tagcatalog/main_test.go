package main

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestReadNetworkModes(t *testing.T) {
	for name, test := range map[string]struct {
		source string
		modes  []string
		tests  []string
		fails  bool
	}{
		"none": {source: `func TestA(t *testing.T) {}`, modes: []string{}, tests: []string{}},
		"modes": {source: `func TestA(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") { _ = mode }
}
func TestB(t *testing.T) { harness.NetworkModes(t, "host", "bridge") }
func TestC(t *testing.T) {}`, modes: []string{"host", "bridge"}, tests: []string{"TestA", "TestB"}},
		"different modes": {source: `func TestA(t *testing.T) { harness.NetworkModes(t, "host", "bridge") }
func TestB(t *testing.T) { harness.NetworkModes(t, "bridge") }`, fails: true},
		"not a literal": {source: `func TestA(t *testing.T) { mode := "host"; harness.NetworkModes(t, mode) }`, fails: true},
		"no mode":       {source: `func TestA(t *testing.T) { harness.NetworkModes(t) }`, fails: true},
		"in a helper":   {source: `func helper(t *testing.T) { harness.NetworkModes(t, "host") }`, fails: true},
		// Only the harness helper declares modes; a local function of the
		// same name is an ordinary call.
		"unqualified": {source: `func TestA(t *testing.T) { NetworkModes(t, "host") }`, modes: []string{}, tests: []string{}},
	} {
		t.Run(name, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "a_test.go", "package a\n"+test.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			modes, tests, err := readNetworkModes(f)
			if (err != nil) != test.fails || (!test.fails && (!slices.Equal(modes, test.modes) || !slices.Equal(tests, test.tests))) {
				t.Fatalf("%v, %v, %v", modes, tests, err)
			}
		})
	}
}

// testify suite methods are found by reflection, so the catalog records them
// beside the tests: a method named like a test that takes and returns nothing.
func TestInspectRecordsSuiteCases(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "suite_test.go")
	source := `package x

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type poolSuite struct{ suite.Suite }

func (s *poolSuite) SetupSuite()               {}
func (s *poolSuite) TestCreate()               {}
func (s *poolSuite) TestRemove()               {}
func (s *poolSuite) TestlowerIsNotATest()      {}
func (s *poolSuite) TestWithArg(n int)         {}
func (s *poolSuite) TestWithResult() error     { return nil }
func (s *poolSuite) helper()                   {}

func TestPools(t *testing.T) {
	for _, mode := range []string{"a"} {
		t.Run(mode, func(t *testing.T) { suite.Run(t, &poolSuite{}) })
	}
}
`
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := inspect(dir, path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Tests, []string{"TestPools"}) || !slices.Equal(got.SuiteCases, []string{"poolSuite.TestCreate", "poolSuite.TestRemove"}) ||
		!slices.Equal(got.SuiteRuns["TestPools"], []string{"poolSuite"}) {
		t.Fatalf("tests %v suite cases %v runs %v", got.Tests, got.SuiteCases, got.SuiteRuns)
	}
}

// The catalog lists the units of a file's harness.StartReproduction calls
// as Go code reads them: a call in a comment or a string starts nothing.
func TestReadReproductionUnits(t *testing.T) {
	source := `package a

// harness.StartReproduction(t, "rgw_comment", "trigger")
func TestNative(t *testing.T) {
	repro := harness.StartReproduction(t, "rgw_case", "trigger")
	repro.Absent(t, "trigger", "absent")
	t.Log(` + "`harness.StartReproduction(t, \"rgw_raw\", \"x\")`" + `)
}

func helper(t *testing.T) { _ = harness.StartReproduction(t, "rgw_case/host", "trigger") }

var start = func(t *testing.T) { _ = harness.StartReproduction(t, "rgw_case", "again") }

/* func hidden(t *testing.T) { harness.StartReproduction(t, "rgw_hidden", "trigger") } */
`
	f, err := parser.ParseFile(token.NewFileSet(), "a_test.go", source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	units, err := readReproductionUnits(f)
	if err != nil || !slices.Equal(units, []string{"rgw_case", "rgw_case/host"}) {
		t.Fatalf("units %v, %v", units, err)
	}
	for name, body := range map[string]string{
		"not a literal": `func TestA(t *testing.T) { unit := "rgw_case"; harness.StartReproduction(t, unit) }`,
		"no unit":       `func TestA(t *testing.T) { harness.StartReproduction(t) }`,
	} {
		f, err := parser.ParseFile(token.NewFileSet(), "a_test.go", "package a\n"+body, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readReproductionUnits(f); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
