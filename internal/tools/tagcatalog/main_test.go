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

// The units a declaration starts and the names it uses come from Go code
// only: a comment or a string that names a test or a helper is not a call.
func TestReadReproductions(t *testing.T) {
	source := `package a

// TestNative runs against the selected image; TestPure only parses.
func TestPure(t *testing.T) { t.Log("TestNative", ` + "`helper(t)`" + `) /* helper(t) */ }

func TestNative(t *testing.T) {
	repro := harness.StartReproduction(t, "rgw_case", "trigger")
	repro.Absent(t, "trigger", "absent")
}

func (s *caseSuite) TestHost() { helper(s.T()) }

func TestShadow(t *testing.T) { runCase := func() {}; runCase(); start := 1; _ = start }

var start, other = func(t *testing.T) { harness.StartReproduction(t, "rgw_case/host", "trigger") }, 1

// func hidden(t *testing.T) { harness.StartReproduction(t, "rgw_hidden", "trigger") }
`
	f, err := parser.ParseFile(token.NewFileSet(), "a_test.go", source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readReproductions(f)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{}
	for key := range got {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"TestNative", "TestPure", "TestShadow", "caseSuite.TestHost", "other", "start"}) {
		t.Fatalf("declarations %v", keys)
	}
	for key, want := range map[string]reproductionReach{
		"TestPure":           {Starts: []string{}, Names: []string{}, Selectors: []string{"Log"}},
		"TestNative":         {Starts: []string{"rgw_case"}, Names: []string{"harness"}, Selectors: []string{"Absent", "StartReproduction"}},
		"TestShadow":         {Starts: []string{}, Names: []string{}, Selectors: []string{}},
		"caseSuite.TestHost": {Starts: []string{}, Names: []string{"helper"}, Selectors: []string{"T"}},
		"start":              {Starts: []string{"rgw_case/host"}, Names: []string{"harness", "testing"}, Selectors: []string{"StartReproduction", "T"}},
		"other":              {Starts: []string{}, Names: []string{}, Selectors: []string{}},
	} {
		if !slices.Equal(got[key].Starts, want.Starts) || !slices.Equal(got[key].Names, want.Names) || !slices.Equal(got[key].Selectors, want.Selectors) {
			t.Errorf("%s: %+v, want %+v", key, got[key], want)
		}
	}
	for name, body := range map[string]string{
		"not a literal": `func TestA(t *testing.T) { unit := "rgw_case"; harness.StartReproduction(t, unit) }`,
		"no unit":       `func TestA(t *testing.T) { harness.StartReproduction(t) }`,
	} {
		f, err := parser.ParseFile(token.NewFileSet(), "a_test.go", "package a\n"+body, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readReproductions(f); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
