package main

import (
	"go/parser"
	"go/token"
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
