package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The public packages must match the implementation. After changing an
// exported declaration in internal/cluster or internal/multicluster, run
// "go run ./internal/facadegen" from the repository root.
func TestGeneratedFacadesAreCurrent(t *testing.T) {
	root := filepath.Join("..", "..")
	g, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range Facades {
		want, err := g.Generate(pkg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(root, pkg, "api.go"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s/api.go is stale; run go run ./internal/facadegen", pkg)
		}
	}
}
