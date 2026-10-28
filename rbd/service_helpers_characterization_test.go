package rbd

import (
	"errors"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// These controls pin the behavior of the helpers that the tenant adapter
// shares with the rest of the package, so that moving them into leaf internal
// packages (docs/design/shared-harness-boundary.md, decision 8) is checked
// against the same expectations before and after. They run without Docker.

const resourceNameMessage = "RBD %s name must use letters, digits, underscores, dots or dashes and cannot start with a dot or dash"

func TestRBDBootstrapKindValue(t *testing.T) {
	b, err := newPoolsBootstrap(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Kind(); got != "rbd" {
		t.Fatalf("bootstrap kind = %q, want %q", got, "rbd")
	}
}

// TestRBDInvalidResourceNamesAreRefusedWithTheSameError drives every
// production path that validates a name, with a nil cluster so that a name
// that passed validation would fail differently.
func TestRBDInvalidResourceNamesAreRefusedWithTheSameError(t *testing.T) {
	bad := []string{"", "-lead", ".lead", "with space", "a/b", "tab\t", "café", strings.Repeat("a", 129)}
	paths := map[string]func(name string) error{
		"initPool pool":          func(n string) error { return initPool(t.Context(), nil, n) },
		"listNamespaces pool":    func(n string) error { _, err := listNamespaces(t.Context(), nil, n); return err },
		"createNamespace pool":   func(n string) error { _, err := createNamespace(t.Context(), nil, n, "ns"); return err },
		"createNamespace ns":     func(n string) error { _, err := createNamespace(t.Context(), nil, "pool", n); return err },
		"imageClients pool":      func(n string) error { _, err := imageClients(t.Context(), nil, n, "", "image"); return err },
		"imageClients namespace": func(n string) error { _, err := imageClients(t.Context(), nil, "pool", n, "image"); return err },
		"imageClients image":     func(n string) error { _, err := imageClients(t.Context(), nil, "pool", "", n); return err },
	}
	kindOf := func(path string) string {
		switch {
		case strings.HasSuffix(path, "pool"):
			return "pool"
		case strings.HasSuffix(path, "ns"), strings.HasSuffix(path, "namespace"):
			return "namespace"
		}
		return "image"
	}
	for path, call := range paths {
		for _, name := range bad {
			if path == "imageClients namespace" && name == "" {
				continue // an empty namespace is the default namespace
			}
			err := call(name)
			var op *ceph.OperationError
			if !errors.As(err, &op) || op.Kind != ceph.ErrInvalidRequest || !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Errorf("%s %q: error %v is not an invalid-request OperationError", path, name, err)
				continue
			}
			want := strings.Replace(resourceNameMessage, "%s", kindOf(path), 1)
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s %q: message %q lacks %q", path, name, err.Error(), want)
			}
		}
	}
}

func TestRBDValidResourceNamesPassValidation(t *testing.T) {
	for _, name := range []string{"a", "_", "0", "pool.1", "a-b", "A_b.c-d", strings.Repeat("a", 128)} {
		if err := validateResourceName("pool", name); err != nil {
			t.Errorf("%q refused: %v", name, err)
		}
	}
}
