package override

import (
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// exportedFunctions parses the non-test source of the package and returns the
// name of every exported top-level function. Methods are the operations of a
// handle and are not entry points.
func exportedFunctions(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.IsExported() {
				names = append(names, fn.Name.Name)
			}
		}
	}
	slices.Sort(names)
	return names
}

// zeroArgumentCalls calls every exported function with zero arguments on the
// cluster. A new exported function has to be added here, and
// TestEveryExportedFunctionIsClassified requires it.
func zeroArgumentCalls(t *testing.T, c *ceph.Container) map[string]func() error {
	ctx := t.Context()
	return map[string]func() error{
		"AddBlocklist":       func() error { _, err := AddBlocklist(ctx, c, "", time.Duration(0)); return err },
		"SetDebugLevel":      func() error { _, err := SetDebugLevel(ctx, c, ceph.DebugLevelSetting{}); return err },
		"SetMGRModule":       func() error { _, err := SetMGRModule(ctx, c, "", false); return err },
		"SetFullRatios":      func() error { _, err := SetFullRatios(ctx, c, ceph.FullRatios{}); return err },
		"SetOSDFlag":         func() error { _, err := SetOSDFlag(ctx, c, "", false); return err },
		"SetConfig":          func() error { _, err := SetConfig(ctx, c, ceph.ConfigSetting{}); return err },
		"SetMessengerPolicy": func() error { _, err := SetMessengerPolicy(ctx, c, ceph.MessengerPolicy{}); return err },
	}
}

// TestEveryExportedFunctionIsClassified fails for an exported function of the
// package that has no shared-cluster policy, and for a policy that no
// function uses. An unclassified name is refused by the admission, even on a
// cluster of the test's own.
func TestEveryExportedFunctionIsClassified(t *testing.T) {
	own := testhelper.TestCluster(nil, testhelper.TestConfig{})
	functions := exportedFunctions(t)
	if len(functions) == 0 {
		t.Fatal("the parser found no exported function")
	}
	for _, name := range functions {
		if err := overridehook.Admit(own, "override."+name); err != nil {
			t.Errorf("override.%s has no policy for a cluster of its own: %v", name, err)
		}
		shared := testhelper.TestCluster(nil, testhelper.TestConfig{Shared: true})
		if err := overridehook.Admit(shared, "override."+name); !errors.Is(err, ceph.ErrSharedCluster) {
			t.Errorf("override.%s is not refused on a shared cluster: %v", name, err)
		}
	}
	for _, unclassified := range []string{"override.Unclassified", "AddBlocklist", ""} {
		if err := overridehook.Admit(own, unclassified); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("the unclassified operation %q was admitted: %v", unclassified, err)
		}
	}
	// The table of calls covers exactly the exported functions.
	called := slices.Sorted(maps.Keys(zeroArgumentCalls(t, own)))
	if !slices.Equal(called, functions) {
		t.Errorf("the zero-argument calls %v do not match the exported functions %v", called, functions)
	}
}

// TestExportedFunctionsRefuseSharedClusterBeforeAnyEffect calls every exported
// function with zero arguments on a shared cluster. The refusal comes first,
// so the zero arguments are never validated and no command is run.
func TestExportedFunctionsRefuseSharedClusterBeforeAnyEffect(t *testing.T) {
	native := &blocklistFixture{entries: map[string]time.Time{}}
	c := testhelper.TestCluster(native, testhelper.TestConfig{Shared: true, StartupTimeout: time.Second})
	for name, call := range zeroArgumentCalls(t, c) {
		err := call()
		var op *ceph.OperationError
		if !errors.Is(err, ceph.ErrSharedCluster) || !errors.As(err, &op) || op.Op != "override."+name {
			t.Errorf("override.%s bypassed admission: %v", name, err)
		}
	}
	if native.commands != 0 {
		t.Fatalf("a refused call ran %d commands", native.commands)
	}
}
