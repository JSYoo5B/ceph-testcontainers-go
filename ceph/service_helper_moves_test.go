package ceph

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// This file guards the service-private helper moves of decision 8 in
// docs/design/shared-harness-boundary.md. The adapters' tenant.go files used to
// name unexported helpers of their service packages; the later rbd/harness,
// cephfs/harness and rgw/harness packages cannot name those. The helpers that
// have no other owner now live in leaf internal packages that import no
// service package. The tests run without Docker.

const modulePath = "github.com/JSYoo5B/ceph-testcontainers-go"

// movedHelpers lists, per service directory, the old private names that were
// removed without an alias and the leaf package that took each one.
var movedHelpers = map[string]map[string]string{
	"rbd":    {"bootstrapKind": "internal/servicekind", "validateResourceName": "rbd/internal/resourcename"},
	"cephfs": {"cephFSBootstrapKind": "internal/servicekind"},
	"rgw":    {"rgwBootstrapKind": "internal/servicekind", "gatewayRegistry": "internal/namedregistry", "gatewaysFrom": "internal/namedregistry"},
}

// leafHelperPackages are the leaf packages and the only module packages each
// may import, so none can import a service package or ceph/harness.
var leafHelperPackages = map[string][]string{
	"internal/servicekind":      {},
	"internal/namedregistry":    {},
	"rbd/internal/resourcename": {"internal/naming"},
}

// declaredIn returns the package-level names that the non-test files of dir declare.
func declaredIn(t *testing.T, dir string) []string {
	t.Helper()
	var all []string
	for name, src := range nonTestSources(t, dir) {
		names, _ := declaredNames(t, name, src)
		all = append(all, names...)
	}
	return all
}

// moduleImports returns the module-internal import paths (without the module
// prefix) of the non-test files in dir.
func moduleImports(t *testing.T, dir string) []string {
	t.Helper()
	set := map[string]bool{}
	for name, src := range nonTestSources(t, dir) {
		f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if rest, ok := strings.CutPrefix(p, modulePath+"/"); ok {
				set[rest] = true
			}
		}
	}
	return sortedKeys(set)
}

func TestMovedServiceHelpersAreGoneFromTheServicePackages(t *testing.T) {
	for service, moved := range movedHelpers {
		declared := declaredIn(t, filepath.Join("..", service))
		for name := range moved {
			if slices.Contains(declared, name) {
				t.Errorf("%s still declares %s; the helper moved without an alias", service, name)
			}
		}
	}
}

// leafViolations returns the imports that the allowed list does not admit.
func leafViolations(imports, allowed []string) []string {
	var bad []string
	for _, imp := range imports {
		if !slices.Contains(allowed, imp) {
			bad = append(bad, imp)
		}
	}
	return bad
}

func TestMovedHelpersLiveInLeafPackagesThatImportNoService(t *testing.T) {
	for pkg, allowed := range leafHelperPackages {
		imports := moduleImports(t, filepath.Join("..", filepath.FromSlash(pkg)))
		if bad := leafViolations(imports, allowed); len(bad) > 0 {
			t.Errorf("%s imports %v; a leaf helper package may import only %v", pkg, bad, allowed)
		}
	}
	// Negative control: a service or ceph import is reported.
	if bad := leafViolations([]string{"rbd", "ceph", "internal/naming"}, []string{"internal/naming"}); !slices.Equal(bad, []string{"rbd", "ceph"}) {
		t.Fatalf("the checker missed a forbidden import: %v", bad)
	}
}

// TestAdapterPrivateReachNamesNoMovedHelper pins the caller table after the
// move: the private names that each tenant.go still names are exactly the
// ones the design note lists as not moved yet.
func TestAdapterPrivateReachNamesNoMovedHelper(t *testing.T) {
	want := map[string][]string{
		"rbd":    {"invalid"},
		"cephfs": {"invalid"},
		"rgw":    {"invalid"},
	}
	for service, names := range want {
		dir := filepath.Join("..", service)
		rest := map[string]string{}
		for name, src := range nonTestSources(t, dir) {
			if name != "tenant.go" {
				rest[name] = src
			}
		}
		adapter := map[string]string{"tenant.go": nonTestSources(t, dir)["tenant.go"]}
		got := privateReach(t, adapter, rest)
		if !slices.Equal(got, names) {
			t.Errorf("%s tenant.go private reach = %v, want %v", service, got, names)
		}
	}
}

// TestRGWGatewayStateIsTheGenericRegistry pins the shape of the rgw gateway
// state: gateways returns the generic registry of internal/namedregistry
// instantiated with the rgw Gateway, and no rgw type keeps a byName map of
// its own. The harness reaches the same registry through the leaf package.
func TestRGWGatewayStateIsTheGenericRegistry(t *testing.T) {
	src := nonTestSources(t, filepath.Join("..", "rgw"))
	if !strings.Contains(src["entry.go"], "func gateways(c *ceph.Container) *namedregistry.Registry[*Gateway]") {
		t.Error("rgw gateways no longer returns *namedregistry.Registry[*Gateway]")
	}
	for name, body := range src {
		if strings.Contains(body, "byName") {
			t.Errorf("%s keeps a private byName map beside the generic registry", name)
		}
	}
	if src["tenant.go"] == "" || !strings.Contains(src["tenant.go"], "namedregistry.From[*Gateway]") {
		t.Error("rgw tenant.go does not reach the gateways through the leaf registry")
	}
}
