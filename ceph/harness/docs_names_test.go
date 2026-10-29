package harness

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// removedHarnessName matches a qualified use of a name that left its old
// package in the move to the harness packages (issue 155) and was removed
// without an alias.
var removedHarnessName = regexp.MustCompile(
	`\bceph\.(NewShared|RunTests|SharedCluster|SharedOption|LeaseOption|SharedStats|WithSharedRunOptions|WithMaxTenants|WithMaxRecycles|WithKeepOnTenantFailure|ObserveTenant|SharedPool|Tenant|TenantAccess|TenantReadOnly|TenantReadWrite)\b` +
		`|\b(rbd|cephfs|rgw)\.(Lease|TenantNamespace|TenantGroup|TenantUser)\b`)

// staleDocNames returns the removed names that text still uses.
func staleDocNames(text string) []string {
	return removedHarnessName.FindAllString(text, -1)
}

// TestDocsNameNoRemovedHarnessName fails when a package page, the README or an
// example page still names a harness API under its old package. The examples in
// examples_test.go compile the current spelling; this scan covers the prose and
// the signature lists that no compiler reads.
func TestDocsNameNoRemovedHarnessName(t *testing.T) {
	root := filepath.Join("..", "..")
	pages, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) < 8 {
		t.Fatalf("found %d package pages, want the package pages of docs/", len(pages))
	}
	pages = append(pages, filepath.Join(root, "README.md"))
	for _, page := range pages {
		b, err := os.ReadFile(page)
		if err != nil {
			t.Fatal(err)
		}
		if stale := staleDocNames(string(b)); len(stale) > 0 {
			t.Errorf("%s names removed harness API %q; use the ceph/harness spelling", filepath.Base(page), stale)
		}
	}
}

// Negative control: the scan reports each old spelling and accepts the new.
func TestStaleDocNamesScanSeesTheOldSpelling(t *testing.T) {
	// Built from parts so that this file does not itself spell a removed name.
	pkg := "ce" + "ph"
	old := "shared := " + pkg + ".NewShared(img)\nos.Exit(" + pkg + ".RunTests(m, shared))\nrb" + "d.Lease(ctx, tenant, \"p\")\n*" + pkg + ".Tenant"
	if got := staleDocNames(old); len(got) != 4 {
		t.Fatalf("scan found %q, want four old names", got)
	}
	current := "cephharness.NewShared(img)\ncephharness.RunTests(m, s)\nrbdharness.Lease(ctx, tenant, \"p\")\n*cephharness.Tenant\nceph.ErrSharedCluster"
	if got := staleDocNames(current); len(got) != 0 {
		t.Fatalf("scan rejected the current spelling: %q", got)
	}
}
