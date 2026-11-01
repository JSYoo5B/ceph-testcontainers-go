package cephfs

import (
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
)

// TestCopiedFilesystemCannotReserveAnMDSName pins that a value copy of a
// registered Filesystem is not its owner. The copy shares the foundation
// Handle but keeps its own config and MDS roster, so admitting it would give
// one filesystem two independent rosters reserving the same daemon name.
func TestCopiedFilesystemCannotReserveAnMDSName(t *testing.T) {
	fs, _, _, _ := cephFSContextAdmissionFixture()
	copied := *fs
	if fsbase.Of(&copied) == fs.base() || fsbase.Of(&copied).Registered() {
		t.Fatal("a copied descriptor resolved to the registered core")
	}
	if !fs.base().Registered() {
		t.Fatal("the original descriptor lost its registration")
	}

	if _, err := copied.fleet().admitMDSStart(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "filesystem ownership changed") {
		t.Fatalf("copied descriptor admission = %v, want the ownership refusal", err)
	}
	admission, err := fs.fleet().admitMDSStart(t.Context(), nil)
	if err != nil {
		t.Fatalf("original descriptor admission: %v", err)
	}
	if got, want := admission.id, cephFSMDSID(fs.base().Name(), 0); got != want {
		t.Fatalf("original reserved %q, want %q", got, want)
	}
	if copied.mds.roster.issued() != 0 || fs.mds.roster.issued() != 1 {
		t.Fatalf("issued indexes copied=%d original=%d, want 0 and 1", copied.mds.roster.issued(), fs.mds.roster.issued())
	}
	if _, err := copied.fleet().admitMDSStart(t.Context(), nil); err == nil {
		t.Fatal("copied descriptor admitted after the original reserved a name")
	}
}
