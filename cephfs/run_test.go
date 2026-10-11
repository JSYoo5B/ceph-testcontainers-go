package cephfs_test

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// The service default joins the composition that Run validates before it
// creates any network or container, so a cluster without OSDs is refused.
func TestRunRequiresStorageForItsDefaultService(t *testing.T) {
	cluster, err := cephfs.Run(context.Background(), ceph.DefaultImage, ceph.WithNoInitialOSDs())
	if err == nil || cluster != nil {
		t.Fatalf("Run accepted a cluster without storage: %v", err)
	}
}
