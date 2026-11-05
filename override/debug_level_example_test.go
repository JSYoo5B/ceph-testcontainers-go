package override_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// An MDS window around one CephFS operation. The receipt names the GID of the
// MDS process the level was set in; if that MDS respawns, for example after
// ceph mds fail, Restore reports target-changed and sets nothing. This
// example is compiled, not run.
func ExampleSetDebugLevel_mds() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage, cephfs.WithFilesystems(cephfs.Config{}))
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	fs := cephfs.Filesystems(cluster.Container)[0]
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		panic(err)
	}
	window, err := override.SetDebugLevel(ctx, cluster.Container, override.DebugLevelSetting{
		Daemon: "mds." + status.Active[0].Name, Name: "debug_mds", Level: "10",
	})
	// The handle is never nil; restore it with a context of its own.
	defer func() { _ = window.Restore(context.Background()) }()
	if err != nil {
		panic(err)
	}
	// Run the one CephFS operation to diagnose here, then restore and keep
	// the receipt with the test's own evidence.
	if err := window.Restore(context.Background()); err != nil {
		panic(err)
	}
	receipt, _ := json.Marshal(window.Receipt())
	fmt.Println(string(receipt))
}
