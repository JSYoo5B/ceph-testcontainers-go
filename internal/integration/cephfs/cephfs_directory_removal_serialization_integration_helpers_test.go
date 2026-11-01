//go:build all || (integration && multicluster)

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// testCephFSDirectoryRemovalSerialization maps four directories to one mirror
// daemon and delays every packet that the source's MGR sends to its one OSD
// by a second. The mirroring module's remove holds its policy lock while it
// waits for its RADOS write, and a daemon's acknowledgement of another
// directory's acquire or release, which runs on the same librados callback
// strand and takes that lock, then blocks that write's completion: without
// the fixture's admission a second remove sent right after the first stops
// the module, and its ls does not answer. Here a fifth directory is added and
// all five are removed at once, each with one RemoveDirectory and no retry;
// each must return, and afterwards the native policy, read straight from the
// source's CLI, answers and tracks none of them.
func testCephFSDirectoryRemovalSerialization(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	source, destination, _, _ := harness.NewMultiClusterPair(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	initial := []string{"/serial-a", "/serial-b", "/serial-c", "/serial-d"}
	const added = "/serial-e"
	mirror, err := cephfsmirror.Run(ctx, harness.NativeMirrorImage(t, "cephfs", harness.StaticFacts(source).Images.CephFS), cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		DestinationSite: "removal-serialization", Directories: initial, DaemonCount: 1,
	}))
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate removal serialization fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	policy := func(ctx context.Context) ([]string, error) {
		data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", sourceFS.FilesystemName)
		if err != nil {
			return nil, err
		}
		var directories []string
		if err := json.Unmarshal(data, &directories); err != nil || directories == nil {
			return nil, fmt.Errorf("native mirror ls: %v: %.256s", err, data)
		}
		return directories, nil
	}
	state := func(ctx context.Context, directory string) (string, error) {
		data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", sourceFS.FilesystemName, directory)
		if err != nil {
			return "", err
		}
		var status struct {
			State *string `json:"state"`
		}
		if err := json.Unmarshal(data, &status); err != nil {
			return "", fmt.Errorf("native dirmap of %s: %v: %.256s", directory, err, data)
		}
		if status.State == nil {
			return "null", nil
		}
		return *status.State, nil
	}
	mapped, err := harness.Observe(ctx, 3*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) ([]string, bool, error) {
		var states []string
		for _, directory := range initial {
			value, err := state(probeCtx, directory)
			if err != nil {
				return states, false, err
			}
			states = append(states, directory+"="+value)
		}
		return states, !slices.ContainsFunc(states, func(s string) bool { return !strings.HasSuffix(s, "=mapped") }), nil
	})
	if err != nil {
		t.Fatalf("the directories are not mapped: %v, %v", mapped.Value, err)
	}

	daemons, err := harness.ObserveCluster(source).DaemonAddresses(ctx)
	if err != nil || len(daemons.OSDs) != 1 || len(daemons.OSDs[0].Public) == 0 {
		t.Fatalf("the source's one OSD: %+v, %v", daemons.OSDs, err)
	}
	osd := harness.AddressIP(t, daemons.OSDs[0].Public[0].Addr)
	lease, err := fault.ApplyNetworkProfile(ctx, source, clusteraccess.InitialManager(t, source), fault.NetworkProfile{
		HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: osd, Delay: time.Second,
	})
	if lease != nil {
		t.Cleanup(func() {
			restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := lease.Restore(restoreCtx); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}

	addCtx, stop := context.WithTimeout(ctx, 3*time.Minute)
	err = mirror.AddDirectory(addCtx, added)
	stop()
	if err != nil {
		t.Fatal("add a directory with the slow MGR path:", err)
	}
	var timeline []string
	for _, directory := range append(slices.Clone(initial), added) {
		started := time.Now()
		removeCtx, stop := context.WithTimeout(ctx, 3*time.Minute)
		err := mirror.RemoveDirectory(removeCtx, directory)
		stop()
		took := time.Since(started).Round(time.Millisecond)
		if err != nil {
			t.Fatalf("remove %s after %s (earlier: %v): %v", directory, took, timeline, err)
		}
		timeline = append(timeline, fmt.Sprintf("%s %s", directory, took))
	}

	// The module answers, and its policy drains.
	drained, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) ([]string, bool, error) {
		directories, err := policy(probeCtx)
		return directories, err == nil && len(directories) == 0, err
	})
	if err != nil {
		t.Fatalf("the native mirroring policy did not drain: %v, %v", drained.Value, err)
	}
	for _, directory := range append(slices.Clone(initial), added) {
		if value, err := state(ctx, directory); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("native dirmap of removed %s: %q, %v", directory, value, err)
		}
	}
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied {
		t.Fatalf("the MGR's delay: %+v, %v", status, err)
	}
	t.Logf("CEPHFS_MIRROR_REMOVAL_SERIALIZATION %s: MGR to osd %s delayed %s; removals %s; policy drained after %s (%d probes)",
		harness.StaticFacts(source).Images.Essential, osd, time.Second, strings.Join(timeline, ", "), drained.Elapsed.Round(time.Millisecond), drained.Attempts)
}
