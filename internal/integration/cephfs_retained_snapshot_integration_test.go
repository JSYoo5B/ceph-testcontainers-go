//go:build integration && features

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// This executable recipe intentionally composes public Ceph commands for the
// native snapshot-retained state and custom volumes metadata. These are server
// provisioning operations; file I/O below is performed only by a Linux native
// client to verify the fixture. Raw commands must be fenced from outside users.
// After a raw retention/recreation, old typed handles never adopt a new UUID.
func TestCephFSRetainedSnapshotAndMetadataRecipe(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testCephFSRetainedSnapshotAndMetadataRecipe(t, host) })
	}
}

func testCephFSRetainedSnapshotAndMetadataRecipe(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const filesystem, groupName = "retained-recipe", "tenants"
	options := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithCephFS(ceph.CephFSConfig{Name: filesystem})}
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, options...)
	fs := cluster.Filesystems()[0]
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	group, err := fs.CreateSubvolumeGroup(ctx, ceph.CephFSSubvolumeGroupConfig{Name: groupName})
	if err != nil {
		t.Fatal(err)
	}
	source, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: "source", GroupName: groupName, SizeBytes: 8 << 20, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	sourceInfo, err := fs.SubvolumeInfo(ctx, source.Name, source.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, "", "seed", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	command := func(args ...string) []byte {
		t.Helper()
		data, err := cluster.Ceph(ctx, args...)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	metadata := func(name string) map[string]string {
		t.Helper()
		data := command("fs", "subvolume", "metadata", "ls", filesystem, name, "--group_name", groupName)
		var result map[string]string
		if err := json.Unmarshal(data, &result); err != nil || result == nil {
			t.Fatalf("native custom metadata=%s error=%v", data, err)
		}
		return result
	}
	command("fs", "subvolume", "metadata", "set", filesystem, source.Name, "CaseKey", "original-value", "--group_name", groupName)
	if metadata(source.Name)["casekey"] != "original-value" {
		t.Fatal("native metadata keys were not normalized to lower case")
	}
	snapshot, err := fs.CreateSubvolumeSnapshot(ctx, source, "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	data := command("fs", "subvolume", "snapshot", "metadata", "ls", filesystem, source.Name, snapshot.Name, "--group_name", groupName)
	var initialSnapshotMetadata map[string]string
	if err := json.Unmarshal(data, &initialSnapshotMetadata); err != nil || len(initialSnapshotMetadata) != 0 {
		t.Fatal("source custom metadata was copied into a snapshot", string(data), err)
	}
	command("fs", "subvolume", "snapshot", "metadata", "set", filesystem, source.Name, snapshot.Name, "CheckpointLabel", "frozen-label", "--group_name", groupName)
	command("fs", "subvolume", "metadata", "set", filesystem, source.Name, "CaseKey", "new-source-value", "--group_name", groupName)
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, snapshot.Path, "update-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	// Retention removes only the live incarnation and its custom metadata. Ceph
	// keeps a snapshot-retained descriptor, while no live getpath remains.
	command("fs", "subvolume", "rm", filesystem, source.Name, "--group_name", groupName, "--retain-snapshots")
	retained, err := fs.SubvolumeInfo(ctx, source.Name, groupName)
	if err != nil || retained.State != "snapshot-retained" || retained.Path != "" {
		t.Fatalf("native retained descriptor=%+v error=%v", retained, err)
	}
	if _, err := cluster.Ceph(ctx, "fs", "subvolume", "getpath", filesystem, source.Name, "--group_name", groupName); err == nil {
		t.Fatal("retained subvolume exposed a live incarnation path")
	}
	// Native v2 does not allow custom metadata operations in retained state;
	// snapshot list/info/getpath and clone-source operations remain available.
	if _, err := cluster.Ceph(ctx, "fs", "subvolume", "metadata", "ls", filesystem, source.Name, "--group_name", groupName); err == nil {
		t.Fatal("retained native state unexpectedly allowed subvolume metadata")
	}
	if _, err := cluster.Ceph(ctx, "fs", "subvolume", "snapshot", "metadata", "get", filesystem, source.Name, snapshot.Name, "checkpointlabel", "--group_name", groupName); err == nil {
		t.Fatal("retained native state unexpectedly allowed snapshot metadata access")
	}
	if err := fs.ResizeSubvolume(ctx, source, 16<<20); err == nil {
		t.Fatal("typed old handle resized a retained incarnation")
	}
	if clone, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: "unsafe-adoption", GroupName: groupName}); err == nil || clone != nil {
		t.Fatal("typed old snapshot handle adopted raw retained state")
	}
	retainedSnapshotPath := strings.TrimSpace(string(command("fs", "subvolume", "snapshot", "getpath", filesystem, source.Name, snapshot.Name, "--group_name", groupName)))
	command("fs", "subvolume", "snapshot", "clone", filesystem, source.Name, snapshot.Name, "restored", "--group_name", groupName, "--target_group_name", groupName)
	for {
		data := command("fs", "clone", "status", filesystem, "restored", "--group_name", groupName, "--format", "json")
		var status struct {
			Status struct {
				State string `json:"state"`
			} `json:"status"`
		}
		if err := json.Unmarshal(data, &status); err != nil {
			t.Fatal(err)
		}
		if status.Status.State == "complete" {
			break
		}
		if status.Status.State != "pending" && status.Status.State != "in-progress" {
			t.Fatalf("raw retained-snapshot clone did not complete: %s", data)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	restored, err := fs.SubvolumeInfo(ctx, "restored", groupName)
	if err != nil || restored.State != "complete" || restored.Path == source.Path || restored.QuotaBytes != 8<<20 || restored.DataPool != sourceInfo.DataPool || restored.PoolNamespace != sourceInfo.PoolNamespace {
		t.Fatalf("retained snapshot clone native layout=%+v error=%v", restored, err)
	}
	if len(metadata("restored")) != 0 {
		t.Fatal("source custom metadata was copied into the native clone")
	}
	cephFSSubvolumeExec(t, ctx, client, "python3", "-c", cephFSRetainedSnapshotBytes, filesystem, retainedSnapshotPath, restored.Path)
	// Recreating an owned raw retained name can wait for asynchronous trash
	// purging. Retry only that native EAGAIN; never force cleanup or adopt a
	// concurrently created complete descriptor.
	for {
		current, err := fs.SubvolumeInfo(ctx, source.Name, groupName)
		if err != nil || current.State != "snapshot-retained" {
			t.Fatalf("retained source changed before recreation: %+v error=%v", current, err)
		}
		_, err = cluster.Ceph(ctx, "fs", "subvolume", "create", filesystem, source.Name, "--group_name", groupName, "--size", "8388608", "--namespace-isolated")
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "asynchronous purge of subvolume in progress") {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	recreated, err := fs.SubvolumeInfo(ctx, source.Name, groupName)
	if err != nil || recreated.State != "complete" || recreated.Path == source.Path || recreated.CreatedAt == sourceInfo.CreatedAt {
		t.Fatalf("retained name did not receive a fresh native incarnation: %+v error=%v", recreated, err)
	}
	if len(metadata(source.Name)) != 0 {
		t.Fatal("old source custom metadata survived live-incarnation retention")
	}
	label := command("fs", "subvolume", "snapshot", "metadata", "get", filesystem, source.Name, snapshot.Name, "CHECKPOINTLABEL", "--group_name", groupName)
	if strings.TrimSpace(string(label)) != "frozen-label" {
		t.Fatal("independent snapshot metadata was lost on source recreation", string(label))
	}
	for _, mutate := range []func() error{
		func() error { return fs.ResizeSubvolume(ctx, source, 16<<20) },
		func() error { return fs.RemoveSubvolume(ctx, source) },
		func() error { return fs.RemoveSubvolumeSnapshot(ctx, snapshot) },
	} {
		if err := mutate(); err == nil {
			t.Fatal("old typed descriptor changed a raw recreated incarnation")
		}
	}
	command("fs", "subvolume", "snapshot", "rm", filesystem, source.Name, snapshot.Name, "--group_name", groupName)
	// Exercise explicit public metadata overwrite/remove on the new raw
	// incarnation. These operations never touch client POSIX metadata.
	command("fs", "subvolume", "metadata", "set", filesystem, source.Name, "CaseKey", "replacement-value", "--group_name", groupName)
	if metadata(source.Name)["casekey"] != "replacement-value" {
		t.Fatal("native metadata overwrite failed")
	}
	command("fs", "subvolume", "metadata", "rm", filesystem, source.Name, "CASEKEY", "--group_name", groupName)
	if len(metadata(source.Name)) != 0 {
		t.Fatal("native metadata remove failed")
	}
	for _, name := range []string{source.Name, "restored"} {
		command("fs", "subvolume", "rm", filesystem, name, "--group_name", groupName)
	}
	names, err := fs.Subvolumes(ctx, groupName)
	if err != nil || slices.Contains(names, source.Name) || slices.Contains(names, "restored") {
		t.Fatalf("raw retained recipe logical cleanup failed: %v error=%v", names, err)
	}
	if err := fs.RemoveSubvolumeGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	t.Log("public native Ceph recipe: retained snapshot clone bytes/quota/namespace, source recreation UUID fencing, independent custom metadata and explicit logical cleanup passed")
}

const cephFSRetainedSnapshotBytes = `import cephfs, hashlib, json, os, sys, threading
filesystem, snapshot, clone = sys.argv[1:]
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '15')
fs.conf_set('rados_osd_op_timeout', '10')
try:
    fs.mount(filesystem_name=filesystem.encode())
    original = bytes(range(256)) * 2048
    for root in (snapshot, clone):
        fd = fs.open(root + '/data', os.O_RDONLY)
        try:
            assert fs.read(fd, 0, len(original) + 1) == original
        finally:
            fs.close(fd)
    print(json.dumps({'retained_snapshot_and_clone_sha256': hashlib.sha256(original).hexdigest()}))
finally:
    fs.shutdown()
    deadline.cancel()
`
