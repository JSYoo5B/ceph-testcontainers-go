//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestCephFSFilesystem(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t)
	fs, err := cluster.StartCephFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	assertCephFSActive(t, ctx, cluster, fs.FilesystemName)
	// libcephfs creates a userspace client session. This is real CephFS I/O,
	// with metadata served by the MDS and file data read/written through OSDs.
	// No /dev/fuse, CAP_SYS_ADMIN or kernel filesystem support is involved.
	execCommand(t, ctx, client, "python3", "-c", cephFSClientScript, fs.FilesystemName, "seed")
	t.Log("CephFS independent client: mkdir, 16 file writes/fsync/reads/renames, temporary file unlink")
	advanceServiceTopology(t, ctx, cluster)
	assertCephFSActive(t, ctx, cluster, fs.FilesystemName)
	// A new Python process establishes a new libcephfs session so retained
	// bytes cannot come from the first session's file cache.
	execCommand(t, ctx, client, "python3", "-c", cephFSClientScript, fs.FilesystemName, "after-topology")
	t.Log("CephFS fresh session after OSD replacement: all retained bytes verified, new file write/read/rename/unlink")
	execCommand(t, ctx, client, "python3", "-c", cephFSClientScript, fs.FilesystemName, "final")
	t.Log("CephFS third session verified post-topology writes and deletion visibility")
}

func assertCephFSActive(t *testing.T, ctx context.Context, cluster *ceph.Container, name string) {
	t.Helper()
	data, err := cluster.Ceph(ctx, "fs", "get", name, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		MDSMap struct {
			Info map[string]struct {
				Rank  int    `json:"rank"`
				State string `json:"state"`
			} `json:"info"`
		} `json:"mdsmap"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	for _, info := range result.MDSMap.Info {
		if info.Rank == 0 && info.State == "up:active" {
			t.Logf("CephFS %s: rank 0 MDS up:active", name)
			return
		}
	}
	t.Fatalf("CephFS has no active rank zero: %s", data)
}

const cephFSClientScript = `import cephfs
import os
import sys

filesystem_name, phase = sys.argv[1:]
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem_name.encode())

def expected(index):
    return b'testcontainers-cephfs-roundtrip\n' * 2048 + bytes(range(256)) + str(index).encode()

def write(path, data):
    fd = fs.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        assert fs.write(fd, data, 0) == len(data), 'short write: ' + path
        fs.fsync(fd, False)
    finally:
        fs.close(fd)

def verify(path, data):
    fd = fs.open(path, os.O_RDONLY)
    try:
        actual = fs.read(fd, 0, len(data) + 1)
        assert actual == data, 'payload mismatch: ' + path
    finally:
        fs.close(fd)

def absent(path):
    try:
        fs.stat(path)
    except cephfs.ObjectNotFound:
        return
    raise AssertionError('deleted path exists: ' + path)

try:
    if phase == 'seed':
        fs.mkdir('/tc-cephfs', 0o700)
        for i in range(16):
            path = '/tc-cephfs/file-%02d' % i
            write(path, expected(i))
            verify(path, expected(i))
            fs.rename(path, path + '.renamed')
            absent(path)
        write('/tc-cephfs/temporary', b'unlink-test')
        verify('/tc-cephfs/temporary', b'unlink-test')
        fs.unlink('/tc-cephfs/temporary')
        absent('/tc-cephfs/temporary')
    else:
        for i in range(15 if phase == 'final' else 16):
            verify('/tc-cephfs/file-%02d.renamed' % i, expected(i))
        absent('/tc-cephfs/temporary')
        if phase == 'after-topology':
            fs.mkdir('/tc-cephfs/after-topology', 0o700)
            write('/tc-cephfs/after-topology/new', b'cephfs-after-osd-replacement')
            verify('/tc-cephfs/after-topology/new', b'cephfs-after-osd-replacement')
            fs.rename('/tc-cephfs/after-topology/new', '/tc-cephfs/retained-after-topology')
            fs.rmdir('/tc-cephfs/after-topology')
            fs.unlink('/tc-cephfs/file-15.renamed')
        elif phase == 'final':
            verify('/tc-cephfs/retained-after-topology', b'cephfs-after-osd-replacement')
            absent('/tc-cephfs/file-15.renamed')
            absent('/tc-cephfs/after-topology')
    fs.sync_fs()
finally:
    fs.shutdown()
`
