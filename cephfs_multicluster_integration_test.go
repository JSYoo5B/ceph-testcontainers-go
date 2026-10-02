//go:build integration && multicluster

package ceph_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/jsyoo5b/ceph-testcontainers-go/federation"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// This exercises two separate mechanisms: an application snapshot archive and
// the native cephfs-mirror daemon. The archive is deliberately a small fixture
// format, not a general-purpose filesystem backup utility.
func TestMultiClusterCephFSSnapshotMirrorAndBackup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	sourceFS, err := source.StartCephFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := destination.StartCephFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(cephFSInterClusterScript), "/tmp/cephfs-intercluster.py", 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fsCommand := func(client testcontainers.Container, filesystem string, args ...string) {
		t.Helper()
		command := append([]string{"python3", "/tmp/cephfs-intercluster.py", filesystem}, args...)
		output := multiClusterExecOutput(t, ctx, client, command...)
		if summary := strings.TrimSpace(string(output)); summary != "" {
			t.Logf("CephFS %s: %s", args[0], summary)
		}
	}
	fsCommand(sourceClient, sourceFS.FilesystemName, "seed")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-1", "/tmp/cephfs-backup-1.json")
	archive := multiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-1.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-1.json", 0o600); err != nil {
		t.Fatal(err)
	}
	fsCommand(destinationClient, destinationFS.FilesystemName, "restore", "/restored", "/tmp/cephfs-backup-1.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify", "/restored", "/tmp/cephfs-backup-1.json")
	t.Log("CephFS snapshot backup: restored binary/empty/nested files, relative symlink, modes, owners and user xattrs into an independent filesystem")

	// Cluster/filesystem setup above stays separate from federation setup.
	// Select a compatible runtime supplying the userspace mirror daemon.
	mirrorImage := os.Getenv("CEPH_TEST_MIRROR_IMAGE")
	if mirrorImage == "" {
		mirrorImage, _ = integrationImages(t)
	}
	mirror, err := federation.RunCephFSMirror(ctx, mirrorImage, federation.CephFSMirrorConfig{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: []string{"/federation"},
	})
	if mirror != nil {
		// A partial setup may have an owned network attachment but no daemon.
		// Federation cleanup closes both before the pair's cluster cleanup.
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && mirror.Container != nil {
				multiClusterLogContainer(t, cleanupCtx, mirror.Container)
			}
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate CephFS federation: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-1", true)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-1", "/tmp/cephfs-backup-1.json")
	t.Log("native cephfs-mirror: first snapshot reached the destination; content, names, symlink, modes and owners checked separately from user xattrs")

	stopTimeout := 5 * time.Second
	if err := mirror.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	fsCommand(sourceClient, sourceFS.FilesystemName, "mutate")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	archive = multiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-2.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-2.json", 0o600); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-2", false)
	if err := mirror.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-2", true)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-1", "/tmp/cephfs-backup-1.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	t.Log("native cephfs-mirror restart: caught up changed bytes, deleted file, renamed file and new file; old snapshot retained its original data; user xattr differences reported explicitly")
	fsCommand(sourceClient, sourceFS.FilesystemName, "remove-snapshot", "backup-1")
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-1", false)
	t.Log("native CephFS snapshot deletion propagated to the destination")
	advanceServiceTopology(t, ctx, destination)

	if err := mirror.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	stopMultiClusterSource(t, ctx, source)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify", "/restored", "/tmp/cephfs-backup-1.json")
	t.Log("source MON/MDS/OSDs stopped: fresh destination userspace sessions still read the mirrored snapshot and separately restored backup after destination OSD replacement")
}

func cephFSWaitForRemoteSnapshot(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, snapshot string, present bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	expectation := "absent"
	if present {
		expectation = "present"
	}
	var last string
	for {
		code, r, err := client.Exec(waitCtx, []string{"python3", "/tmp/cephfs-intercluster.py", filesystem, "snapshot-state", snapshot, expectation}, tcexec.Multiplexed())
		if err != nil {
			last = err.Error()
		} else {
			out, err := io.ReadAll(r)
			last = string(out)
			if err == nil && code == 0 {
				return
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("CephFS remote snapshot %s did not become %s: %s", snapshot, expectation, last)
		case <-time.After(time.Second):
		}
	}
}

const cephFSInterClusterScript = `import base64
import cephfs
import hashlib
import json
import os
import stat
import sys

filesystem, phase, *args = sys.argv[1:]
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem.encode())

def write(path, data, mode=0o640):
    fd = fs.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, mode)
    try:
        # libcephfs rejects a zero-length write with EINVAL. O_CREAT/O_TRUNC
        # still creates the actual empty file; fsync covers it below.
        if data:
            assert fs.write(fd, data, 0) == len(data), 'short write: ' + path
        fs.fsync(fd, False)
    finally:
        fs.close(fd)

def read(path, size):
    fd = fs.open(path, os.O_RDONLY)
    try:
        data = fs.read(fd, 0, size + 1)
        assert len(data) == size, 'short read: ' + path
        return data
    finally:
        fs.close(fd)

def children(path):
    names = []
    with fs.opendir(path) as directory:
        while True:
            entry = fs.readdir(directory)
            if entry is None:
                break
            if entry.d_name not in (b'.', b'..', b'.snap'):
                names.append(entry.d_name.decode('utf-8'))
    return sorted(names)

def capture(root):
    entries = []
    def visit(relative):
        path = root + ('/' + relative if relative else '')
        st = fs.stat(path, follow_symlink=False)
        item = {'path': relative, 'mode': stat.S_IMODE(st.st_mode), 'uid': st.st_uid, 'gid': st.st_gid}
        if stat.S_ISDIR(st.st_mode):
            item['kind'] = 'directory'
        elif stat.S_ISREG(st.st_mode):
            item['kind'] = 'file'
            data = read(path, st.st_size)
            item['data'] = base64.b64encode(data).decode('ascii')
            item['sha256'] = hashlib.sha256(data).hexdigest()
        elif stat.S_ISLNK(st.st_mode):
            item['kind'] = 'symlink'
            item['target'] = fs.readlink(path, 4096).decode('utf-8')
        else:
            raise AssertionError('unsupported fixture type: ' + path)
        # Preserve user attributes only. Ceph virtual layout/quota attributes
        # belong to a filesystem's configuration and are outside this fixture.
        item['xattrs'] = {}
        if item['kind'] != 'symlink':
            _, keys = fs.listxattr(path)
            for key in keys.split(b'\x00'):
                if key.startswith(b'user.'):
                    item['xattrs'][key.decode()] = base64.b64encode(fs.getxattr(path, key, 65536)).decode('ascii')
        entries.append(item)
        if item['kind'] == 'directory':
            for child in children(path):
                visit(relative + '/' + child if relative else child)
    visit('')
    return entries

def load_archive(path):
    with open(path) as reader:
        archive = json.load(reader)
    assert archive['format'] == 'testcontainers-cephfs-fixture-v1'
    for item in archive['entries']:
        relative = item['path']
        assert not relative.startswith('/') and '..' not in relative.split('/'), 'invalid archive path'
        if item['kind'] == 'file':
            data = base64.b64decode(item['data'], validate=True)
            assert hashlib.sha256(data).hexdigest() == item['sha256'], 'archive digest mismatch'
    return archive['entries']

try:
    if phase == 'seed':
        fs.mkdir('/federation', 0o750)
        fs.mkdir('/federation/nested', 0o700)
        fs.mkdir('/federation/nested/deeper', 0o750)
        write('/federation/nested/deeper/자료.bin', bytes(range(256)) * 512, 0o600)
        write('/federation/empty', b'', 0o600)
        write('/federation/space name.txt', b'CephFS intercluster snapshot backup\n')
        write('/federation/remove-me', b'present only in the first snapshot')
        write('/federation/rename-me', b'content survives rename')
        fs.symlink('nested/deeper/자료.bin', '/federation/binary-link')
        fs.setxattr('/federation', 'user.testcontainers', b'root-metadata', 0)
        fs.setxattr('/federation/nested/deeper/자료.bin', 'user.testcontainers', b'binary-metadata-v1\x00\xff', 0)
        fs.chown('/federation/space name.txt', 1234, 2345)
        fs.chmod('/federation/space name.txt', 0o640)
        fs.sync_fs()
        fs.mkdir('/federation/.snap/backup-1', 0o755)
    elif phase == 'mutate':
        write('/federation/nested/deeper/자료.bin', b'updated snapshot payload\n' * 3000, 0o600)
        fs.setxattr('/federation/nested/deeper/자료.bin', 'user.testcontainers', b'binary-metadata-v2', 0)
        fs.unlink('/federation/remove-me')
        fs.rename('/federation/rename-me', '/federation/renamed')
        write('/federation/new-after-outage', b'written while mirror was stopped', 0o600)
        fs.sync_fs()
        fs.mkdir('/federation/.snap/backup-2', 0o755)
    elif phase == 'archive':
        root, archive_path = args
        entries = capture(root)
        with open(archive_path, 'w') as writer:
            json.dump({'format': 'testcontainers-cephfs-fixture-v1', 'entries': entries}, writer, sort_keys=True)
        print(json.dumps({'entries': len(entries), 'files': sum(e['kind'] == 'file' for e in entries), 'action': 'archived'}))
    elif phase == 'restore':
        root, archive_path = args
        entries = load_archive(archive_path)
        for item in entries:
            path = root + ('/' + item['path'] if item['path'] else '')
            if item['kind'] == 'directory':
                fs.mkdir(path, 0o700)
            elif item['kind'] == 'file':
                write(path, base64.b64decode(item['data']), 0o600)
            elif item['kind'] == 'symlink':
                fs.symlink(item['target'], path)
            fs.chown(path, item['uid'], item['gid'], follow_symlink=False)
            if item['kind'] != 'symlink':
                fs.chmod(path, item['mode'])
                for key, value in item['xattrs'].items():
                    fs.setxattr(path, key, base64.b64decode(value), 0)
        fs.sync_fs()
    elif phase in ('verify', 'verify-mirror'):
        root, archive_path = args
        expected = load_archive(archive_path)
        actual = capture(root)
        xattr_differences = []
        if phase == 'verify-mirror':
            # Native snapshot mirroring and this application backup are
            # different mechanisms. Report native user-xattr preservation
            # independently instead of silently claiming archive semantics.
            assert [e['path'] for e in actual] == [e['path'] for e in expected], 'mirrored paths differ'
            for observed, wanted in zip(actual, expected):
                if observed['xattrs'] != wanted['xattrs']:
                    xattr_differences.append(wanted['path'] or '.')
                del observed['xattrs']
                del wanted['xattrs']
        if actual != expected:
            assert [e['path'] for e in actual] == [e['path'] for e in expected], 'snapshot/restore paths differ at ' + root
            for observed, wanted in zip(actual, expected):
                if observed != wanted:
                    changed = sorted(k for k in set(observed) | set(wanted) if observed.get(k) != wanted.get(k))
                    # Report the exact entry and fields, without dumping the
                    # binary fixture's base64 payload into integration logs.
                    raise AssertionError('snapshot/restore mismatch at ' + root + '/' + wanted['path'] + ': ' + ','.join(changed))
        print(json.dumps({'entries': len(actual), 'files': sum(e['kind'] == 'file' for e in actual), 'action': 'verified', 'user_xattr_difference_paths': xattr_differences}))
    elif phase == 'snapshot-state':
        name, expectation = args
        try:
            fs.stat('/federation/.snap/' + name)
            present = True
        except cephfs.ObjectNotFound:
            present = False
        assert present == (expectation == 'present'), 'snapshot state still pending'
    elif phase == 'remove-snapshot':
        fs.rmdir('/federation/.snap/' + args[0])
    else:
        raise AssertionError('unknown phase: ' + phase)
finally:
    fs.shutdown()
`
