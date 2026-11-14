//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_mds_requests_json)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_mds_requests_json_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.5-all"

const probe = `import errno
import fcntl
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

import cephfs


def mount(stack, socket):
    fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
    fs.conf_set("admin_socket", str(socket))
    fs.conf_set("client_mount_timeout", "30")
    fs.mount(filesystem_name=sys.argv[1].encode())
    stack.callback(fs.shutdown)
    return fs


def dump(socket):
    return subprocess.check_output(
        ["ceph", "--admin-daemon", str(socket), "mds_requests"], timeout=10,
    ).decode()


class Members(list):
    """Keep duplicate object members while traversing the raw reply."""


def requests(node):
    if isinstance(node, (Members, dict)):
        fields = dict(node)
        if fields.get("op") == "setfilelock":
            yield int(fields["ino"], 0), fields["tid"]
        children = (v for k, v in node) if isinstance(node, Members) else node.values()
    elif isinstance(node, list):
        children = node
    else:
        return
    for child in children:
        yield from requests(child)


def observe(socket, expected):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        raw = dump(socket)
        seen = list(requests(json.loads(raw, object_pairs_hook=Members)))
        if (len(seen) == len(expected) == 2 and {ino for ino, tid in seen} == expected
                and len({tid for ino, tid in seen}) == 2):
            return raw, set(seen)
        time.sleep(0.2)
    raise RuntimeError("two identifiable requests did not reach the client dump: " + raw)


with TemporaryDirectory() as run, ExitStack() as stack:
    socket = Path(run, "actor.asok")
    holder = mount(stack, Path(run, "holder.asok"))
    actor = mount(stack, socket)
    assert holder.get_instance_id() != actor.get_instance_id()
    baseline = dump(socket)
    assert not list(requests(json.loads(baseline)))
    files = []
    for i in range(2):
        path = ("/mds-requests-%d" % i).encode()
        owner_fd = holder.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        stack.callback(holder.unlink, path)
        stack.callback(holder.close, owner_fd)
        fd = actor.open(path, os.O_RDWR, 0)
        stack.callback(actor.close, fd)
        holder.flock(owner_fd, fcntl.LOCK_EX, 10 + i)
        try:
            actor.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 20 + i)
        except cephfs.Error as error:
            if error.errno not in (errno.EACCES, errno.EAGAIN):
                raise
        else:
            raise RuntimeError("contention control unexpectedly acquired lock")
        files.append((owner_fd, fd, holder.stat(path).st_ino))

    with ThreadPoolExecutor(max_workers=2) as pool, ExitStack() as locks:
        waiters = []
        for i, (owner_fd, fd, inode) in enumerate(files):
            locks.callback(holder.flock, owner_fd, fcntl.LOCK_UN, 10 + i)
            waiters.append(pool.submit(actor.flock, fd, fcntl.LOCK_EX, 20 + i))
        raw, lossless = observe(socket, {f[2] for f in files})
        ordinary = set(requests(json.loads(raw)))
        assert all(not future.done() for future in waiters)
        for i, (owner_fd, fd, inode) in enumerate(files):
            holder.flock(owner_fd, fcntl.LOCK_UN, 10 + i)
            waiters[i].result(timeout=15)
            if i == 0:
                assert not waiters[1].done()
            actor.flock(fd, fcntl.LOCK_UN, 20 + i)
        locks.pop_all()
    after = dump(socket)
    assert not list(requests(json.loads(after)))
    assert ordinary <= lossless
    receipt = dict(raw=raw, ordinary=sorted(ordinary), lossless=sorted(lossless),
                   missing=sorted(lossless - ordinary), baseline=baseline, after=after)

print(json.dumps(receipt))
`

// TestCephFSConcurrentMDSRequestsJSON holds two distinct file locks and reads
// the waiting client's admin socket. Each blocked request must finish when its
// own lock is released. Ordinary JSON decoding must retain both identities.
func TestCephFSConcurrentMDSRequestsJSON(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := harness.NewSingleImageCluster(t, harness.CEPHFSRun, image, harness.FunctionalOSDConfig(t))
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	run, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv:    []string{"python3", "-c", probe, fs.FilesystemName},
		Timeout: 2 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 8 << 10, Label: "observe waiting client file locks",
	})
	if err != nil || run.ExitCode != 0 || !run.Complete() {
		t.Fatalf("file lock controls, observation or cleanup failed: exit=%d err=%v stderr=%s", run.ExitCode, err, run.Stderr)
	}
	var report struct {
		Missing [][2]uint64
	}
	if err := json.Unmarshal(run.Stdout, &report); err != nil {
		t.Fatal(err)
	}
	t.Logf("MDS_REQUESTS_JSON_RECEIPT %s", run.Stdout)
	result := "NOT_REPRODUCED"
	var symptoms []string
	if len(report.Missing) > 0 {
		result = "REPRODUCED"
		symptoms = append(symptoms, fmt.Sprintf("ordinary decoding lost inode/tid %v", report.Missing))
	}
	t.Logf("EXPERIMENT_RESULT case=cephfs_mds_requests_json result=%s image=%s symptoms=%q", result, image, symptoms)
}
